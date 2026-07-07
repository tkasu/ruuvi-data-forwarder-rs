use crate::dto::RuuviTelemetry;
use crate::error::SourceError;
use futures::Stream;
use tokio::io::AsyncRead;
use tokio_stream::StreamExt;
use tokio_util::bytes::BytesMut;
use tokio_util::codec::{Decoder, FramedRead, LinesCodec, LinesCodecError};

/// Upper bound for one JSON line. Valid telemetry records are a few hundred
/// bytes; the cap prevents a newline-less garbage stream from growing the
/// read buffer without limit.
pub const MAX_LINE_BYTES: usize = 64 * 1024;

/// Creates a stream that reads newline-delimited JSON telemetry from stdin.
/// Each line is parsed into a `RuuviTelemetry` and validated. Parse and
/// validation failures are yielded as `SourceError::ParseError`; a line longer
/// than `MAX_LINE_BYTES` is discarded and reported the same way. When stdin
/// closes (EOF), `SourceError::StreamShutdown` is yielded as the final item.
pub fn stdin_source() -> impl Stream<Item = Result<RuuviTelemetry, SourceError>> {
    reader_source(tokio::io::stdin())
}

/// The generic implementation behind `stdin_source`, split out so tests can
/// feed an in-memory reader.
pub fn reader_source<R: AsyncRead>(
    reader: R,
) -> impl Stream<Item = Result<RuuviTelemetry, SourceError>> {
    FramedRead::new(reader, BoundedLinesCodec::new(MAX_LINE_BYTES))
        .map(|line_result| match line_result {
            Ok(BoundedLine::Line(line)) => parse_line(&line),
            Ok(BoundedLine::Oversized) => Err(SourceError::ParseError(format!(
                "line exceeds the maximum length of {MAX_LINE_BYTES} bytes"
            ))),
            Err(error) => Err(SourceError::IoError(error)),
        })
        .chain(tokio_stream::once(Err(SourceError::StreamShutdown)))
}

enum BoundedLine {
    Line(String),
    Oversized,
}

/// A `LinesCodec` wrapper that reports an oversized line as an item rather
/// than a decode error: `FramedRead` fuses the stream after any error, while
/// `LinesCodec` itself recovers by discarding up to the next newline.
struct BoundedLinesCodec {
    inner: LinesCodec,
}

impl BoundedLinesCodec {
    fn new(max_length: usize) -> Self {
        Self {
            inner: LinesCodec::new_with_max_length(max_length),
        }
    }
}

impl Decoder for BoundedLinesCodec {
    type Item = BoundedLine;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BoundedLine>, std::io::Error> {
        map_line(self.inner.decode(src))
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<BoundedLine>, std::io::Error> {
        map_line(self.inner.decode_eof(src))
    }
}

fn map_line(
    result: Result<Option<String>, LinesCodecError>,
) -> Result<Option<BoundedLine>, std::io::Error> {
    match result {
        Ok(line) => Ok(line.map(BoundedLine::Line)),
        Err(LinesCodecError::MaxLineLengthExceeded) => Ok(Some(BoundedLine::Oversized)),
        Err(LinesCodecError::Io(error)) => Err(error),
    }
}

fn parse_line(line: &str) -> Result<RuuviTelemetry, SourceError> {
    let telemetry: RuuviTelemetry =
        serde_json::from_str(line).map_err(|e| SourceError::ParseError(e.to_string()))?;
    validate(&telemetry)?;
    Ok(telemetry)
}

/// `mac_address` is transported as JSON numbers (`Vec<i16>` for parity with
/// the Scala forwarder's `Seq[Short]`); each element must be one unsigned
/// byte, otherwise `mac_address_hex()` would silently truncate it.
fn validate(telemetry: &RuuviTelemetry) -> Result<(), SourceError> {
    if telemetry.mac_address.len() != 6 {
        return Err(SourceError::ParseError(format!(
            "mac_address must have 6 bytes, got {}",
            telemetry.mac_address.len()
        )));
    }
    if let Some(byte) = telemetry
        .mac_address
        .iter()
        .find(|byte| !(0..=255).contains(*byte))
    {
        return Err(SourceError::ParseError(format!(
            "mac_address byte {byte} is outside 0..=255"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn collect(input: String) -> Vec<Result<RuuviTelemetry, SourceError>> {
        let reader = std::io::Cursor::new(input.into_bytes());
        let source = reader_source(reader);
        tokio::pin!(source);
        let mut items = Vec::new();
        while let Some(item) = source.next().await {
            items.push(item);
        }
        items
    }

    fn valid_line() -> String {
        r#"{"battery_potential":2335,"humidity":653675,"measurement_ts_ms":1693460525701,"mac_address":[254,38,136,122,102,102],"measurement_sequence_number":53300,"movement_counter":2,"pressure":100755,"temperature_millicelsius":-29020,"tx_power":4}"#.to_string()
    }

    fn line_with_mac(mac: &str) -> String {
        valid_line().replace("[254,38,136,122,102,102]", mac)
    }

    #[tokio::test]
    async fn valid_line_parses_and_stream_ends_with_shutdown() {
        let items = collect(format!("{}\n", valid_line())).await;
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0].as_ref().unwrap().mac_address,
            vec![254, 38, 136, 122, 102, 102]
        );
        assert!(matches!(items[1], Err(SourceError::StreamShutdown)));
    }

    #[tokio::test]
    async fn wrong_mac_length_is_a_parse_error() {
        for mac in ["[]", "[1,2,3]", "[1,2,3,4,5,6,7]"] {
            let items = collect(format!("{}\n", line_with_mac(mac))).await;
            assert!(
                matches!(&items[0], Err(SourceError::ParseError(m)) if m.contains("6 bytes")),
                "expected parse error for mac {mac}, got {:?}",
                items[0]
            );
        }
    }

    #[tokio::test]
    async fn out_of_range_mac_byte_is_a_parse_error() {
        for mac in ["[256,2,3,4,5,6]", "[-1,2,3,4,5,6]"] {
            let items = collect(format!("{}\n", line_with_mac(mac))).await;
            assert!(
                matches!(&items[0], Err(SourceError::ParseError(m)) if m.contains("0..=255")),
                "expected parse error for mac {mac}, got {:?}",
                items[0]
            );
        }
    }

    #[tokio::test]
    async fn oversized_line_is_discarded_and_stream_recovers() {
        let oversized = "x".repeat(MAX_LINE_BYTES + 1);
        let items = collect(format!("{oversized}\n{}\n", valid_line())).await;
        assert!(
            matches!(&items[0], Err(SourceError::ParseError(m)) if m.contains("maximum length")),
            "expected line-length error, got {:?}",
            items[0]
        );
        assert!(items[1].is_ok(), "stream did not recover: {:?}", items[1]);
        assert!(matches!(
            items.last(),
            Some(Err(SourceError::StreamShutdown))
        ));
    }
}
