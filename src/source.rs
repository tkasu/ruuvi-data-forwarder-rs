use crate::dto::RuuviTelemetry;
use crate::error::SourceError;
use futures::Stream;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;

/// Creates a stream that reads newline-delimited JSON telemetry from stdin.
/// Each line is parsed into a `RuuviTelemetry`. Parse errors are yielded as `SourceError::ParseError`.
/// When stdin closes (EOF), `SourceError::StreamShutdown` is yielded as the final item.
pub fn stdin_source() -> impl Stream<Item = Result<RuuviTelemetry, SourceError>> {
    let reader = BufReader::new(tokio::io::stdin());
    let lines = LinesStream::new(reader.lines());

    lines
        .map(|line_result| {
            let line = line_result.map_err(SourceError::IoError)?;
            serde_json::from_str::<RuuviTelemetry>(&line)
                .map_err(|e| SourceError::ParseError(e.to_string()))
        })
        .chain(tokio_stream::once(Err(SourceError::StreamShutdown)))
}
