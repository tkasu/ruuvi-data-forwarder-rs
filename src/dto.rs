use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuuviTelemetry {
    pub temperature_millicelsius: i32,
    pub humidity: i32,
    pub pressure: i32,
    pub battery_potential: i32,
    pub tx_power: i32,
    pub movement_counter: i32,
    pub measurement_sequence_number: i32,
    pub measurement_ts_ms: i64,
    pub mac_address: Vec<i16>,
}

impl RuuviTelemetry {
    /// Format MAC address as colon-separated uppercase hex string, e.g. "FE:26:88:7A:66:66".
    /// Each i16 is treated as an unsigned byte (low 8 bits), matching Scala's `b & 0xff`.
    pub fn mac_address_hex(&self) -> String {
        self.mac_address
            .iter()
            .map(|b| format!("{:02X}", (*b as u8)))
            .collect::<Vec<_>>()
            .join(":")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mac_address_hex() {
        let telemetry = RuuviTelemetry {
            temperature_millicelsius: 0,
            humidity: 0,
            pressure: 0,
            battery_potential: 0,
            tx_power: 0,
            movement_counter: 0,
            measurement_sequence_number: 0,
            measurement_ts_ms: 0,
            mac_address: vec![254, 38, 136, 122, 102, 102],
        };
        assert_eq!(telemetry.mac_address_hex(), "FE:26:88:7A:66:66");
    }
}
