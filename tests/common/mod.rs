use ruuvi_data_forwarder_rs::dto::RuuviTelemetry;

pub fn telemetry1() -> RuuviTelemetry {
    RuuviTelemetry {
        battery_potential: 2335,
        humidity: 653675,
        mac_address: vec![254, 38, 136, 122, 102, 102],
        measurement_ts_ms: 1693460525699,
        measurement_sequence_number: 53300,
        movement_counter: 2,
        pressure: 100755,
        temperature_millicelsius: -29020,
        tx_power: 4,
    }
}

pub fn telemetry2() -> RuuviTelemetry {
    RuuviTelemetry {
        battery_potential: 2176,
        humidity: 576425,
        mac_address: vec![213, 18, 52, 102, 20, 20],
        measurement_ts_ms: 1693460525701,
        measurement_sequence_number: 1589,
        movement_counter: 79,
        pressure: 100556,
        temperature_millicelsius: 22080,
        tx_power: 4,
    }
}

#[allow(dead_code)]
pub fn parse_mac_hex(s: &str) -> Vec<i16> {
    s.split(':')
        .map(|h| i16::from_str_radix(h, 16).unwrap())
        .collect()
}
