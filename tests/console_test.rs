use assert_cmd::Command;
use ruuvi_data_forwarder_rs::dto::RuuviTelemetry;

fn make_telemetry1() -> RuuviTelemetry {
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

fn make_telemetry2() -> RuuviTelemetry {
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

#[test]
fn console_sink_passes_through_values() {
    let t1 = make_telemetry1();
    let t2 = make_telemetry2();

    let input = format!(
        "{}\n{}\n",
        serde_json::to_string(&t1).unwrap(),
        serde_json::to_string(&t2).unwrap()
    );

    let output = Command::cargo_bin("ruuvi-data-forwarder-rs")
        .unwrap()
        .env("RUUVI_SINK_TYPE", "console")
        .write_stdin(input)
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2, "Expected 2 output lines");

    let parsed1: RuuviTelemetry = serde_json::from_str(lines[0]).unwrap();
    let parsed2: RuuviTelemetry = serde_json::from_str(lines[1]).unwrap();

    assert_eq!(vec![parsed1, parsed2], vec![t1, t2]);
}

#[test]
fn console_broken_pipe_returns_an_error_without_panicking() {
    use std::io::Write;
    use std::process::Stdio;

    let telemetry = make_telemetry1();
    let mut child =
        std::process::Command::new(assert_cmd::cargo::cargo_bin!("ruuvi-data-forwarder-rs"))
            .env("RUUVI_SINK_TYPE", "console")
            .env("RUUVI_MAX_WRITE_RETRIES", "0")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

    drop(child.stdout.take());
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{}", serde_json::to_string(&telemetry).unwrap()).unwrap();
    drop(stdin);

    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("panicked"), "unexpected panic: {stderr}");
    assert!(stderr.contains("Broken pipe"), "unexpected error: {stderr}");
}
