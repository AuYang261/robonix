// SPDX-License-Identifier: MulanPSL-2.0
// Tonic-generated wire types. `contract_proto_modules.rs` is emitted by
// robonix-codegen (build.rs) and declares `pub mod <pkg>` for each proto
// package, ordered so prost `super::sibling` references resolve.
//
// Used here:
//   pb::contracts::robonix_system_vitals_get_server    — GetVitals RPC handler
//   pb::contracts::robonix_system_vitals_stream_server  — StreamVitals server_stream handler
//   pb::vitals                                          — VitalsSnapshot, PowerState, HealthSignal
#![allow(
    dead_code,
    unused_imports,
    unused_variables,
    clippy::all,
    rustdoc::broken_intra_doc_links,
    rustdoc::invalid_html_tags
)]

include!(concat!(env!("OUT_DIR"), "/contract_proto_modules.rs"));

#[cfg(test)]
mod tests {
    use super::vitals::{HealthSignal, health_signal::HealthSignalEnum};
    use prost::Message;
    use std::path::Path;

    fn proto_message_block<'a>(proto: &'a str, message_name: &str) -> &'a str {
        let marker = format!("message {message_name} {{");
        let start = proto
            .find(&marker)
            .unwrap_or_else(|| panic!("generated proto missing {marker}"));
        let tail = &proto[start..];
        let end = tail
            .find("\n}")
            .unwrap_or_else(|| panic!("generated proto message {message_name} is unterminated"))
            + 2;
        &tail[..end]
    }

    #[test]
    fn health_signal_target_idl_and_codegen_are_declared() {
        let idl_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../capabilities/lib/vitals/msg/HealthSignal.msg");
        let idl = std::fs::read_to_string(&idl_path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", idl_path.display()));
        let declarations: Vec<&str> = idl
            .lines()
            .map(|line| line.split('#').next().unwrap_or_default().trim())
            .filter(|line| !line.is_empty())
            .collect();
        assert_eq!(
            declarations,
            [
                "uint8 OK=0",
                "uint8 WARN=1",
                "uint8 ERROR=2",
                "uint8 STALE=3",
                "string key",
                "uint8 status",
                "string detail",
                "float32 observed_value",
                "float32 reference_value",
            ],
            "HealthSignal constants or field declarations changed"
        );

        let proto = include_str!(concat!(env!("OUT_DIR"), "/vitals.proto"));
        assert!(
            proto.contains("message HealthSignal {")
                && proto.contains("string key = 1;")
                && proto.contains("uint32 status = 2;")
                && proto.contains("string detail = 3;")
                && proto.contains("float observed_value = 4;")
                && proto.contains("float reference_value = 5;"),
            "generated HealthSignal field types or tags changed"
        );

        for (name, value) in [("OK", 0), ("WARN", 1), ("ERROR", 2), ("STALE", 3)] {
            assert_eq!(
                HealthSignalEnum::from_str_name(name).map(|variant| variant as i32),
                Some(value),
                "generated HealthSignal constant {name} changed"
            );
        }

        let encoded = HealthSignal {
            key: "k".to_string(),
            status: 3,
            detail: "d".to_string(),
            observed_value: 1.5,
            reference_value: -1.0,
        }
        .encode_to_vec();
        assert_eq!(
            encoded,
            [
                0x0a, 0x01, b'k', 0x10, 0x03, 0x1a, 0x01, b'd', 0x25, 0x00, 0x00, 0xc0, 0x3f, 0x2d,
                0x00, 0x00, 0x80, 0xbf,
            ],
            "HealthSignal field tags or wire types changed"
        );
    }

    #[test]
    fn target_v1_output_idl_and_codegen_are_exact() {
        let msg_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../capabilities/lib/vitals/msg");
        let declarations = |name: &str| {
            let path = msg_dir.join(name);
            std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
                .lines()
                .map(|line| line.split('#').next().unwrap_or_default().trim())
                .filter(|line| !line.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        };

        assert_eq!(
            declarations("BodyComponent.msg"),
            [
                "string id",
                "string parent_id",
                "string name",
                "string kind",
                "string model"
            ]
        );
        assert_eq!(
            declarations("BodyHealth.msg"),
            [
                "uint32 NORMAL=0",
                "uint32 FAULT=1",
                "uint32 ESTOP=2",
                "string key",
                "string model",
                "uint32 status",
                "BodyComponent[] components",
            ]
        );
        assert_eq!(
            declarations("PowerState.msg"),
            [
                "float32 soc_percent",
                "float32 voltage",
                "bool charging",
                "int64 remaining_s",
            ]
        );
        assert_eq!(
            declarations("VitalsSnapshot.msg"),
            [
                "uint64 ts_ns",
                "PowerState power",
                "HealthSignal[] health_signals",
                "BodyHealth[] bodies",
            ]
        );
        assert!(
            !msg_dir.join("ComponentHealth.msg").exists(),
            "ComponentHealth must be replaced directly by HealthSignal"
        );

        let proto = include_str!(concat!(env!("OUT_DIR"), "/vitals.proto"));
        let body_component = proto_message_block(proto, "BodyComponent");
        assert!(body_component.contains("string id = 1;"));
        assert!(body_component.contains("string parent_id = 2;"));
        assert!(body_component.contains("string name = 3;"));
        assert!(body_component.contains("string kind = 4;"));
        assert!(body_component.contains("string model = 5;"));

        let body_health = proto_message_block(proto, "BodyHealth");
        assert!(body_health.contains("string key = 1;"));
        assert!(body_health.contains("string model = 2;"));
        assert!(body_health.contains("uint32 status = 3;"));
        assert!(body_health.contains("repeated BodyComponent components = 4;"));

        let power_state = proto_message_block(proto, "PowerState");
        assert!(power_state.contains("float soc_percent = 1;"));
        assert!(power_state.contains("float voltage = 2;"));
        assert!(power_state.contains("bool charging = 3;"));
        assert!(power_state.contains("int64 remaining_s = 4;"));

        let snapshot = proto_message_block(proto, "VitalsSnapshot");
        assert!(snapshot.contains("uint64 ts_ns = 1;"));
        assert!(snapshot.contains("PowerState power = 2;"));
        assert!(snapshot.contains("repeated HealthSignal health_signals = 3;"));
        assert!(snapshot.contains("repeated BodyHealth bodies = 4;"));

        assert!(!proto.contains("message ComponentHealth {"));
    }
}
