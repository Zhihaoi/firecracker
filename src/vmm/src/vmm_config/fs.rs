// Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::devices::virtio::device::VirtioDevice;
use crate::devices::virtio::fs::VhostUserFsError;
use crate::devices::virtio::fs::device::{VhostUserFs, VhostUserFsConfig};

/// Errors associated with the operations allowed on an fs device.
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum FsConfigError {
    /// Unable to create the vhost-user fs device: {0}
    CreateFsDevice(VhostUserFsError),
}

/// Use this structure to set up an Fs Device before booting the kernel.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FsDeviceConfig {
    /// Unique identifier of the fs device.
    pub fs_id: String,
    /// Path to the vhost-user backend socket.
    pub socket: String,
    /// Mount tag presented to the guest. Defaults to `fs_id`.
    pub tag: Option<String>,
    /// Number of request queues, in addition to the hiprio queue. Defaults to 1.
    pub num_request_queues: Option<u16>,
}

/// Wrapper for the collection that holds all the Fs Devices.
#[derive(Debug, Default)]
pub struct FsBuilder {
    /// The list of fs devices.
    pub devices: Vec<Arc<Mutex<VhostUserFs>>>,
}

impl FsBuilder {
    /// Gets the index of the device with the specified `fs_id` if it exists in the list.
    fn get_index_of_fs_id(&self, fs_id: &str) -> Option<usize> {
        self.devices
            .iter()
            .position(|f| f.lock().expect("Poisoned lock").id().eq(fs_id))
    }

    /// Inserts a `VhostUserFs` in the fs devices list using the specified configuration.
    /// If a device with the same id already exists, it will overwrite it.
    /// The vhost-user backend socket is connected here, so an unreachable
    /// backend fails this call.
    pub fn insert(&mut self, config: FsDeviceConfig) -> Result<(), FsConfigError> {
        let position = self.get_index_of_fs_id(&config.fs_id);
        let fs = Arc::new(Mutex::new(
            VhostUserFs::new(VhostUserFsConfig::from(&config))
                .map_err(FsConfigError::CreateFsDevice)?,
        ));

        match position {
            // New fs device.
            None => self.devices.push(fs),
            // Update existing fs device.
            Some(index) => self.devices[index] = fs,
        }
        Ok(())
    }

    /// Returns a vec with the structures used to configure the devices.
    pub fn configs(&self) -> Vec<FsDeviceConfig> {
        self.devices
            .iter()
            .map(|f| f.lock().expect("Poisoned lock").config())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_tmp_socket;

    #[test]
    fn test_fs_config_deserialization() {
        // Full configuration.
        let json = r#"{
            "fs_id": "rootfs",
            "socket": "/tmp/backend.sock",
            "tag": "my_tag",
            "num_request_queues": 4
        }"#;
        let config = serde_json::from_str::<FsDeviceConfig>(json).unwrap();
        assert_eq!(config.fs_id, "rootfs");
        assert_eq!(config.socket, "/tmp/backend.sock");
        assert_eq!(config.tag, Some("my_tag".to_string()));
        assert_eq!(config.num_request_queues, Some(4));

        // Optional fields can be omitted.
        let json = r#"{
            "fs_id": "rootfs",
            "socket": "/tmp/backend.sock"
        }"#;
        let config = serde_json::from_str::<FsDeviceConfig>(json).unwrap();
        assert_eq!(config.tag, None);
        assert_eq!(config.num_request_queues, None);

        // Unknown fields are rejected.
        let json = r#"{
            "fs_id": "rootfs",
            "socket": "/tmp/backend.sock",
            "unknown_field": true
        }"#;
        serde_json::from_str::<FsDeviceConfig>(json).unwrap_err();
    }

    #[test]
    fn test_fs_builder_insert_dead_socket() {
        // Inserting a device whose backend socket does not exist fails the
        // call cleanly, without adding the device to the list.
        let mut builder = FsBuilder::default();
        let config = FsDeviceConfig {
            fs_id: "rootfs".to_string(),
            socket: "/nonexistent/backend.sock".to_string(),
            tag: None,
            num_request_queues: None,
        };
        assert!(matches!(
            builder.insert(config).unwrap_err(),
            FsConfigError::CreateFsDevice(VhostUserFsError::VhostUser(_))
        ));
        assert!(builder.devices.is_empty());

        // A backend socket that no process is listening on fails the same way.
        let (tmp_dir, tmp_socket_path) = create_tmp_socket();
        drop(tmp_dir);
        let config = FsDeviceConfig {
            fs_id: "rootfs".to_string(),
            socket: tmp_socket_path,
            tag: None,
            num_request_queues: None,
        };
        assert!(matches!(
            builder.insert(config).unwrap_err(),
            FsConfigError::CreateFsDevice(VhostUserFsError::VhostUser(_))
        ));
        assert!(builder.devices.is_empty());
    }
}
