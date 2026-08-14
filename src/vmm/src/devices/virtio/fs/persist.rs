// Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Defines the structures needed for saving/restoring fs devices.

use serde::{Deserialize, Serialize};

use super::device::{VhostUserFsConfig, VhostUserFsImpl};
use super::{NUM_HIPRIO_QUEUES, QUEUE_SIZE, VhostUserFsError};
use crate::MutEventSubscriber;
use crate::devices::virtio::device::VirtioDeviceType;
use crate::devices::virtio::persist::{PersistError as VirtioStateError, VirtioDeviceState};
use crate::devices::virtio::vhost_user::VhostUserHandleBackend;
use crate::snapshot::Persist;
use crate::utils::u64_to_usize;
use crate::vstate::memory::GuestMemoryMmap;
use crate::vstate::vm::KvmVm;

/// vhost-user fs device state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VhostUserFsState {
    pub id: String,
    pub tag: String,
    pub num_request_queues: u16,
    pub socket_path: String,
    pub vu_acked_protocol_features: u64,
    pub virtio_state: VirtioDeviceState,
    /// Backend device-state blob, captured via the vhost-user DEVICE_STATE
    /// transfer. Snapshot files are bitcode-serialized and cannot carry
    /// fds, so the blob rides inside the device state. Empty when the
    /// device was not activated at snapshot time.
    pub backend_state: Vec<u8>,
    /// Size of the DAX cache window in MiB, if configured.
    #[serde(default)]
    pub dax_window_size_mib: Option<u64>,
    /// Guest physical address where the DAX window is mapped.
    #[serde(default)]
    pub dax_window_gpa: Option<u64>,
}

/// Auxiliary structure for creating a device when resuming from a snapshot.
#[derive(Debug)]
pub struct FsConstructorArgs<'a> {
    pub mem: GuestMemoryMmap,
    pub vm: Option<&'a KvmVm>,
}

impl<'a, T> Persist<'a> for VhostUserFsImpl<T>
where
    T: VhostUserHandleBackend + Send + 'static,
    VhostUserFsImpl<T>: MutEventSubscriber,
{
    type State = VhostUserFsState;
    type ConstructorArgs = FsConstructorArgs<'a>;
    type Error = VhostUserFsError;

    /// Embed the backend state blob captured by `capture_backend_state`
    /// (which runs first, in `Vmm::save_state`, where errors can still be
    /// returned) together with the virtio state.
    fn save(&self) -> Self::State {
        VhostUserFsState {
            id: self.id.clone(),
            tag: self.tag.clone(),
            num_request_queues: self.num_request_queues,
            socket_path: self.vu_handle.socket_path.clone(),
            vu_acked_protocol_features: self.vu_acked_protocol_features,
            virtio_state: VirtioDeviceState::from_device(self),
            backend_state: self.backend_state.clone().unwrap_or_default(),
            dax_window_size_mib: self.dax_window_size_mib,
            dax_window_gpa: self.dax_window_gpa(),
        }
    }

    fn restore(
        constructor_args: Self::ConstructorArgs,
        state: &Self::State,
    ) -> Result<Self, Self::Error> {
        let num_queues = NUM_HIPRIO_QUEUES + u64::from(state.num_request_queues);
        let queues = state.virtio_state.build_queues_checked(
            &constructor_args.mem,
            VirtioDeviceType::Fs,
            u64_to_usize(num_queues),
            QUEUE_SIZE,
        )?;

        // Reconnect to the backend (a fresh process at the same socket
        // path) and re-negotiate features, DEVICE_STATE included.
        let mut device = VhostUserFsImpl::<T>::new(VhostUserFsConfig {
            fs_id: state.id.clone(),
            socket: state.socket_path.clone(),
            tag: state.tag.clone(),
            num_request_queues: state.num_request_queues,
            dax_window_size_mib: state.dax_window_size_mib,
        })?;

        // Re-create the DAX window before the device is attached to the
        // MMIO transport, pinned to the persisted GPA (the restored guest's
        // mapping table names it).
        if state.dax_window_size_mib.is_some() {
            let vm = constructor_args.vm.ok_or(VhostUserFsError::DaxWindowGpaMismatch)?;
            device.create_dax_window(vm, state.dax_window_gpa)?;
            if device.dax_window_gpa() != state.dax_window_gpa {
                return Err(VhostUserFsError::DaxWindowGpaMismatch);
            }
        }

        // Sanity: the freshly negotiated virtio features must cover what
        // the guest had acked, and if there is backend state to load, the
        // fresh backend must be able to receive it.
        if device.avail_features & state.virtio_state.acked_features
            != state.virtio_state.acked_features
        {
            return Err(VirtioStateError::InvalidInput.into());
        }
        if !state.backend_state.is_empty() && !device.snapshot_capable() {
            return Err(VhostUserFsError::DeviceStateNotNegotiated);
        }

        device.queues = queues;
        device.acked_features = state.virtio_state.acked_features;
        if !state.backend_state.is_empty() {
            // Consumed by `activate`, which loads the blob into the backend
            // before the vrings are set up and enabled.
            device.backend_state = Some(state.backend_state.clone());
        }
        Ok(device)
    }
}

#[cfg(test)]
mod tests {
    use vhost::vhost_user::message::VhostUserProtocolFeatures;

    use super::*;

    #[test]
    fn test_fs_state_serde_roundtrip() {
        let state = VhostUserFsState {
            id: "fs0".to_string(),
            tag: "my_tag".to_string(),
            num_request_queues: 3,
            socket_path: "/tmp/backend.sock".to_string(),
            vu_acked_protocol_features: VhostUserProtocolFeatures::DEVICE_STATE.bits(),
            virtio_state: VirtioDeviceState {
                device_type: VirtioDeviceType::Fs,
                avail_features: 0x69,
                acked_features: 0x42,
                queues: vec![],
                activated: true,
            },
            backend_state: b"backend-state-blob".to_vec(),
            dax_window_size_mib: None,
            dax_window_gpa: None,
        };

        let serialized = bitcode::serialize(&state).unwrap();
        let restored: VhostUserFsState = bitcode::deserialize(&serialized).unwrap();

        assert_eq!(restored.id, state.id);
        assert_eq!(restored.tag, state.tag);
        assert_eq!(restored.num_request_queues, state.num_request_queues);
        assert_eq!(restored.socket_path, state.socket_path);
        assert_eq!(
            restored.vu_acked_protocol_features,
            state.vu_acked_protocol_features
        );
        assert_eq!(restored.virtio_state, state.virtio_state);
        assert_eq!(restored.backend_state, state.backend_state);
    }
}
