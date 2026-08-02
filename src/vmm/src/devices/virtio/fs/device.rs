// Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::Arc;

use utils::time::{ClockType, get_time_us};
use vhost::vhost_user::Frontend;
use vhost::vhost_user::message::*;
use vmm_sys_util::eventfd::EventFd;

use super::{NUM_HIPRIO_QUEUES, QUEUE_SIZE, TAG_LEN, VhostUserFsError};
use crate::devices::virtio::ActivateError;
use crate::devices::virtio::device::{ActiveState, DeviceState, VirtioDevice, VirtioDeviceType};
use crate::devices::virtio::generated::virtio_config::VIRTIO_F_VERSION_1;
use crate::devices::virtio::generated::virtio_ring::VIRTIO_RING_F_EVENT_IDX;
use crate::devices::virtio::queue::Queue;
use crate::devices::virtio::transport::VirtioInterrupt;
use crate::devices::virtio::vhost_user::{
    VhostUserError, VhostUserHandleBackend, VhostUserHandleImpl,
};
use crate::devices::virtio::vhost_user_metrics::{
    VhostUserDeviceMetrics, VhostUserMetricsPerDevice,
};
use crate::logger::{IncMetric, StoreMetric, log_dev_preview_warning};
use crate::utils::u64_to_usize;
use crate::vmm_config::fs::FsDeviceConfig;
use crate::vstate::memory::GuestMemoryMmap;
use crate::{MutEventSubscriber, impl_device_type};

/// Fs device config space size in bytes: the mount tag (`TAG_LEN` bytes)
/// followed by `num_request_queues` as a little-endian u16.
const FS_CONFIG_SPACE_SIZE: usize = TAG_LEN + 2;

const AVAILABLE_FEATURES: u64 = (1 << VIRTIO_F_VERSION_1)
    | (1 << VIRTIO_RING_F_EVENT_IDX)
    // vhost-user specific bit. Not defined in standard virtio spec.
    // Specifies ability of frontend to negotiate protocol features.
    | VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits();

/// Default number of request queues of the fs device, in addition to the
/// hiprio queue.
const DEFAULT_NUM_REQUEST_QUEUES: u16 = 1;

/// Protocol features requested from the backend. DEVICE_STATE enables the
/// backend device-state transfer that snapshotting relies on; if the
/// backend does not advertise it the device works normally, but snapshots
/// are refused at save time.
const REQUESTED_PROTOCOL_FEATURES: VhostUserProtocolFeatures =
    VhostUserProtocolFeatures::DEVICE_STATE;

/// Builds the fs device config space from the mount tag and the number of
/// request queues. The tag is a frontend property, so the config space is
/// built locally and never fetched from the backend.
fn build_config_space(tag: &str, num_request_queues: u16) -> Vec<u8> {
    let mut config_space = Vec::with_capacity(FS_CONFIG_SPACE_SIZE);
    // The tag is NUL-padded to `TAG_LEN` bytes. A tag that does not fit is
    // truncated to `TAG_LEN - 1` bytes so that the field always ends with a
    // NUL byte.
    let tag_len = tag.len().min(TAG_LEN - 1);
    config_space.extend_from_slice(&tag.as_bytes()[..tag_len]);
    config_space.resize(TAG_LEN, 0);
    config_space.extend_from_slice(&num_request_queues.to_le_bytes());
    config_space
}

/// Use this structure to set up the Fs Device before booting the kernel.
#[derive(Debug, PartialEq, Eq)]
pub struct VhostUserFsConfig {
    /// Unique identifier of the fs device.
    pub fs_id: String,
    /// Socket path of the vhost-user process.
    pub socket: String,
    /// Mount tag presented to the guest in the device config space.
    pub tag: String,
    /// Number of request queues, in addition to the hiprio queue.
    pub num_request_queues: u16,
}

impl From<&FsDeviceConfig> for VhostUserFsConfig {
    fn from(value: &FsDeviceConfig) -> Self {
        Self {
            fs_id: value.fs_id.clone(),
            socket: value.socket.clone(),
            tag: value.tag.clone().unwrap_or_else(|| value.fs_id.clone()),
            num_request_queues: value
                .num_request_queues
                .unwrap_or(DEFAULT_NUM_REQUEST_QUEUES),
        }
    }
}

pub type VhostUserFs = VhostUserFsImpl<Frontend>;

/// vhost-user fs device.
pub struct VhostUserFsImpl<T: VhostUserHandleBackend> {
    // Virtio fields.
    pub avail_features: u64,
    pub acked_features: u64,
    pub config_space: Vec<u8>,
    pub activate_evt: EventFd,

    // Transport related fields.
    pub queues: Vec<Queue>,
    pub queue_evts: Vec<EventFd>,
    pub device_state: DeviceState,

    // Implementation specific fields.
    pub id: String,
    pub tag: String,
    pub num_request_queues: u16,

    // Vhost user protocol handle
    pub vu_handle: VhostUserHandleImpl<T>,
    pub vu_acked_protocol_features: u64,
    /// Backend device-state blob. Populated by `capture_backend_state` on
    /// the snapshot save path and consumed by `activate` on the restore
    /// path, where it is loaded into the fresh backend before the vrings
    /// are set up and enabled.
    pub backend_state: Option<Vec<u8>>,
    pub metrics: Arc<VhostUserDeviceMetrics>,
}

// Need custom implementation because otherwise `Debug` is required for `vhost::Master`
impl<T: VhostUserHandleBackend> std::fmt::Debug for VhostUserFsImpl<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VhostUserFsImpl")
            .field("avail_features", &self.avail_features)
            .field("acked_features", &self.acked_features)
            .field("config_space", &self.config_space)
            .field("activate_evt", &self.activate_evt)
            .field("queues", &self.queues)
            .field("queue_evts", &self.queue_evts)
            .field("device_state", &self.device_state)
            .field("id", &self.id)
            .field("tag", &self.tag)
            .field("num_request_queues", &self.num_request_queues)
            .field("vu_handle", &self.vu_handle)
            .field(
                "vu_acked_protocol_features",
                &self.vu_acked_protocol_features,
            )
            .field("backend_state", &self.backend_state.as_ref().map(Vec::len))
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl<T: VhostUserHandleBackend> VhostUserFsImpl<T> {
    pub fn new(config: VhostUserFsConfig) -> Result<Self, VhostUserFsError> {
        log_dev_preview_warning("vhost-user-fs device", Option::None);
        let start_time = get_time_us(ClockType::Monotonic);

        // The config space (including the mount tag) is a frontend property,
        // so no CONFIG protocol feature is requested. DEVICE_STATE is
        // requested so the backend can transfer its internal state for
        // snapshotting.
        let requested_protocol_features = REQUESTED_PROTOCOL_FEATURES;

        let num_queues = NUM_HIPRIO_QUEUES + u64::from(config.num_request_queues);
        let mut vu_handle = VhostUserHandleImpl::<T>::new(&config.socket, num_queues)
            .map_err(VhostUserFsError::VhostUser)?;
        let (acked_features, acked_protocol_features) = vu_handle
            .negotiate_features(AVAILABLE_FEATURES, requested_protocol_features)
            .map_err(VhostUserFsError::VhostUser)?;

        let config_space = build_config_space(&config.tag, config.num_request_queues);

        let activate_evt = EventFd::new(libc::EFD_NONBLOCK).map_err(VhostUserFsError::EventFd)?;

        let queues = vec![Queue::new(QUEUE_SIZE); u64_to_usize(num_queues)];
        let queue_evts = (0..num_queues)
            .map(|_| EventFd::new(libc::EFD_NONBLOCK))
            .collect::<Result<Vec<_>, _>>()
            .map_err(VhostUserFsError::EventFd)?;
        let device_state = DeviceState::Inactive;

        // We negotiated features with backend. Now these acked_features
        // are available for guest driver to choose from.
        let avail_features = acked_features;
        let acked_features = acked_features & VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits();
        let vhost_user_fs_metrics_name = format!("fs_{}", config.fs_id);

        let metrics = VhostUserMetricsPerDevice::alloc(vhost_user_fs_metrics_name);
        let delta_us = get_time_us(ClockType::Monotonic) - start_time;
        metrics.init_time_us.store(delta_us);

        Ok(Self {
            avail_features,
            acked_features,
            config_space,
            activate_evt,

            queues,
            queue_evts,
            device_state,

            id: config.fs_id,
            tag: config.tag,
            num_request_queues: config.num_request_queues,

            vu_handle,
            vu_acked_protocol_features: acked_protocol_features,
            backend_state: None,
            metrics,
        })
    }

    pub fn config(&self) -> FsDeviceConfig {
        FsDeviceConfig {
            fs_id: self.id.clone(),
            socket: self.vu_handle.socket_path.clone(),
            tag: Some(self.tag.clone()),
            num_request_queues: Some(self.num_request_queues),
        }
    }

    /// Whether the backend acknowledged the DEVICE_STATE protocol feature,
    /// i.e. whether it can transfer its internal state for snapshotting.
    pub fn snapshot_capable(&self) -> bool {
        self.vu_acked_protocol_features & VhostUserProtocolFeatures::DEVICE_STATE.bits() != 0
    }

    /// Quiesce the backend, capture its device-state blob and re-enable the
    /// vrings. Must be called with the VM paused, before `Persist::save`
    /// embeds the captured blob in the device state. The backend keeps
    /// serving the device afterwards, so the source VM can be resumed.
    pub fn capture_backend_state(&mut self) -> Result<(), VhostUserFsError> {
        self.backend_state = None;
        if !self.device_state.is_activated() {
            // The device was never activated, so the backend has no state
            // bound to a running VM. The snapshot carries an empty blob and
            // the restore side skips the LOAD transfer.
            return Ok(());
        }
        if !self.snapshot_capable() {
            return Err(VhostUserFsError::DeviceStateNotNegotiated);
        }

        // Stop the backend from picking up new requests. In-flight requests
        // drain on the backend side: its state transfer is serialized
        // against request processing, which stops once the vrings are
        // disabled.
        let capture_result = self
            .set_vrings_enabled(false)
            .and_then(|()| self.save_backend_state());
        // Re-enable the vrings even if the transfer failed, so the source
        // VM is not left with a dead fs device.
        let resume_result = self.set_vrings_enabled(true);
        self.backend_state = Some(capture_result?);
        resume_result?;
        Ok(())
    }

    fn set_vrings_enabled(&mut self, enabled: bool) -> Result<(), VhostUserFsError> {
        for index in 0..self.queues.len() {
            self.vu_handle
                .vu
                .set_vring_enable(index, enabled)
                .map_err(VhostUserError::VhostUserSetVringEnable)
                .map_err(VhostUserFsError::VhostUser)?;
        }
        Ok(())
    }

    /// Run the SAVE device-state transfer: the backend writes its state
    /// blob into a memfd, `check_device_state` waits for the transfer to
    /// complete, then the blob is read back to bytes.
    fn save_backend_state(&self) -> Result<Vec<u8>, VhostUserFsError> {
        let mut file = memfd::MemfdOptions::default()
            .create("vhost_user_fs_state")
            .map_err(VhostUserFsError::Memfd)?
            .into_file();
        // The backend writes the blob at the shared file offset (0 for a
        // fresh memfd). Pass a dup so this end stays usable for the
        // readback below.
        self.vu_handle
            .set_device_state_fd(
                VhostTransferStateDirection::SAVE,
                VhostTransferStatePhase::STOPPED,
                file.try_clone()
                    .map_err(VhostUserFsError::DeviceStateTransfer)?
                    .into(),
            )
            .map_err(VhostUserFsError::VhostUser)?;
        self.vu_handle
            .check_device_state()
            .map_err(VhostUserFsError::VhostUser)?;
        file.seek(SeekFrom::Start(0))
            .map_err(VhostUserFsError::DeviceStateTransfer)?;
        let mut blob = Vec::new();
        file.read_to_end(&mut blob)
            .map_err(VhostUserFsError::DeviceStateTransfer)?;
        Ok(blob)
    }

    /// Run the LOAD device-state transfer: write the blob to a memfd and
    /// hand it to the backend, which reads it to EOF. Must happen before
    /// the vrings are set up and enabled.
    fn load_backend_state(&self, blob: &[u8]) -> Result<(), VhostUserFsError> {
        let mut file = memfd::MemfdOptions::default()
            .create("vhost_user_fs_state")
            .map_err(VhostUserFsError::Memfd)?
            .into_file();
        file.write_all(blob)
            .map_err(VhostUserFsError::DeviceStateTransfer)?;
        // The backend reads the blob starting at the shared file offset.
        file.seek(SeekFrom::Start(0))
            .map_err(VhostUserFsError::DeviceStateTransfer)?;
        self.vu_handle
            .set_device_state_fd(
                VhostTransferStateDirection::LOAD,
                VhostTransferStatePhase::STOPPED,
                file.into(),
            )
            .map_err(VhostUserFsError::VhostUser)?;
        self.vu_handle
            .check_device_state()
            .map_err(VhostUserFsError::VhostUser)?;
        Ok(())
    }
}

impl<T: VhostUserHandleBackend + Send + 'static> VirtioDevice for VhostUserFsImpl<T>
where
    VhostUserFsImpl<T>: MutEventSubscriber,
{
    impl_device_type!(VirtioDeviceType::Fs);

    fn id(&self) -> &str {
        &self.id
    }

    fn avail_features(&self) -> u64 {
        self.avail_features
    }

    fn acked_features(&self) -> u64 {
        self.acked_features
    }

    fn set_acked_features(&mut self, acked_features: u64) {
        self.acked_features = acked_features;
    }

    fn queues(&self) -> &[Queue] {
        &self.queues
    }

    fn queues_mut(&mut self) -> &mut [Queue] {
        &mut self.queues
    }

    fn queue_events(&self) -> &[EventFd] {
        &self.queue_evts
    }

    fn interrupt_trigger(&self) -> &dyn VirtioInterrupt {
        self.device_state
            .active_state()
            .expect("Device is not initialized")
            .interrupt
            .deref()
    }

    fn config_as_bytes(&self) -> &[u8] {
        self.config_space.as_slice()
    }

    fn write_config(&mut self, _offset: u64, _data: &[u8]) {
        // The fs config space (mount tag and number of request queues) is
        // immutable; the virtio-fs specification defines no writable fields.
    }

    fn activate(
        &mut self,
        mem: GuestMemoryMmap,
        interrupt: Arc<dyn VirtioInterrupt>,
    ) -> Result<(), ActivateError> {
        assert!(!self.is_activated());

        for q in self.queues.iter_mut() {
            q.initialize(&mem)
                .map_err(ActivateError::QueueMemoryError)?;
        }

        let start_time = get_time_us(ClockType::Monotonic);
        // Setting features again, because now we negotiated them
        // with guest driver as well.
        self.vu_handle
            .set_features(self.acked_features)
            .map_err(|err| {
                self.metrics.activate_fails.inc();
                ActivateError::VhostUser(err)
            })?;

        // A device restored from a snapshot carries the captured backend
        // state blob: load it into the fresh backend now, before the vrings
        // are set up and enabled. The backend's state restore is
        // independent of the memory table and vrings, but request
        // processing must only start after the state is in place.
        if let Some(blob) = self.backend_state.take() {
            self.load_backend_state(&blob).map_err(|err| {
                self.metrics.activate_fails.inc();
                ActivateError::VhostUserFs(err)
            })?;
        }

        // All queues - the hiprio queue at index 0 and the request
        // queues after it - are handed over to the backend. The
        // frontend never parses FUSE frames, it only ferries vring
        // buffers.
        let queues: Vec<(usize, &Queue, &EventFd)> = self
            .queues
            .iter()
            .zip(self.queue_evts.iter())
            .enumerate()
            .map(|(index, (queue, queue_evt))| (index, queue, queue_evt))
            .collect();
        self.vu_handle
            .setup_backend(&mem, &queues, interrupt.clone())
            .map_err(|err| {
                self.metrics.activate_fails.inc();
                ActivateError::VhostUser(err)
            })?;
        self.device_state = DeviceState::Activated(ActiveState { mem, interrupt });
        let delta_us = get_time_us(ClockType::Monotonic) - start_time;
        self.metrics.activate_time_us.store(delta_us);
        Ok(())
    }

    fn is_activated(&self) -> bool {
        self.device_state.is_activated()
    }

    fn deactivate(&mut self) {
        self.device_state = DeviceState::Inactive;
    }

    fn _reset(&mut self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::undocumented_unsafe_blocks)]

    use std::os::unix::net::UnixStream;

    use event_manager::{EventOps, Events, MutEventSubscriber};
    use vhost::{VhostUserMemoryRegionInfo, VringConfigData};
    use vmm_sys_util::tempfile::TempFile;

    use super::*;
    use crate::devices::virtio::test_utils::{VirtQueue, default_interrupt};
    use crate::devices::virtio::vhost_user::tests::create_mem;
    use crate::test_utils::create_tmp_socket;
    use crate::vstate::memory::GuestAddress;

    fn expected_config_space(tag: &str, num_request_queues: u16) -> Vec<u8> {
        build_config_space(tag, num_request_queues)
    }

    #[test]
    fn test_from_config() {
        // Tag and request queue count fall back to defaults.
        let fs_config = FsDeviceConfig {
            fs_id: "test_fs".to_string(),
            socket: "sock".to_string(),
            tag: None,
            num_request_queues: None,
        };
        let config = VhostUserFsConfig::from(&fs_config);
        assert_eq!(config.fs_id, "test_fs");
        assert_eq!(config.socket, "sock");
        assert_eq!(config.tag, "test_fs");
        assert_eq!(config.num_request_queues, DEFAULT_NUM_REQUEST_QUEUES);

        // Explicit values are kept as-is.
        let fs_config = FsDeviceConfig {
            fs_id: "test_fs".to_string(),
            socket: "sock".to_string(),
            tag: Some("my_tag".to_string()),
            num_request_queues: Some(4),
        };
        let config = VhostUserFsConfig::from(&fs_config);
        assert_eq!(config.tag, "my_tag");
        assert_eq!(config.num_request_queues, 4);
    }

    #[test]
    fn test_config_space() {
        // Default tag and request queue count.
        let config_space = expected_config_space("test_fs", 1);
        assert_eq!(config_space.len(), FS_CONFIG_SPACE_SIZE);
        assert_eq!(&config_space[..7], b"test_fs");
        assert_eq!(&config_space[7..TAG_LEN], &[0u8; TAG_LEN - 7]);
        assert_eq!(&config_space[TAG_LEN..], &1u16.to_le_bytes());

        // A tag of 35 bytes is not truncated.
        let tag = "a".repeat(TAG_LEN - 1);
        let config_space = expected_config_space(&tag, 2);
        assert_eq!(&config_space[..TAG_LEN - 1], tag.as_bytes());
        assert_eq!(config_space[TAG_LEN - 1], 0);
        assert_eq!(&config_space[TAG_LEN..], &2u16.to_le_bytes());

        // A tag longer than 35 bytes is truncated and NUL-terminated.
        let tag = "a".repeat(2 * TAG_LEN);
        let config_space = expected_config_space(&tag, 2);
        assert_eq!(&config_space[..TAG_LEN - 1], &tag.as_bytes()[..TAG_LEN - 1]);
        assert_eq!(config_space[TAG_LEN - 1], 0);
    }

    #[test]
    fn test_new_no_features() {
        struct MockMaster {
            sock: UnixStream,
            max_queue_num: u64,
            is_owner: std::cell::UnsafeCell<bool>,
            features: u64,
            protocol_features: VhostUserProtocolFeatures,
            hdr_flags: std::cell::UnsafeCell<VhostUserHeaderFlag>,
        }

        impl VhostUserHandleBackend for MockMaster {
            fn from_stream(sock: UnixStream, max_queue_num: u64) -> Self {
                Self {
                    sock,
                    max_queue_num,
                    is_owner: std::cell::UnsafeCell::new(false),
                    features: 0,
                    protocol_features: VhostUserProtocolFeatures::empty(),
                    hdr_flags: std::cell::UnsafeCell::new(VhostUserHeaderFlag::empty()),
                }
            }

            fn set_owner(&self) -> Result<(), vhost::Error> {
                unsafe { *self.is_owner.get() = true };
                Ok(())
            }

            fn set_hdr_flags(&self, flags: VhostUserHeaderFlag) {
                unsafe { *self.hdr_flags.get() = flags };
            }

            fn get_features(&self) -> Result<u64, vhost::Error> {
                Ok(self.features)
            }

            fn get_protocol_features(&mut self) -> Result<VhostUserProtocolFeatures, vhost::Error> {
                Ok(self.protocol_features)
            }

            fn set_protocol_features(
                &mut self,
                features: VhostUserProtocolFeatures,
            ) -> Result<(), vhost::Error> {
                self.protocol_features = features;
                Ok(())
            }
        }

        impl MutEventSubscriber for VhostUserFsImpl<MockMaster> {
            fn process(&mut self, _: Events, _: &mut EventOps) {}
            fn init(&mut self, _: &mut EventOps) {}
        }

        let (_tmp_dir, tmp_socket_path) = create_tmp_socket();

        let vhost_fs_config = VhostUserFsConfig {
            fs_id: "test_fs".to_string(),
            socket: tmp_socket_path.clone(),
            tag: "test_fs".to_string(),
            num_request_queues: 1,
        };
        let vhost_fs = VhostUserFsImpl::<MockMaster>::new(vhost_fs_config).unwrap();

        // If backend has no features, nothing should be negotiated and
        // no flags should be set. The config space is built locally, so it
        // is populated regardless of the backend features.
        assert_eq!(
            vhost_fs
                .vu_handle
                .vu
                .sock
                .peer_addr()
                .unwrap()
                .as_pathname()
                .unwrap()
                .to_str()
                .unwrap(),
            &tmp_socket_path,
        );
        assert_eq!(vhost_fs.vu_handle.vu.max_queue_num, 2);
        assert!(unsafe { *vhost_fs.vu_handle.vu.is_owner.get() });
        assert_eq!(vhost_fs.avail_features, 0);
        assert_eq!(vhost_fs.acked_features, 0);
        assert_eq!(vhost_fs.vu_acked_protocol_features, 0);
        assert!(!vhost_fs.snapshot_capable());
        assert_eq!(
            unsafe { &*vhost_fs.vu_handle.vu.hdr_flags.get() }.bits(),
            VhostUserHeaderFlag::empty().bits()
        );
        assert_eq!(vhost_fs.config_space, expected_config_space("test_fs", 1));
    }

    #[test]
    fn test_new_all_features() {
        struct MockMaster {
            sock: UnixStream,
            max_queue_num: u64,
            is_owner: std::cell::UnsafeCell<bool>,
            features: u64,
            protocol_features: VhostUserProtocolFeatures,
            hdr_flags: std::cell::UnsafeCell<VhostUserHeaderFlag>,
        }

        impl VhostUserHandleBackend for MockMaster {
            fn from_stream(sock: UnixStream, max_queue_num: u64) -> Self {
                Self {
                    sock,
                    max_queue_num,
                    is_owner: std::cell::UnsafeCell::new(false),
                    features: AVAILABLE_FEATURES,
                    protocol_features: VhostUserProtocolFeatures::all(),
                    hdr_flags: std::cell::UnsafeCell::new(VhostUserHeaderFlag::empty()),
                }
            }

            fn set_owner(&self) -> Result<(), vhost::Error> {
                unsafe { *self.is_owner.get() = true };
                Ok(())
            }

            fn set_hdr_flags(&self, flags: VhostUserHeaderFlag) {
                unsafe { *self.hdr_flags.get() = flags };
            }

            fn get_features(&self) -> Result<u64, vhost::Error> {
                Ok(self.features)
            }

            fn get_protocol_features(&mut self) -> Result<VhostUserProtocolFeatures, vhost::Error> {
                Ok(self.protocol_features)
            }

            fn set_protocol_features(
                &mut self,
                features: VhostUserProtocolFeatures,
            ) -> Result<(), vhost::Error> {
                self.protocol_features = features;
                Ok(())
            }
        }

        impl MutEventSubscriber for VhostUserFsImpl<MockMaster> {
            fn process(&mut self, _: Events, _: &mut EventOps) {}
            fn init(&mut self, _: &mut EventOps) {}
        }

        let (_tmp_dir, tmp_socket_path) = create_tmp_socket();

        let vhost_fs_config = VhostUserFsConfig {
            fs_id: "test_fs".to_string(),
            socket: tmp_socket_path,
            tag: "my_tag".to_string(),
            num_request_queues: 3,
        };
        let mut vhost_fs = VhostUserFsImpl::<MockMaster>::new(vhost_fs_config).unwrap();

        // If backend has all features, features offered by the fs device
        // should be negotiated. DEVICE_STATE is the only requested protocol
        // feature, and the backend advertises it, so it is acked.
        assert_eq!(vhost_fs.vu_handle.vu.max_queue_num, 4);
        assert!(unsafe { *vhost_fs.vu_handle.vu.is_owner.get() });
        assert_eq!(vhost_fs.avail_features, AVAILABLE_FEATURES);
        assert_eq!(
            vhost_fs.acked_features,
            VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
        );
        assert_eq!(
            vhost_fs.vu_acked_protocol_features,
            VhostUserProtocolFeatures::DEVICE_STATE.bits()
        );
        assert!(vhost_fs.snapshot_capable());
        assert_eq!(
            unsafe { &*vhost_fs.vu_handle.vu.hdr_flags.get() }.bits(),
            VhostUserHeaderFlag::empty().bits()
        );

        // The hiprio queue at index 0 plus the request queues.
        assert_eq!(vhost_fs.queues().len(), 4);
        assert_eq!(vhost_fs.queue_events().len(), 4);
        assert!(
            vhost_fs
                .queues()
                .iter()
                .all(|q| q.max_size == QUEUE_SIZE && q.size == QUEUE_SIZE)
        );

        assert_eq!(vhost_fs.config_space, expected_config_space("my_tag", 3));

        // Test some `VirtioDevice` methods
        assert_eq!(vhost_fs.avail_features(), AVAILABLE_FEATURES);
        assert_eq!(
            vhost_fs.acked_features(),
            VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
        );

        // Valid read
        let mut read_config = vec![0; FS_CONFIG_SPACE_SIZE];
        vhost_fs.read_config(0, &mut read_config);
        assert_eq!(read_config, expected_config_space("my_tag", 3));

        // Invalid offset
        let mut read_config = vec![0; FS_CONFIG_SPACE_SIZE];
        vhost_fs.read_config(0x1000, &mut read_config);
        assert_eq!(read_config, vec![0; FS_CONFIG_SPACE_SIZE]);

        // Writing to the config does nothing
        vhost_fs.write_config(0, &[0x69; FS_CONFIG_SPACE_SIZE]);
        assert_eq!(vhost_fs.config_space, expected_config_space("my_tag", 3));

        // A tag longer than the config space tag field is truncated and
        // NUL-terminated.
        let (_tmp_dir, tmp_socket_path) = create_tmp_socket();
        let vhost_fs_config = VhostUserFsConfig {
            fs_id: "test_fs".to_string(),
            socket: tmp_socket_path,
            tag: "t".repeat(2 * TAG_LEN),
            num_request_queues: 1,
        };
        let vhost_fs = VhostUserFsImpl::<MockMaster>::new(vhost_fs_config).unwrap();
        assert_eq!(vhost_fs.tag.len(), 2 * TAG_LEN);
        assert_eq!(
            vhost_fs.config_space,
            expected_config_space(&"t".repeat(2 * TAG_LEN), 1)
        );

        // The device configuration can be retrieved for introspection.
        let config = vhost_fs.config();
        assert_eq!(config.fs_id, "test_fs");
        assert_eq!(config.tag, Some("t".repeat(2 * TAG_LEN)));
        assert_eq!(config.num_request_queues, Some(1));
    }

    #[test]
    fn test_activate() {
        struct MockMaster {
            features_are_set: std::cell::UnsafeCell<bool>,
            memory_is_set: std::cell::UnsafeCell<bool>,
            vrings_enabled: std::cell::UnsafeCell<Vec<usize>>,
        }

        impl VhostUserHandleBackend for MockMaster {
            fn from_stream(_sock: UnixStream, _max_queue_num: u64) -> Self {
                Self {
                    features_are_set: std::cell::UnsafeCell::new(false),
                    memory_is_set: std::cell::UnsafeCell::new(false),
                    vrings_enabled: std::cell::UnsafeCell::new(vec![]),
                }
            }

            fn set_owner(&self) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_hdr_flags(&self, _flags: VhostUserHeaderFlag) {}

            fn get_features(&self) -> Result<u64, vhost::Error> {
                Ok(0)
            }

            fn get_protocol_features(&mut self) -> Result<VhostUserProtocolFeatures, vhost::Error> {
                Ok(VhostUserProtocolFeatures::empty())
            }

            fn set_protocol_features(
                &mut self,
                _features: VhostUserProtocolFeatures,
            ) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_features(&self, _features: u64) -> Result<(), vhost::Error> {
                unsafe { (*self.features_are_set.get()) = true };
                Ok(())
            }

            fn set_mem_table(
                &self,
                _regions: &[VhostUserMemoryRegionInfo],
            ) -> Result<(), vhost::Error> {
                unsafe { (*self.memory_is_set.get()) = true };
                Ok(())
            }

            fn set_vring_num(&self, _queue_index: usize, _num: u16) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_vring_addr(
                &self,
                _queue_index: usize,
                _config_data: &VringConfigData,
            ) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_vring_base(&self, _queue_index: usize, _base: u16) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_vring_call(
                &self,
                _queue_index: usize,
                _fd: &EventFd,
            ) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_vring_kick(
                &self,
                _queue_index: usize,
                _fd: &EventFd,
            ) -> Result<(), vhost::Error> {
                Ok(())
            }

            fn set_vring_enable(
                &mut self,
                queue_index: usize,
                _enable: bool,
            ) -> Result<(), vhost::Error> {
                unsafe { (*self.vrings_enabled.get()).push(queue_index) };
                Ok(())
            }
        }

        impl MutEventSubscriber for VhostUserFsImpl<MockMaster> {
            fn process(&mut self, _: Events, _: &mut EventOps) {}
            fn init(&mut self, _: &mut EventOps) {}
        }

        // Fs device creation
        let (_tmp_dir, tmp_socket_path) = create_tmp_socket();
        let vhost_fs_config = VhostUserFsConfig {
            fs_id: "test_fs".to_string(),
            socket: tmp_socket_path,
            tag: "test_fs".to_string(),
            num_request_queues: 2,
        };
        let mut vhost_fs = VhostUserFsImpl::<MockMaster>::new(vhost_fs_config).unwrap();
        assert_eq!(vhost_fs.queues().len(), 3);

        // Memory creation
        let region_size = 0x10000;
        let file = TempFile::new().unwrap().into_file();
        file.set_len(region_size as u64).unwrap();
        let regions = vec![(GuestAddress(0x0), region_size)];
        let guest_memory = create_mem(file, &regions);
        for (i, queue) in vhost_fs.queues.iter_mut().enumerate() {
            let q = VirtQueue::new(GuestAddress((i * 0x2000) as u64), &guest_memory, 16);
            *queue = q.create_queue();
        }
        let interrupt = default_interrupt();

        // During activation of the device features, memory and all queues
        // (the hiprio queue at index 0 and the request queues after it)
        // should be set and activated.
        vhost_fs.activate(guest_memory, interrupt).unwrap();
        assert!(unsafe { *vhost_fs.vu_handle.vu.features_are_set.get() });
        assert!(unsafe { *vhost_fs.vu_handle.vu.memory_is_set.get() });
        assert_eq!(
            unsafe { &*vhost_fs.vu_handle.vu.vrings_enabled.get() },
            &[0, 1, 2]
        );
        assert!(vhost_fs.is_activated());
    }

    /// A mock backend that speaks the DEVICE_STATE transfer: on SAVE it
    /// writes a fake state blob into the passed fd (like the real backend,
    /// at the shared file offset), on LOAD it reads the blob back. All
    /// calls are recorded in a thread-local log so tests can assert on the
    /// ordering of the transfer relative to vring setup.
    const FAKE_BLOB: &[u8] = b"fake-backend-state-blob";

    thread_local! {
        static VU_EVENTS: std::cell::RefCell<Vec<String>> =
            const { std::cell::RefCell::new(Vec::new()) };
        static LOADED_BLOB: std::cell::RefCell<Vec<u8>> =
            const { std::cell::RefCell::new(Vec::new()) };
        static MOCK_PROTOCOL_FEATURES: std::cell::RefCell<VhostUserProtocolFeatures> =
            const { std::cell::RefCell::new(VhostUserProtocolFeatures::empty()) };
    }

    fn vu_events() -> Vec<String> {
        VU_EVENTS.with(|events| events.borrow().clone())
    }

    fn clear_vu_events() {
        VU_EVENTS.with(|events| events.borrow_mut().clear());
    }

    fn record_vu_event(event: String) {
        VU_EVENTS.with(|events| events.borrow_mut().push(event));
    }

    struct MockSnapshotMaster {
        protocol_features: VhostUserProtocolFeatures,
    }

    impl VhostUserHandleBackend for MockSnapshotMaster {
        fn from_stream(_sock: UnixStream, _max_queue_num: u64) -> Self {
            Self {
                protocol_features: MOCK_PROTOCOL_FEATURES.with(|features| *features.borrow()),
            }
        }

        fn set_owner(&self) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_hdr_flags(&self, _flags: VhostUserHeaderFlag) {}

        fn get_features(&self) -> Result<u64, vhost::Error> {
            Ok(AVAILABLE_FEATURES)
        }

        fn get_protocol_features(&mut self) -> Result<VhostUserProtocolFeatures, vhost::Error> {
            Ok(self.protocol_features)
        }

        fn set_protocol_features(
            &mut self,
            _features: VhostUserProtocolFeatures,
        ) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_features(&self, _features: u64) -> Result<(), vhost::Error> {
            record_vu_event("set_features".to_string());
            Ok(())
        }

        fn set_mem_table(
            &self,
            _regions: &[VhostUserMemoryRegionInfo],
        ) -> Result<(), vhost::Error> {
            record_vu_event("set_mem_table".to_string());
            Ok(())
        }

        fn set_vring_num(&self, queue_index: usize, _num: u16) -> Result<(), vhost::Error> {
            record_vu_event(format!("set_vring_num:{queue_index}"));
            Ok(())
        }

        fn set_vring_addr(
            &self,
            _queue_index: usize,
            _config_data: &VringConfigData,
        ) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_vring_base(&self, _queue_index: usize, _base: u16) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_vring_call(&self, _queue_index: usize, _fd: &EventFd) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_vring_kick(&self, _queue_index: usize, _fd: &EventFd) -> Result<(), vhost::Error> {
            Ok(())
        }

        fn set_vring_enable(
            &mut self,
            queue_index: usize,
            enable: bool,
        ) -> Result<(), vhost::Error> {
            record_vu_event(format!("set_vring_enable:{queue_index}:{enable}"));
            Ok(())
        }

        fn set_device_state_fd(
            &self,
            direction: VhostTransferStateDirection,
            phase: VhostTransferStatePhase,
            fd: std::os::fd::OwnedFd,
        ) -> Result<Option<std::fs::File>, vhost::Error> {
            assert_eq!(phase, VhostTransferStatePhase::STOPPED);
            record_vu_event(format!("set_device_state_fd:{direction:?}"));
            let mut file = std::fs::File::from(fd);
            match direction {
                VhostTransferStateDirection::SAVE => {
                    // The real backend writes its state blob at the shared
                    // file offset (0 for the fresh memfd).
                    file.write_all(FAKE_BLOB).unwrap();
                }
                VhostTransferStateDirection::LOAD => {
                    // The real backend reads the blob to EOF from the
                    // shared file offset.
                    let mut blob = Vec::new();
                    file.read_to_end(&mut blob).unwrap();
                    LOADED_BLOB.with(|loaded| *loaded.borrow_mut() = blob);
                }
            }
            Ok(None)
        }

        fn check_device_state(&self) -> Result<(), vhost::Error> {
            record_vu_event("check_device_state".to_string());
            Ok(())
        }
    }

    impl MutEventSubscriber for VhostUserFsImpl<MockSnapshotMaster> {
        fn process(&mut self, _: Events, _: &mut EventOps) {}
        fn init(&mut self, _: &mut EventOps) {}
    }

    /// The protocol features the mock advertises must be set before the
    /// device is constructed, because negotiation happens in `new`.
    fn snapshot_master_device(
        protocol_features: VhostUserProtocolFeatures,
        num_request_queues: u16,
    ) -> VhostUserFsImpl<MockSnapshotMaster> {
        MOCK_PROTOCOL_FEATURES.with(|features| *features.borrow_mut() = protocol_features);
        let (_tmp_dir, tmp_socket_path) = create_tmp_socket();
        let vhost_fs_config = VhostUserFsConfig {
            fs_id: "test_fs".to_string(),
            socket: tmp_socket_path,
            tag: "test_fs".to_string(),
            num_request_queues,
        };
        VhostUserFsImpl::<MockSnapshotMaster>::new(vhost_fs_config).unwrap()
    }

    fn activate_device(device: &mut VhostUserFsImpl<MockSnapshotMaster>) {
        let region_size = 0x10000;
        let file = TempFile::new().unwrap().into_file();
        file.set_len(region_size as u64).unwrap();
        let regions = vec![(GuestAddress(0x0), region_size)];
        let guest_memory = create_mem(file, &regions);
        for (i, queue) in device.queues.iter_mut().enumerate() {
            let q = VirtQueue::new(GuestAddress((i * 0x2000) as u64), &guest_memory, QUEUE_SIZE);
            *queue = q.create_queue();
        }
        device.activate(guest_memory, default_interrupt()).unwrap();
    }

    #[test]
    fn test_capture_backend_state() {
        use crate::snapshot::Persist;

        let mut device = snapshot_master_device(VhostUserProtocolFeatures::DEVICE_STATE, 1);
        assert!(device.snapshot_capable());
        activate_device(&mut device);

        clear_vu_events();
        device.capture_backend_state().unwrap();

        // The capture quiesces the backend (vrings disabled), runs the
        // SAVE transfer, waits for it (check_device_state) and re-enables
        // the vrings so the source VM keeps running.
        assert_eq!(
            vu_events(),
            vec![
                "set_vring_enable:0:false".to_string(),
                "set_vring_enable:1:false".to_string(),
                "set_device_state_fd:SAVE".to_string(),
                "check_device_state".to_string(),
                "set_vring_enable:0:true".to_string(),
                "set_vring_enable:1:true".to_string(),
            ]
        );
        assert_eq!(device.backend_state.as_deref(), Some(FAKE_BLOB));

        // The captured blob is embedded in the device state.
        let state = device.save();
        assert_eq!(state.id, "test_fs");
        assert_eq!(state.tag, "test_fs");
        assert_eq!(state.num_request_queues, 1);
        assert_eq!(state.socket_path, device.vu_handle.socket_path);
        assert_eq!(
            state.vu_acked_protocol_features,
            VhostUserProtocolFeatures::DEVICE_STATE.bits()
        );
        assert_eq!(state.backend_state, FAKE_BLOB);
        assert_eq!(state.virtio_state.queues.len(), 2);
        assert!(state.virtio_state.activated);
    }

    #[test]
    fn test_capture_backend_state_not_negotiated() {
        // A backend that did not ack DEVICE_STATE fails the capture
        // cleanly, without touching the vrings and without a panic.
        let mut device = snapshot_master_device(VhostUserProtocolFeatures::empty(), 1);
        assert!(!device.snapshot_capable());
        activate_device(&mut device);

        clear_vu_events();
        assert!(matches!(
            device.capture_backend_state(),
            Err(VhostUserFsError::DeviceStateNotNegotiated)
        ));
        assert!(device.backend_state.is_none());
        assert!(
            !vu_events()
                .iter()
                .any(|event| event.starts_with("set_device_state_fd"))
        );
    }

    #[test]
    fn test_capture_backend_state_inactive() {
        // A device that was never activated has no backend state to
        // capture; the snapshot carries an empty blob.
        use crate::snapshot::Persist;

        let mut device = snapshot_master_device(VhostUserProtocolFeatures::DEVICE_STATE, 1);
        device.capture_backend_state().unwrap();
        assert!(device.backend_state.is_none());
        assert!(device.save().backend_state.is_empty());
    }

    #[test]
    fn test_restore_loads_backend_state_before_vring_enable() {
        use crate::devices::virtio::fs::persist::{FsConstructorArgs, VhostUserFsState};
        use crate::snapshot::Persist;

        // Fabricate a device state carrying a backend blob. The device is
        // activated first so the saved queues are ready (as a real
        // snapshot's would be). The restore below reconnects, so it needs
        // its own socket: the tmp listener's backlog is 1 and is occupied
        // by the fabricating device's connection.
        let (_tmp_dir, tmp_socket_path) = create_tmp_socket();
        let (_tmp_dir2, tmp_socket_path2) = create_tmp_socket();
        MOCK_PROTOCOL_FEATURES
            .with(|features| *features.borrow_mut() = VhostUserProtocolFeatures::DEVICE_STATE);
        let mut device = VhostUserFsImpl::<MockSnapshotMaster>::new(VhostUserFsConfig {
            fs_id: "test_fs".to_string(),
            socket: tmp_socket_path,
            tag: "test_fs".to_string(),
            num_request_queues: 1,
        })
        .unwrap();
        activate_device(&mut device);
        let mut state: VhostUserFsState = device.save();
        state.socket_path = tmp_socket_path2;
        state.backend_state = FAKE_BLOB.to_vec();
        let serialized = bitcode::serialize(&state).unwrap();
        let state: VhostUserFsState = bitcode::deserialize(&serialized).unwrap();

        let region_size = 0x10000;
        let file = TempFile::new().unwrap().into_file();
        file.set_len(region_size as u64).unwrap();
        let guest_memory = create_mem(file, &[(GuestAddress(0x0), region_size)]);

        let mut restored = VhostUserFsImpl::<MockSnapshotMaster>::restore(
            FsConstructorArgs {
                mem: guest_memory.clone(),
            },
            &state,
        )
        .unwrap();
        assert_eq!(restored.backend_state.as_deref(), Some(FAKE_BLOB));
        assert!(!restored.is_activated());

        // On activation the blob must be loaded into the fresh backend
        // before the memory table and vrings are set up and enabled.
        clear_vu_events();
        restored
            .activate(guest_memory, default_interrupt())
            .unwrap();
        let events = vu_events();
        let event_index = |name: &str| {
            events
                .iter()
                .position(|event| event == name)
                .unwrap_or_else(|| panic!("missing event {name} in {events:?}"))
        };
        let load_index = event_index("set_device_state_fd:LOAD");
        assert!(event_index("set_features") < load_index);
        assert_eq!(event_index("check_device_state"), load_index + 1);
        assert!(load_index < event_index("set_mem_table"));
        assert!(load_index < event_index("set_vring_enable:0:true"));
        assert!(load_index < event_index("set_vring_enable:1:true"));

        // The backend received exactly the blob from the snapshot, and the
        // device consumed its stashed copy.
        LOADED_BLOB.with(|blob| assert_eq!(blob.borrow().as_slice(), FAKE_BLOB));
        assert!(restored.backend_state.is_none());
        assert!(restored.is_activated());
        assert_eq!(restored.id, "test_fs");
        assert_eq!(restored.num_request_queues, 1);
    }

    #[test]
    fn test_restore_without_device_state_fails() {
        use crate::devices::virtio::fs::persist::{FsConstructorArgs, VhostUserFsState};
        use crate::snapshot::Persist;

        // Fabricate a device state carrying a backend blob (captured while
        // DEVICE_STATE was available). The restore below reconnects, so it
        // needs its own socket: the tmp listener's backlog is 1 and is
        // occupied by the fabricating device's connection.
        let (_tmp_dir, tmp_socket_path) = create_tmp_socket();
        let (_tmp_dir2, tmp_socket_path2) = create_tmp_socket();
        MOCK_PROTOCOL_FEATURES
            .with(|features| *features.borrow_mut() = VhostUserProtocolFeatures::DEVICE_STATE);
        let device = VhostUserFsImpl::<MockSnapshotMaster>::new(VhostUserFsConfig {
            fs_id: "test_fs".to_string(),
            socket: tmp_socket_path,
            tag: "test_fs".to_string(),
            num_request_queues: 1,
        })
        .unwrap();
        let mut state: VhostUserFsState = device.save();
        state.socket_path = tmp_socket_path2;
        state.backend_state = FAKE_BLOB.to_vec();

        let region_size = 0x10000;
        let file = TempFile::new().unwrap().into_file();
        file.set_len(region_size as u64).unwrap();
        let guest_memory = create_mem(file, &[(GuestAddress(0x0), region_size)]);

        // The fresh backend does not advertise DEVICE_STATE, so the blob
        // cannot be loaded and the restore fails cleanly.
        MOCK_PROTOCOL_FEATURES
            .with(|features| *features.borrow_mut() = VhostUserProtocolFeatures::empty());
        let result = VhostUserFsImpl::<MockSnapshotMaster>::restore(
            FsConstructorArgs {
                mem: guest_memory.clone(),
            },
            &state,
        );
        assert!(matches!(
            result,
            Err(VhostUserFsError::DeviceStateNotNegotiated)
        ));

        // A state without a backend blob restores fine against the same
        // backend (nothing to load).
        state.backend_state = Vec::new();
        let restored = VhostUserFsImpl::<MockSnapshotMaster>::restore(
            FsConstructorArgs { mem: guest_memory },
            &state,
        )
        .unwrap();
        assert!(restored.backend_state.is_none());
    }
}
