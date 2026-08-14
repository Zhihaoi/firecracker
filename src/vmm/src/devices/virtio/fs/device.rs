// Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::ffi::c_void;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::os::fd::AsRawFd;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use utils::time::{ClockType, get_time_us};
use vhost::vhost_user::Frontend;
use vhost::vhost_user::message::*;
use vhost::vhost_user::{
    FrontendReqHandler, HandlerResult, VhostUserFrontendReqHandlerMut,
};
use vmm_sys_util::eventfd::EventFd;

use super::{NUM_HIPRIO_QUEUES, QUEUE_SIZE, TAG_LEN, VhostUserFsError};
use crate::devices::virtio::ActivateError;
use crate::devices::virtio::device::{
    ActiveState, DeviceState, ShmemRegion, VirtioDevice, VirtioDeviceType,
};
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
use crate::logger::{IncMetric, StoreMetric, error, info, log_dev_preview_warning};
use crate::utils::{mib_to_bytes, u64_to_usize};
use crate::vmm_config::fs::FsDeviceConfig;
use crate::vstate::memory::GuestMemoryMmap;
use crate::vstate::resources::AllocPolicy;
use crate::vstate::vm::KvmVm;
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
const BASE_REQUESTED_PROTOCOL_FEATURES: VhostUserProtocolFeatures =
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
    /// Size of the DAX cache window in MiB. If unset, DAX is disabled.
    pub dax_window_size_mib: Option<u64>,
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
            dax_window_size_mib: value.dax_window_size_mib,
        }
    }
}

pub type VhostUserFs = VhostUserFsImpl<Frontend>;

/// virtio-fs shared-memory capability id for the DAX cache window.
const VIRTIO_FS_SHMCAP_ID_CACHE: u8 = 0;

/// A 2 MiB-aligned DAX cache window backed by a memfd and mapped into
/// both host virtual address space and guest physical address space.
#[derive(Debug)]
pub struct DaxWindow {
    /// memfd backing the window.
    #[expect(dead_code)]
    memfd: memfd::Memfd,
    /// Host virtual address of the start of the window.
    host_addr: *mut c_void,
    /// Guest physical address of the start of the window.
    gpa: u64,
    /// Size of the window in bytes.
    size: u64,
    /// KVM slot used to map the GPA range.
    kvm_slot: u32,
}

// SAFETY: `DaxWindow` owns the mapped region and the raw pointer is not
// exposed for concurrent mutation.
unsafe impl Send for DaxWindow {}
// SAFETY: `DaxWindow` owns the mapped region and the raw pointer is not
// exposed for concurrent mutation.
unsafe impl Sync for DaxWindow {}

impl Drop for DaxWindow {
    fn drop(&mut self) {
        // SAFETY: the mapping was created with this exact address and size.
        unsafe {
            libc::munmap(self.host_addr, u64_to_usize(self.size));
        }
    }
}

/// Handler for vhost-user backend requests related to the DAX window.
#[derive(Debug)]
pub struct VhostUserFsReqHandler {
    window_host_addr: *mut c_void,
    window_size: u64,
}

// SAFETY: the raw pointer is to a host VA reservation owned by the
// associated `DaxWindow`, which outlives the handler thread.
unsafe impl Send for VhostUserFsReqHandler {}
// SAFETY: the raw pointer is to a host VA reservation owned by the
// associated `DaxWindow`, which outlives the handler thread.
unsafe impl Sync for VhostUserFsReqHandler {}

impl VhostUserFsReqHandler {
    fn validate_mmap_request(&self, req: &VhostUserMMap) -> Result<(u64, u64), std::io::Error> {
        const PAGE: u64 = 4096;
        if req.shmid != VIRTIO_FS_SHMCAP_ID_CACHE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "unsupported shmem id",
            ));
        }
        // Page granularity is the real constraint (mmap); the 2 MiB FUSE
        // mapping unit is the guest's own allocation granularity and does
        // not constrain the host-side map. The backend's tail clamps and
        // blob-cache offsets are page-aligned, not 2 MiB-aligned.
        if !req.shm_offset.is_multiple_of(PAGE)
            || !req.len.is_multiple_of(PAGE)
            || !req.fd_offset.is_multiple_of(PAGE)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "DAX window mapping must be page aligned",
            ));
        }
        let end = req
            .shm_offset
            .checked_add(req.len)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "overflow"))?;
        if end > self.window_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "DAX window mapping out of bounds",
            ));
        }
        Ok((req.shm_offset, req.len))
    }
}

impl VhostUserFrontendReqHandlerMut for VhostUserFsReqHandler {
    fn shmem_map(&mut self, req: &VhostUserMMap, fd: &dyn AsRawFd) -> HandlerResult<u64> {
        let (offset, len) = self.validate_mmap_request(req)?;
        // Read-only mappings map read-only: a MAP_SHARED|PROT_WRITE mmap of
        // the backend's read-only blobcache fd would fail EACCES.
        let prot = if req.flags & VhostUserMMapFlags::WRITABLE.bits() != 0 {
            libc::PROT_READ | libc::PROT_WRITE
        } else {
            libc::PROT_READ
        };
        let req_fd_offset = req.fd_offset;
        // Per-request chatter: debug! only — info! writes to stdout, which
        // IS the guest serial console in the harness (a map line landing
        // mid-ATVERB corrupts the verb protocol and strands the guest).
        debug!(
            "vhost-user-fs DAX map: window_host_addr={:#x} offset={:#x} len={:#x} fd={} fd_offset={:#x}",
            self.window_host_addr as u64, offset, len, fd.as_raw_fd(), req_fd_offset
        );
        // SAFETY: the validated range falls within the reserved host VA
        // reservation and is page aligned.
        let addr = unsafe {
            libc::mmap(
                self.window_host_addr
                    .cast::<u8>()
                    .add(u64_to_usize(offset))
                    .cast::<c_void>(),
                u64_to_usize(len),
                prot,
                libc::MAP_SHARED | libc::MAP_FIXED,
                fd.as_raw_fd(),
                libc::off_t::try_from(req.fd_offset).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "fd offset overflow")
                })?,
            )
        };
        if addr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(0)
    }

    fn shmem_unmap(&mut self, req: &VhostUserMMap) -> HandlerResult<u64> {
        let (offset, len) = self.validate_mmap_request(req)?;
        // SAFETY: the validated range falls within the reserved host VA
        // reservation. Replacing it with a PROT_NONE anonymous mapping keeps
        // the VA reservation intact.
        let addr = unsafe {
            libc::mmap(
                self.window_host_addr
                    .cast::<u8>()
                    .add(u64_to_usize(offset))
                    .cast::<c_void>(),
                u64_to_usize(len),
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED,
                -1,
                0,
            )
        };
        if addr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(0)
    }
}

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

    // DAX window state.
    pub(crate) dax_window_size_mib: Option<u64>,
    dax_window: Option<DaxWindow>,
    shmem_region: Option<ShmemRegion>,
    /// Thread serving backend requests for the DAX window.
    backend_req_thread: Option<JoinHandle<()>>,
    /// Eventfd used to stop the backend request thread.
    backend_req_stop: Option<EventFd>,
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
            .field("dax_window_size_mib", &self.dax_window_size_mib)
            .field("dax_window", &self.dax_window)
            .field("shmem_region", &self.shmem_region)
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
        // snapshotting. If a DAX window is configured, the backend must
        // support BACKEND_REQ and SHMEM so it can map file pages into the
        // window.
        let dax_enabled = config.dax_window_size_mib.is_some();
        let mut requested_protocol_features = BASE_REQUESTED_PROTOCOL_FEATURES;
        if dax_enabled {
            // REPLY_ACK is load-bearing, not optional: it makes the backend's
            // SHMEM_MAP wait for our mmap to complete before it answers the
            // guest's SETUPMAPPING — otherwise the guest can read the window
            // before the mapping lands (nondeterministic stale reads).
            requested_protocol_features |= VhostUserProtocolFeatures::BACKEND_REQ
                | VhostUserProtocolFeatures::SHMEM
                | VhostUserProtocolFeatures::REPLY_ACK;
        }

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

            dax_window_size_mib: config.dax_window_size_mib,
            dax_window: None,
            shmem_region: None,
            backend_req_thread: None,
            backend_req_stop: None,
        })
    }

    pub fn config(&self) -> FsDeviceConfig {
        FsDeviceConfig {
            fs_id: self.id.clone(),
            socket: self.vu_handle.socket_path.clone(),
            tag: Some(self.tag.clone()),
            num_request_queues: Some(self.num_request_queues),
            dax_window_size_mib: self.dax_window_size_mib,
        }
    }

    /// Create and register the DAX cache window. Must be called before the
    /// device is attached to the MMIO transport, so the shared-memory region
    /// can be exposed via the virtio 1.2 SHM registers. On snapshot restore,
    /// `at_gpa` carries the persisted GPA: the window is re-registered there
    /// exactly (the guest's mapping table names it) without re-allocating —
    /// the restored allocator already holds the range.
    pub fn create_dax_window(
        &mut self,
        vm: &KvmVm,
        at_gpa: Option<u64>,
    ) -> Result<(), VhostUserFsError> {
        let Some(size_mib) = self.dax_window_size_mib else {
            return Ok(());
        };
        if self.dax_window.is_some() {
            return Ok(());
        }

        let size = mib_to_bytes(u64_to_usize(size_mib)) as u64;
        let memfd = Self::create_memfd(size)?;
        let (host_addr, _host_size) = Self::map_window_host(size, memfd.as_file().as_raw_fd())?;

        let gpa = match at_gpa {
            // Restore path: the snapshot serializes the resource allocator
            // WITH this range already allocated (the boot path below took it
            // from past_mmio64_memory), so re-reserving it collides. Follow
            // the MMIO-device idiom instead: persisted resources are
            // re-registered, not re-allocated. Trust the persisted GPA.
            Some(gpa) => gpa,
            None => {
                let gpa_range = vm
                    .resource_allocator()
                    .past_mmio64_memory
                    .allocate(size, size, AllocPolicy::FirstMatch)
                    .map_err(VhostUserFsError::ResourceAllocator)?;
                gpa_range.start()
            }
        };

        let kvm_slot = vm
            .register_device_memory_region(gpa, host_addr as u64, size)
            .map_err(VhostUserFsError::Vm)?;

        self.dax_window = Some(DaxWindow {
            memfd,
            host_addr,
            gpa,
            size,
            kvm_slot,
        });
        self.shmem_region = Some(ShmemRegion {
            id: VIRTIO_FS_SHMCAP_ID_CACHE,
            len: size,
            gpa,
        });
        Ok(())
    }

    fn create_memfd(size: u64) -> Result<memfd::Memfd, VhostUserFsError> {
        let memfd = memfd::MemfdOptions::default()
            .create("vhost_user_fs_dax")
            .map_err(VhostUserFsError::Memfd)?;
        memfd
            .as_file()
            .set_len(size)
            .map_err(VhostUserFsError::MemfdSetLen)?;
        Ok(memfd)
    }

    /// Allocate a 2 MiB-aligned host VA range and map the memfd into it.
    fn map_window_host(size: u64, fd: libc::c_int) -> Result<(*mut c_void, usize), VhostUserFsError> {
        let align = mib_to_bytes(2);
        let size_usize = u64_to_usize(size);
        // Over-allocate by one alignment unit so that trimming head/tail
        // leaves a 2 MiB-aligned region of the requested size, regardless of
        // the host page size.
        let alloc_size = size_usize + align;
        // Reserve a large enough range that trimming head/tail leaves a
        // 2 MiB-aligned region of the requested size.
        // SAFETY: mmap with anonymous, non-backed mapping is safe when
        // requested size is non-zero and we check the returned pointer.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                alloc_size,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(VhostUserFsError::Mmap(std::io::Error::last_os_error()));
        }
        let aligned_ptr = ((ptr as usize + align - 1) & !(align - 1)) as *mut c_void;
        let head_size = aligned_ptr as usize - ptr as usize;
        let tail_size = alloc_size - head_size - size_usize;

        if head_size > 0 {
            // SAFETY: head is within the allocation we just created.
            unsafe {
                libc::munmap(ptr, head_size);
            }
        }
        if tail_size > 0 {
            // SAFETY: tail starts at the end of the desired region.
            unsafe {
                libc::munmap(
                    (aligned_ptr as usize + size_usize) as *mut c_void,
                    tail_size,
                );
            }
        }

        // Map the memfd into the aligned reservation.
        // SAFETY: aligned_ptr is a 2 MiB-aligned reservation of `size` bytes
        // that we own exclusively.
        let mapped = unsafe {
            libc::mmap(
                aligned_ptr,
                size_usize,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_FIXED,
                fd,
                0,
            )
        };
        if mapped == libc::MAP_FAILED {
            // Best-effort cleanup of the reservation before returning the error.
            // SAFETY: aligned_ptr is a valid reservation of `size` bytes.
            unsafe {
                libc::munmap(aligned_ptr, size_usize);
            }
            return Err(VhostUserFsError::Mmap(std::io::Error::last_os_error()));
        }
        assert_eq!(mapped, aligned_ptr);
        Ok((aligned_ptr, size_usize))
    }

    /// Guest physical address where the DAX window starts, if configured.
    pub fn dax_window_gpa(&self) -> Option<u64> {
        self.dax_window.as_ref().map(|w| w.gpa)
    }

    fn start_backend_req_handler(&mut self) -> Result<(), VhostUserFsError> {
        let window = self.dax_window.as_ref().ok_or_else(|| {
            VhostUserFsError::BackendReq(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "DAX window not created",
            ))
        })?;

        let handler = Arc::new(Mutex::new(VhostUserFsReqHandler {
            window_host_addr: window.host_addr,
            window_size: window.size,
        }));
        let mut frontend_handler =
            FrontendReqHandler::new(handler).map_err(VhostUserFsError::BackendReqVhost)?;
        // Answer NEED_REPLY requests when REPLY_ACK was negotiated: the
        // backend uses it so SHMEM_MAP waits for this mmap to complete —
        // the fence behind the restore-time re-map.
        frontend_handler.set_reply_ack_flag(
            self.vu_acked_protocol_features & VhostUserProtocolFeatures::REPLY_ACK.bits() != 0,
        );

        // Spawn the handler thread BEFORE handing the TX end to the
        // backend. set_backend_request_fd waits for the backend's ack, and
        // the backend's set_backend_req_fd handler synchronously re-maps
        // DAX ranges (SHMEM_MAP → waits for OUR reply) before it can ack:
        // spawn-after-send is a circular wait (gate13 v34 restore hang).
        // Requests can't arrive before the fd is sent, so polling early is
        // harmless.
        let stop = EventFd::new(libc::EFD_NONBLOCK).map_err(VhostUserFsError::EventFd)?;
        let stop_fd = stop.as_raw_fd();
        let handler_fd = frontend_handler.as_raw_fd();
        // AsRawFd on FrontendReqHandler yields the receiving (sub_sock) end,
        // which is what OUR handler thread polls — handing that to the
        // backend would strand every backend request unread on the other
        // socket half.
        let tx_fd = frontend_handler.get_tx_raw_fd();
        self.backend_req_stop = Some(stop);
        self.backend_req_thread = Some(std::thread::spawn(move || {
            Self::backend_req_thread(frontend_handler, handler_fd, stop_fd);
        }));

        self.vu_handle
            .vu
            .set_backend_request_fd(&tx_fd)
            .map_err(VhostUserFsError::Vhost)?;
        Ok(())
    }

    fn backend_req_thread(
        mut handler: FrontendReqHandler<Mutex<VhostUserFsReqHandler>>,
        handler_fd: libc::c_int,
        stop_fd: libc::c_int,
    ) {
        loop {
            let mut fds = [
                libc::pollfd {
                    fd: handler_fd,
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: stop_fd,
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            // SAFETY: fds is a valid array of two pollfd entries.
            let ret = unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) };
            if ret < 0 {
                break;
            }
            if fds[1].revents != 0 {
                break;
            }
            if fds[0].revents & libc::POLLIN != 0 {
                // A failed request (bad mmap params, decode error) must not
                // kill the channel — only a broken socket is fatal.
                match handler.handle_request() {
                    Ok(_) => {}
                    Err(e) => {
                        error!("vhost-user-fs DAX backend request failed: {e}");
                        if e.should_reconnect() {
                            break;
                        }
                    }
                }
            }
        }
    }

    fn stop_backend_req_handler(&mut self) {
        if let Some(stop) = self.backend_req_stop.take() {
            let _ = stop.write(1);
        }
        if let Some(thread) = self.backend_req_thread.take() {
            let _ = thread.join();
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

impl<T: VhostUserHandleBackend> Drop for VhostUserFsImpl<T> {
    fn drop(&mut self) {
        self.stop_backend_req_handler();
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

        // If a DAX window is configured, the backend needs the
        // backend-request channel so it can send SHMEM_MAP/SHMEM_UNMAP
        // requests before the guest issues FUSE_SETUPMAPPING.
        if self.dax_window.is_some() {
            self.start_backend_req_handler().map_err(|err| {
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
        // === AGENTVFS LOCAL CHANGE BEGIN: fence the vring-enable before any kick (Gate 12) ===
        // The vhost crate's wait_for_ack is a no-op unless the backend
        // negotiated REPLY_ACK, and our backend doesn't — so
        // SET_VRING_ENABLE is fire-and-forget here. The backend's worker
        // consumes a kick and drops it whenever the vring is not yet marked
        // enabled (vhost-user-backend event_loop.rs: read_kick(), then
        // `if !enabled { return }`), and resume_vm() artificially kicks the
        // devices right after activate() returns — a backend still chewing
        // through SET_MEM_TABLE/SET_VRING_* eats the restore's first kicks
        // FOREVER: the guest's request sits in the ring unprocessed
        // (reproduced under backend-freeze jitter as a restored guest
        // stalled pre-fs, the Gate 12 warmup-9-class flake). Re-querying
        // the protocol features is a reply-bearing request on this ordered
        // socket with no side effects on either end: waiting for the reply
        // proves the backend processed the enables. (GET_VRING_BASE would
        // also force a reply but stops the ring per spec.)
        self.vu_handle
            .get_protocol_features()
            .map_err(|err| {
                self.metrics.activate_fails.inc();
                ActivateError::VhostUser(err)
            })?;
        // === AGENTVFS LOCAL CHANGE END ===
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

    fn shmem_regions(&self) -> &[ShmemRegion] {
        self.shmem_region.as_slice()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::undocumented_unsafe_blocks)]

    use std::os::fd::FromRawFd;
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
            dax_window_size_mib: None,
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
            dax_window_size_mib: None,
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
            dax_window_size_mib: None,
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
            dax_window_size_mib: None,
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
            dax_window_size_mib: None,
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
            dax_window_size_mib: None,
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
            // === AGENTVFS LOCAL CHANGE BEGIN: record the fence query (Gate 12) ===
            record_vu_event("get_protocol_features".to_string());
            // === AGENTVFS LOCAL CHANGE END ===
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
            dax_window_size_mib: None,
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
            dax_window_size_mib: None,
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
                vm: None,
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

    // === AGENTVFS LOCAL CHANGE BEGIN: fence regression test (Gate 12) ===
    #[test]
    fn test_activate_fences_vring_enable_before_resume() {
        // The restore-time kick race (Gate 12 warmup-9-class flake): the
        // backend drops any kick its worker consumes before SET_VRING_ENABLE
        // is processed. activate() must therefore close the enable window
        // with a reply-forcing round trip AFTER the last SET_VRING_ENABLE —
        // pinned here against the mock's event log.
        let mut device = snapshot_master_device(VhostUserProtocolFeatures::DEVICE_STATE, 1);
        clear_vu_events();
        activate_device(&mut device);
        let events = vu_events();
        let last_enable = events
            .iter()
            .rposition(|event| event.starts_with("set_vring_enable"))
            .unwrap_or_else(|| panic!("no set_vring_enable in {events:?}"));
        let fence = events
            .iter()
            .rposition(|event| event == "get_protocol_features")
            .unwrap_or_else(|| panic!("missing fence get_protocol_features in {events:?}"));
        assert!(
            fence > last_enable,
            "the enable fence must follow the last set_vring_enable: {events:?}"
        );
        assert_eq!(fence, events.len() - 1, "the fence closes activate(): {events:?}");
    }
    // === AGENTVFS LOCAL CHANGE END ===

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
            dax_window_size_mib: None,
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
                vm: None,
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
            FsConstructorArgs {
                mem: guest_memory,
                vm: None,
            },
            &state,
        )
        .unwrap();
        assert!(restored.backend_state.is_none());
    }

    const TWO_MIB: u64 = 2 * 1024 * 1024;

    struct TestWindow {
        ptr: *mut c_void,
        size: usize,
    }

    impl TestWindow {
        fn new(size: usize) -> Self {
            // SAFETY: anonymous mmap with a non-zero size; we check the result.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    size,
                    libc::PROT_NONE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                    -1,
                    0,
                )
            };
            assert_ne!(ptr, libc::MAP_FAILED);
            Self { ptr, size }
        }

        fn handler(&self) -> VhostUserFsReqHandler {
            VhostUserFsReqHandler {
                window_host_addr: self.ptr,
                window_size: self.size as u64,
            }
        }
    }

    impl Drop for TestWindow {
        fn drop(&mut self) {
            // SAFETY: the mapping was created with this exact address and size.
            unsafe {
                libc::munmap(self.ptr, self.size);
            }
        }
    }

    struct BadFd;
    impl AsRawFd for BadFd {
        fn as_raw_fd(&self) -> i32 {
            -1
        }
    }

    fn mmap_req(shmid: u8, shm_offset: u64, len: u64) -> VhostUserMMap {
        VhostUserMMap {
            shmid,
            padding: [0; 7],
            fd_offset: 0,
            shm_offset,
            len,
            flags: 0,
        }
    }

    #[test]
    fn test_shmem_map_wrong_shmid() {
        let window = TestWindow::new(u64_to_usize(4 * TWO_MIB));
        let mut handler = window.handler();
        let req = mmap_req(1, 0, TWO_MIB);
        assert!(handler.shmem_map(&req, &BadFd).is_err());
    }

    #[test]
    fn test_shmem_map_misaligned() {
        let window = TestWindow::new(u64_to_usize(4 * TWO_MIB));
        let mut handler = window.handler();
        // shm_offset not 2 MiB aligned.
        let req = mmap_req(0, TWO_MIB + 1, TWO_MIB);
        assert!(handler.shmem_map(&req, &BadFd).is_err());
        // len not 2 MiB aligned.
        let req = mmap_req(0, 0, TWO_MIB + 1);
        assert!(handler.shmem_map(&req, &BadFd).is_err());
    }

    #[test]
    fn test_shmem_map_out_of_bounds() {
        let window = TestWindow::new(u64_to_usize(4 * TWO_MIB));
        let mut handler = window.handler();
        let req = mmap_req(0, 2 * TWO_MIB, 4 * TWO_MIB);
        assert!(handler.shmem_map(&req, &BadFd).is_err());
    }

    #[test]
    fn test_shmem_map_unmap_happy_path() {
        let window = TestWindow::new(u64_to_usize(4 * TWO_MIB));
        let mut handler = window.handler();
        let memfd = memfd::MemfdOptions::default()
            .create("test_dax")
            .unwrap();
        memfd.as_file().set_len(TWO_MIB).unwrap();

        let req = mmap_req(0, 0, TWO_MIB);
        assert_eq!(handler.shmem_map(&req, memfd.as_file()).unwrap(), 0);
        assert_eq!(handler.shmem_unmap(&req).unwrap(), 0);
    }

    /// Wire-level probe: feed the exact byte layout the vendored nydusd
    /// (vhost 0.15 + agentvfs patch) puts on the backend-req channel and
    /// verify handle_request parses it and the file content lands in the
    /// window.
    #[test]
    fn test_shmem_map_wire_compat() {
        let mut f = TempFile::new().unwrap().into_file();
        let pattern: Vec<u8> = (0..TWO_MIB as usize).map(|i| (i % 251) as u8).collect();
        f.write_all(&pattern).unwrap();
        f.flush().unwrap();

        let window = TestWindow::new(u64_to_usize(4 * TWO_MIB));
        let handler = std::sync::Arc::new(Mutex::new(window.handler()));
        let mut frontend = FrontendReqHandler::new(handler).unwrap();

        // Header {code=9 (SHMEM_MAP), flags=1 (version 1), size=40} then the
        // 40-byte VhostUserMMap {shmid 0, fd_offset 0, shm_offset 0,
        // len 2 MiB, flags 2 (FUSE READ)}, fd via SCM_RIGHTS.
        let mut hdr = [0u8; 12];
        hdr[0..4].copy_from_slice(&9u32.to_ne_bytes());
        hdr[4..8].copy_from_slice(&1u32.to_ne_bytes());
        hdr[8..12].copy_from_slice(&40u32.to_ne_bytes());
        let mut body = [0u8; 40];
        body[24..32].copy_from_slice(&TWO_MIB.to_ne_bytes());
        // flags = 0: the wire carries vhost-user VhostUserMMapFlags (WRITABLE
        // is the only defined bit); a read mapping is all zeros.

        let iov = [
            libc::iovec {
                iov_base: hdr.as_ptr() as *mut c_void,
                iov_len: hdr.len(),
            },
            libc::iovec {
                iov_base: body.as_ptr() as *mut c_void,
                iov_len: body.len(),
            },
        ];
        let mut cmsg_space = [0u8; 64];
        // SAFETY: all pointers valid and the cmsg buffer is large enough for
        // one fd.
        let sent = unsafe {
            let mut msg: libc::msghdr = std::mem::zeroed();
            msg.msg_iov = iov.as_ptr() as *mut libc::iovec;
            msg.msg_iovlen = iov.len();
            msg.msg_control = cmsg_space.as_mut_ptr() as *mut c_void;
            msg.msg_controllen = cmsg_space.len();
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(4) as usize;
            std::ptr::write(libc::CMSG_DATA(cmsg) as *mut i32, f.as_raw_fd());
            msg.msg_controllen = libc::CMSG_SPACE(4) as usize;
            libc::sendmsg(frontend.get_tx_raw_fd(), &msg, 0)
        };
        assert_eq!(sent, 52);

        // Stage-by-stage bisect of handle_request's validation.
        use vhost::vhost_user::message::VhostUserMsgValidator;
        let mmap = VhostUserMMap {
            shmid: 0,
            padding: [0; 7],
            fd_offset: 0,
            shm_offset: 0,
            len: TWO_MIB,
            flags: 0,
        };
        assert!(mmap.is_valid(), "mmap body invalid");

        frontend.handle_request().expect("handle_request");
        // SAFETY: the window is 4 MiB reserved; the first 2 MiB were mapped.
        let win = unsafe { std::slice::from_raw_parts(window.ptr as *const u8, TWO_MIB as usize) };
        assert_eq!(win, &pattern[..], "window must carry the file content");
    }
}
