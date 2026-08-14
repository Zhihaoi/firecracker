// Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

pub mod device;
pub mod event_handler;
pub mod persist;

use self::device::VhostUserFs;
use crate::devices::virtio::queue::FIRECRACKER_MAX_QUEUE_SIZE;
use crate::devices::virtio::vhost_user::VhostUserError;
use crate::vstate::interrupts::InterruptError;

/// Number of hiprio queues of the vhost-user fs device. The hiprio queue is
/// always the queue at index 0, all request queues follow it.
pub const NUM_HIPRIO_QUEUES: u64 = 1;

/// Queue size for the vhost-user fs device.
pub const QUEUE_SIZE: u16 = FIRECRACKER_MAX_QUEUE_SIZE;

/// Length in bytes of the tag field in the fs device config space.
pub const TAG_LEN: usize = 36;

/// Vhost-user fs device error.
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum VhostUserFsError {
    /// Cannot create config
    Config,
    /// The backend did not negotiate the DEVICE_STATE protocol feature, so
    /// its internal state cannot be transferred for snapshotting
    DeviceStateNotNegotiated,
    /// Error transferring the backend device state: {0}
    DeviceStateTransfer(std::io::Error),
    /// Error creating memfd for the backend device state transfer: {0}
    Memfd(#[from] memfd::Error),
    /// Error resizing memfd: {0}
    MemfdSetLen(std::io::Error),
    /// Error creating/mapping the DAX window: {0}
    Mmap(std::io::Error),
    /// Error allocating guest resources for the DAX window: {0}
    ResourceAllocator(vm_allocator::Error),
    /// Error registering the DAX window with KVM: {0}
    Vm(crate::vstate::vm::VmError),
    /// Error setting up the backend request channel: {0}
    BackendReq(std::io::Error),
    /// Error setting up the backend request channel: {0}
    BackendReqVhost(vhost::vhost_user::Error),
    /// Invalid DAX window state in snapshot: persisted GPA differs from
    /// restored allocation
    DaxWindowGpaMismatch,
    /// Invalid virtio state in snapshot: {0}
    VirtioState(#[from] crate::devices::virtio::persist::PersistError),
    /// Vhost-user error: {0}
    VhostUser(VhostUserError),
    /// Vhost error: {0}
    Vhost(vhost::Error),
    /// Error opening eventfd: {0}
    EventFd(std::io::Error),
    /// Error creating irqfd: {0}
    Interrupt(InterruptError),
}
