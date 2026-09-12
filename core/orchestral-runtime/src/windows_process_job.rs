//! Own Windows process trees for cancellation. A Job Object is not a sandbox.

use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};

use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};

pub(crate) struct ProcessJob(OwnedHandle);

impl ProcessJob {
    pub(crate) fn attach(process: RawHandle) -> io::Result<Self> {
        // SAFETY: null security/name creates an unnamed, non-inheritable job.
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateJobObjectW returned a new owned handle above.
        let job = Self(unsafe { OwnedHandle::from_raw_handle(raw) });
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: the job and caller-owned process handles are live, and the
        // information pointer/size refer to the matching initialized structure.
        unsafe {
            if SetInformationJobObject(
                job.0.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&limits) as u32,
            ) == 0
                || AssignProcessToJobObject(job.0.as_raw_handle(), process) == 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(job)
    }

    pub(crate) fn terminate(&self) {
        // SAFETY: self owns a live job handle until this method returns.
        unsafe {
            TerminateJobObject(self.0.as_raw_handle(), 1);
        }
    }
}
