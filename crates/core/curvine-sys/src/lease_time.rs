// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Local elapsed time for leases that must expire across host suspend.

use std::io;

/// Milliseconds on a suspend-aware monotonic clock since the local boot.
///
/// Values are local to this host and must not be compared across hosts or
/// persisted as wall-clock timestamps. Linux uses `CLOCK_BOOTTIME`, which
/// advances during process pauses and host suspend. Other platforms return
/// `Unsupported`; callers must disable protocols that require this clock.
/// This clock does not establish a bound on clock-rate drift between hosts.
pub fn lease_time_millis() -> io::Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let mut sample = std::mem::MaybeUninit::<libc::timespec>::uninit();
        // SAFETY: sample points to writable storage for one timespec.
        if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, sample.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful clock_gettime initialized both timespec fields.
        let sample = unsafe { sample.assume_init() };
        checked_millis(i128::from(sample.tv_sec), i128::from(sample.tv_nsec))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "suspend-aware lease clock requires Linux CLOCK_BOOTTIME",
        ))
    }
}

#[cfg(any(target_os = "linux", test))]
fn checked_millis(seconds: i128, nanoseconds: i128) -> io::Result<u64> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid lease clock sample");
    if !(0..1_000_000_000).contains(&nanoseconds) {
        return Err(invalid());
    }
    let seconds = u64::try_from(seconds).map_err(|_| invalid())?;
    let milliseconds = u64::try_from(nanoseconds / 1_000_000).map_err(|_| invalid())?;
    seconds
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(milliseconds))
        .ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_timespec_without_rounding_up_or_overflow() {
        assert_eq!(checked_millis(7, 123_999_999).unwrap(), 7_123);
        assert_eq!(checked_millis(0, 999_999).unwrap(), 0);
        let seconds = i128::from(u64::MAX / 1_000);
        let nanoseconds = i128::from(u64::MAX % 1_000) * 1_000_000;
        assert_eq!(checked_millis(seconds, nanoseconds).unwrap(), u64::MAX);
        assert!(checked_millis(seconds, nanoseconds + 1_000_000).is_err());
        assert!(checked_millis(seconds + 1, 0).is_err());
        for (seconds, nanoseconds) in [(-1, 0), (0, -1), (0, 1_000_000_000)] {
            assert_eq!(
                checked_millis(seconds, nanoseconds).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn repeated_clock_samples_never_go_backwards() {
        let mut previous = lease_time_millis().unwrap();
        for _ in 0..128 {
            let current = lease_time_millis().unwrap();
            assert!(current >= previous);
            previous = current;
        }
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn unsupported_platform_does_not_substitute_another_clock() {
        assert_eq!(
            lease_time_millis().unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }
}
