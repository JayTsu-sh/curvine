// Copyright 2026 OPPO. Licensed under the Apache License, Version 2.0.
//! Lease deadlines use a per-term monotonic timeline. The committed floor lets
//! a new leader start at or after earlier grants; a full lease grace protects
//! readers whose old leader had a different wall clock. This clock grants no
//! access itself: the reader rows still become authoritative only on apply.
use super::*;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(super) const FLOOR: &[u8] = b"\x03hsm:read-clock:v1";
pub(super) const LEASE_MS: i64 = 120_000;

// Field order is frozen by compact namespace journal v5. Add a new journal
// DTO/version before changing this serialized projection of the lease clock.
#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReadTime {
    pub(crate) now: i64,
    recovering: bool,
}
impl ReadTime {
    pub(crate) fn recovering(self) -> bool {
        self.recovering
    }
    // Runtime admission may conservatively delay reclamation. Keep active()
    // unchanged: compact namespace v5 replay uses its captured projection.
    pub(crate) fn protection(self) -> Self {
        Self {
            now: self.now.saturating_sub(1000),
            ..self
        }
    }
    pub(crate) fn active(self, expiry: i64) -> bool {
        self.recovering || expiry > self.now
    }
}

struct Epoch {
    term: u64,
    base: i64,
    started: Instant,
    fresh: bool,
}
#[derive(Default)]
pub(crate) struct ReadClock(Mutex<Option<Epoch>>);
impl ReadClock {
    fn sample(
        &self,
        term: u64,
        at: Instant,
        initial: impl FnOnce() -> CommonResult<i64>,
    ) -> CommonResult<ReadTime> {
        let mut epoch = self.0.lock().unwrap();
        if epoch.as_ref().is_none_or(|e| e.term != term) {
            *epoch = Some(Epoch {
                term,
                base: initial()?.max(0),
                started: at,
                fresh: false,
            });
        }
        let epoch = epoch.as_ref().unwrap();
        let elapsed = at.saturating_duration_since(epoch.started);
        Ok(ReadTime {
            now: epoch
                .base
                .saturating_add(elapsed.as_millis().min(i64::MAX as u128) as i64),
            recovering: !epoch.fresh && elapsed < Duration::from_millis(LEASE_MS as u64),
        })
    }
    pub(crate) fn mark_fresh(&self, term: u64) {
        if let Some(epoch) = self.0.lock().unwrap().as_mut() {
            if epoch.term == term {
                epoch.fresh = true;
            }
        }
    }
    pub(crate) fn reset(&self) {
        *self.0.lock().unwrap() = None;
    }
}
impl FsDir {
    pub(crate) fn hsm_read_time(&self) -> CommonResult<ReadTime> {
        self.hsm_read_clock
            .sample(self.journal_writer.current_term(), Instant::now(), || {
                let floor = self
                    .get_rocks_store()
                    .hsm_get(FLOOR)?
                    .map(|v| decode::<i64>(&v))
                    .transpose()?
                    .unwrap_or(0);
                Ok(floor.max(curvine_runtime::common::LocalTime::mills() as i64))
            })
    }
    pub(super) fn hsm_readers_active(&self, record: &Record) -> CommonResult<bool> {
        if record.readers.is_empty() {
            return Ok(false);
        }
        let time = self.hsm_read_time()?.protection();
        Ok(record.readers.values().any(|expiry| time.active(*expiry)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_margin_does_not_change_frozen_journal_clock_semantics() {
        let time = ReadTime {
            now: 120_000,
            recovering: false,
        };
        assert!(!time.active(119_500));
        assert!(time.protection().active(119_500));
        assert!(!time.protection().active(119_000));
    }
    #[test]
    fn recovery_is_global_and_only_owned_fresh_initialization_can_skip_it() -> CommonResult<()> {
        let clock = ReadClock::default();
        let start = Instant::now();
        assert!(clock.sample(7, start, || Ok(0))?.recovering());
        clock.mark_fresh(7);
        assert!(!clock.sample(7, start, || panic!())?.recovering());
        assert!(clock.sample(8, start, || Ok(0))?.recovering());
        clock.mark_fresh(7);
        assert!(clock.sample(8, start, || panic!())?.recovering());
        clock.reset();
        clock.mark_fresh(8);
        assert!(clock.sample(8, start, || Ok(0))?.recovering());
        assert!(!clock
            .sample(
                8,
                start + Duration::from_millis(LEASE_MS as u64),
                || panic!()
            )?
            .recovering());
        Ok(())
    }
    #[test]
    fn wall_clock_changes_cannot_shorten_a_grant_and_term_change_waits_a_full_lease(
    ) -> CommonResult<()> {
        let clock = ReadClock::default();
        let start = Instant::now();
        let initial = clock.sample(7, start, || Ok(10_000))?;
        assert_eq!(initial.now, 10_000);
        let grant = initial.now + LEASE_MS;
        // Neither a forward nor a backward wall adjustment is sampled again.
        let next = clock.sample(7, start + Duration::from_secs(30), || {
            panic!("wall clock resampled")
        })?;
        assert_eq!(next.now, 40_000);
        assert!(next.active(grant));
        assert!(!clock
            .sample(7, start + Duration::from_secs(121), || panic!())?
            .active(grant));
        let promoted = start + Duration::from_secs(122);
        assert!(clock.sample(8, promoted, || Ok(1_000_000))?.active(grant));
        assert!(clock
            .sample(8, promoted + Duration::from_secs(119), || panic!())?
            .active(grant));
        assert!(!clock
            .sample(8, promoted + Duration::from_secs(120), || panic!())?
            .active(grant));
        clock.reset();
        assert!(clock
            .sample(8, promoted + Duration::from_secs(121), || Ok(1))?
            .active(0));
        Ok(())
    }
}
