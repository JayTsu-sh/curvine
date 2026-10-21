//! Real-RocksDB regressions and opt-in metadata scale measurements.
//! No file payload, Raft, namespace traversal or end-to-end tiering is simulated
//! by this benchmark; use the cluster harness for those acceptance checks.
use super::*;
use crate::master::meta::store::{InodeWriteBatch, RocksInodeStore};
use curvine_rocksdb::DBConf;
use serde_json::json;
use std::time::Instant;

fn record(id: usize) -> Record {
    serde_json::from_value(json!({
        "ordinary":true,"native_blocks":true,"sealed":true,
        "version":1,"atime_ms":1000 + id as i64,"mtime_ms":1000,"state_since_ms":1000,
        "owner":if id % 100 == 0 {"selected"} else {"other"},"group":"users",
        "state":{"data":{"entity":{"storage":"scale","namespace":"metadata-only","entity":id.to_string()},"generation":1},
            "revision":1,"residency":"Online","length":if id % 1000 == 0 {8*1024*1024} else {1024},
            "digest":{"algorithm":"blake3","value":vec![1;32]},"anchors":[]},
        "path":format!("/{}/{id}",if id % 10 == 0 {"selected"} else {"other"}),
        "published":null,"pending":null,"views":{},"garbage":[]
    })).unwrap()
}
fn policy() -> LifecyclePolicy {
    serde_json::from_value(
        json!({"id":"scale","revision":1,"enabled":true,"prefix":"/",
        "min_size":0,"max_size":null,"min_residency_ms":0,"action":"release","backend":"local"}),
    )
    .unwrap()
}
fn open_store(dir: &std::path::Path) -> CommonResult<RocksInodeStore> {
    RocksInodeStore::new(DBConf::new(dir.to_string_lossy().into_owned()), true)
}
fn persist(store: &RocksInodeStore, old: Option<&Record>, new: &Record) -> CommonResult<u64> {
    let before = store.db.get_db().latest_sequence_number();
    let mut batch = store.new_batch();
    ordinary::write_record(&mut batch, old, new)?;
    batch.commit()?;
    Ok(store.db.get_db().latest_sequence_number() - before)
}
#[test]
fn record_updates_only_rewrite_changed_derived_keys() -> CommonResult<()> {
    let dir = tempfile::tempdir()?;
    let store = open_store(dir.path())?;
    let first = record(1001);
    assert_eq!(persist(&store, None, &first)?, 2);
    let mut next = first.clone();
    next.version += 1;
    next.state.revision += 1;
    assert_eq!(persist(&store, Some(&first), &next)?, 1);
    let old = next.clone();
    next.atime_ms += 100;
    next.version += 1;
    assert_eq!(persist(&store, Some(&old), &next)?, 3);
    assert!(store.hsm_get(&ordinary::index_key(&old))?.is_none());
    assert!(store.hsm_get(&ordinary::index_key(&next))?.is_some());
    let got: Record = decode(&store.hsm_get(&record_key(1001))?.unwrap())?;
    assert_eq!(got.version, next.version);
    assert_eq!(got.atime_ms, next.atime_ms);
    let old = next.clone();
    next.state.residency = Residency::Dual;
    assert_eq!(persist(&store, Some(&old), &next)?, 3);
    assert!(store.hsm_get(&ordinary::index_key(&old))?.is_none());
    assert!(store.hsm_get(&ordinary::index_key(&next))?.is_some());
    Ok(())
}
#[test]
fn changed_seal_and_gc_facts_keep_their_recovery_indexes() -> CommonResult<()> {
    let dir = tempfile::tempdir()?;
    let store = open_store(dir.path())?;
    let mut current = record(1001);
    persist(&store, None, &current)?;
    let old = current.clone();
    current.sealed = false;
    current.seal_token = Some("test-seal".into());
    assert_eq!(persist(&store, Some(&old), &current)?, 2);
    let old = current.clone();
    current.version += 1;
    assert_eq!(persist(&store, Some(&old), &current)?, 1);
    let old = current.clone();
    current.state.data.generation += 1;
    assert_eq!(persist(&store, Some(&old), &current)?, 2);
    let seal: DataKey = decode(&store.hsm_get(b"\x03hsm:seal:1001")?.unwrap())?;
    assert_eq!(seal, current.state.data);
    let old = current.clone();
    current.native_garbage.insert(42, vec![]);
    assert_eq!(persist(&store, Some(&old), &current)?, 2);
    let old = current.clone();
    current.native_garbage.insert(43, vec![]);
    assert_eq!(persist(&store, Some(&old), &current)?, 1);
    assert!(store.hsm_get(b"\x03hsm:gc:1001")?.is_some());
    let old = current.clone();
    current.native_garbage.clear();
    current.sealed = true;
    current.seal_token = None;
    assert_eq!(persist(&store, Some(&old), &current)?, 3);
    assert!(store.hsm_get(b"\x03hsm:seal:1001")?.is_none());
    assert!(store.hsm_get(b"\x03hsm:gc:1001")?.is_none());
    Ok(())
}
#[test]
fn metadata_rejections_skip_path_resolution_but_eligible_records_use_current_path() {
    let mut p = policy();
    let mut r = record(1001);
    p.min_size = 1024 * 1024;
    assert!(!lifecycle::selected_current_path(
        &p,
        &mut r,
        1_000_000,
        || panic!("small file must not resolve a path")
    ));
    p.min_size = 0;
    p.selection.owner = Some("selected".into());
    assert!(!lifecycle::selected_current_path(
        &p,
        &mut r,
        1_000_000,
        || panic!("wrong owner must not resolve a path")
    ));
    p.selection.owner = None;
    p.prefix = "/selected/".into();
    p.selection.exclude_prefixes = vec!["/selected/excluded/".into()];
    assert!(lifecycle::selected_current_path(
        &p,
        &mut r,
        1_000_000,
        || Some("/selected/moved".into())
    ));
    assert_eq!(r.path, "/selected/moved");
    assert!(!lifecycle::selected_current_path(
        &p,
        &mut r,
        1_000_000,
        || Some("/selected/excluded/moved".into())
    ));
    assert!(!lifecycle::selected_current_path(
        &p,
        &mut r,
        1_000_000,
        || None
    ));
}

// Preserve the published 039631f4 algorithms only inside this benchmark, so
// measurements can compare the same DB, fixture and process configuration.
fn legacy_write(
    batch: &mut InodeWriteBatch,
    old: Option<&Record>,
    new: &Record,
) -> CommonResult<()> {
    if let Some(old) = old {
        batch.hsm_delete(&ordinary::index_key(old))?;
    }
    batch.hsm_put(
        &record_key(new.state.data.entity.entity.parse()?),
        &pack(new)?,
    )?;
    batch.hsm_put(
        &ordinary::index_key(new),
        &pack(&new.state.data.entity.entity)?,
    )?;
    let seal = format!("\u{3}hsm:seal:{}", new.state.data.entity.entity);
    let gc = format!("\u{3}hsm:gc:{}", new.state.data.entity.entity);
    if new.seal_token.is_some() && !new.sealed {
        batch.hsm_put(seal.as_bytes(), &pack(&new.state.data)?)?;
    } else {
        batch.hsm_delete(seal.as_bytes())?;
    }
    if new.garbage.is_empty() && new.native_garbage.is_empty() {
        batch.hsm_delete(gc.as_bytes())?;
    } else {
        batch.hsm_put(gc.as_bytes(), &pack(&new.state.data.entity)?)?;
    }
    Ok(())
}
fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(0)
}
fn process_write_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("write_bytes: ").and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

#[test]
#[ignore = "explicit real-RocksDB scale benchmark; HSM_SCALE_RECORDS defaults to one million"]
fn hsm_metadata_scale_benchmark() -> CommonResult<()> {
    let n = std::env::var("HSM_SCALE_RECORDS")
        .unwrap_or_else(|_| "1000000".into())
        .parse::<usize>()?;
    assert!((1..=10_000_000).contains(&n));
    let variant = std::env::var("HSM_SCALE_VARIANT").unwrap_or_else(|_| "optimized".into());
    assert!(variant == "optimized" || variant == "baseline");
    let baseline = variant == "baseline";
    let dir = tempfile::tempdir()?;
    let store = open_store(dir.path())?;
    let started = Instant::now();
    for first in (0..n).step_by(1024) {
        let mut batch = store.new_batch();
        for id in first..(first + 1024).min(n) {
            let r = record(id);
            ordinary::write_record(&mut batch, None, &r)?;
        }
        batch.commit()?;
    }
    store.db.flush(true)?;
    let seed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let mut report = json!({"kind":"real-rocksdb-metadata-only","variant":variant,"records":n,"seed_ms":seed_ms,"scan_page":256,"results":[]});
    let updates = std::env::var("HSM_SCALE_UPDATES")
        .unwrap_or_else(|_| "50000".into())
        .parse::<usize>()?;
    for atime in [false, true] {
        let seq = store.db.get_db().latest_sequence_number();
        let io = process_write_bytes();
        let started = Instant::now();
        for i in 0..updates {
            let id = i % n.min(1024);
            let old: Record = decode(&store.hsm_get(&record_key(id as i64))?.unwrap())?;
            let mut new = old.clone();
            new.version += 1;
            new.state.revision += 1;
            if atime {
                new.atime_ms += 1;
            }
            let mut batch = store.new_batch();
            if baseline {
                legacy_write(&mut batch, Some(&old), &new)?;
            } else {
                ordinary::write_record(&mut batch, Some(&old), &new)?;
            }
            batch.commit()?;
        }
        store.db.flush(true)?;
        let mutations = store.db.get_db().latest_sequence_number() - seq;
        assert_eq!(
            mutations,
            updates as u64
                * if baseline {
                    5
                } else if atime {
                    3
                } else {
                    1
                }
        );
        report["results"].as_array_mut().unwrap().push(json!({"operation":if atime {"atime_update"} else {"same_sort_key_update"},"updates":updates,"ms":started.elapsed().as_secs_f64()*1000.0,"kv_mutations":mutations,"process_write_bytes":process_write_bytes().saturating_sub(io)}));
    }
    for case in [
        "all",
        "size_zero",
        "size_0.1_percent",
        "owner_1_percent",
        "path_10_percent",
    ] {
        let mut p = policy();
        match case {
            "size_zero" => p.min_size = 64 * 1024 * 1024,
            "size_0.1_percent" => p.min_size = 4 * 1024 * 1024,
            "owner_1_percent" => p.selection.owner = Some("selected".into()),
            "path_10_percent" => p.prefix = "/selected/".into(),
            _ => (),
        }
        let mut cursor = None;
        let mut examined = 0;
        let mut paths = 0;
        let mut selected = 0;
        let started = Instant::now();
        loop {
            let page = store.hsm_scan(b"\x03hsm:candidate:o:", cursor.as_deref(), 257)?;
            let more = page.len() > 256;
            for (key, value) in page.iter().take(256) {
                let id: String = decode(value)?;
                let mut r: Record = decode(&store.hsm_get(&record_key(id.parse()?))?.unwrap())?;
                examined += 1;
                let eligible = if baseline {
                    paths += 1;
                    lifecycle::selected_metadata(&p, &r, 1_000_000_000)
                        && r.path.starts_with(&p.prefix)
                        && !p
                            .selection
                            .exclude_prefixes
                            .iter()
                            .any(|e| r.path.starts_with(e))
                } else {
                    let path = r.path.clone();
                    lifecycle::selected_current_path(&p, &mut r, 1_000_000_000, || {
                        paths += 1;
                        Some(path)
                    })
                };
                if eligible {
                    selected += 1;
                }
                cursor = Some(key.clone());
            }
            if !more {
                break;
            }
        }
        assert_eq!(examined, n);
        let expected = match case {
            "size_zero" => 0,
            "size_0.1_percent" => (n - 1) / 1000 + 1,
            "owner_1_percent" => (n - 1) / 100 + 1,
            "path_10_percent" => (n - 1) / 10 + 1,
            _ => n,
        };
        assert_eq!(selected, expected);
        if !baseline && case == "size_zero" {
            assert_eq!(paths, 0);
        }
        report["results"].as_array_mut().unwrap().push(json!({"operation":"scan","case":case,"examined":examined,"selected":selected,"path_callbacks":paths,"ms":started.elapsed().as_secs_f64()*1000.0,"note":"callback counts only; no native namespace traversal or Raft admission"}));
    }
    // Isolate reset RSS sampling from earlier allocation high-water marks.
    use std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    };
    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicU64::new(rss_kib()));
    let thread = {
        let stop = stop.clone();
        let peak = peak.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(rss_kib(), Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        })
    };
    let before_rss = rss_kib();
    let started = Instant::now();
    let reset = if baseline {
        let keys = store
            .db
            .prefix_scan(RocksInodeStore::CF_COMMON, b"\x03hsm:")?
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<Vec<_>, _>>()?;
        let mut batch = store.new_batch();
        let count = keys.len();
        for key in keys {
            batch.hsm_delete(&key)?;
        }
        batch.commit()?;
        json!({"keys":count,"batches":1,"max_batch_keys":count})
    } else {
        let stats = store.reset_hsm_projection_bounded(1024, 256 * 1024)?;
        assert!(stats.max_batch_keys <= 1024 && stats.max_batch_bytes <= 256 * 1024);
        json!({"keys":stats.keys,"batches":stats.batches,"max_batch_keys":stats.max_batch_keys,"max_batch_bytes":stats.max_batch_bytes})
    };
    let reset_ms = started.elapsed().as_secs_f64() * 1000.0;
    stop.store(true, Ordering::Relaxed);
    thread.join().unwrap();
    assert_eq!(reset["keys"].as_u64(), Some(n as u64 * 2));
    assert!(store.hsm_scan(b"\x03hsm:", None, 1)?.is_empty());
    report["reset"] = reset;
    report["reset"]["ms"] = json!(reset_ms);
    report["reset"]["rss_before_kib"] = json!(before_rss);
    report["reset"]["sampled_peak_rss_kib"] = json!(peak.load(Ordering::Relaxed));
    if let Ok(output) = std::env::var("HSM_SCALE_OUTPUT") {
        std::fs::write(output, serde_json::to_vec_pretty(&report)?)?;
    }
    println!("HSM_SCALE_RESULT={}", serde_json::to_string(&report)?);
    Ok(())
}
