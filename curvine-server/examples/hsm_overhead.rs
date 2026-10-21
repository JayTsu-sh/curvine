//! Ordinary SDK performance comparison. Copy this identical example into the
//! pre-HSM worktree; enable hsm-scale-test only for the HSM build.
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use futures::{stream, StreamExt, TryStreamExt};
use serde_json::{json, Value};
use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug)]
enum Action {
    Write(usize),
    Read(usize),
    Open,
    Stat,
    List,
    Rename,
    Delete,
}

async fn action(fs: &CurvineFileSystem, path: &Path, op: Action) -> Result<u64> {
    match op {
        Action::Write(size) => {
            let mut writer = fs.create(path, false).await?;
            let block = vec![0x5a; size.min(1024 * 1024)];
            let mut left = size;
            while left > 0 {
                let n = left.min(block.len());
                writer.write(&block[..n]).await?;
                left -= n;
            }
            writer.complete().await?;
            Ok(size as u64)
        }
        Action::Read(size) => {
            let mut reader = fs.open(path).await?;
            let mut buffer = vec![0; 1024 * 1024];
            let mut total = 0;
            loop {
                let n = reader.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                if buffer[0] != 0x5a || buffer[n - 1] != 0x5a {
                    return Err("payload marker mismatch".into());
                }
                total += n;
            }
            reader.complete().await?;
            if total != size {
                return Err(format!("read length {total} != {size}").into());
            }
            Ok(total as u64)
        }
        Action::Open => {
            let mut reader = fs.open(path).await?;
            reader.complete().await?;
            Ok(0)
        }
        Action::Stat => {
            fs.get_status(path).await?;
            Ok(0)
        }
        Action::List => {
            if fs.list_status(path).await?.is_empty() {
                return Err("empty listing".into());
            }
            Ok(0)
        }
        Action::Rename => {
            if !fs
                .rename(path, &Path::from_str(format!("{}.renamed", path.path()))?)
                .await?
            {
                return Err("rename false".into());
            }
            Ok(0)
        }
        Action::Delete => {
            fs.delete(path, false).await?;
            Ok(0)
        }
    }
}
fn proc_stats() -> Result<Value> {
    let stat = fs::read_to_string("/proc/self/stat")?;
    let parts: Vec<_> = stat
        .rsplit_once(')')
        .ok_or("invalid process stat")?
        .1
        .split_whitespace()
        .collect();
    let status = fs::read_to_string("/proc/self/status")?;
    let kb = |name: &str| -> u64 {
        status
            .lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    };
    Ok(
        json!({"user_ticks":parts[11].parse::<u64>()?,"system_ticks":parts[12].parse::<u64>()?,"rss_kib":kb("VmRSS:"),"high_water_rss_kib":kb("VmHWM:")}),
    )
}
async fn measure(
    fs: &CurvineFileSystem,
    paths: &[Path],
    op: Action,
    count: usize,
    concurrency: usize,
    name: &str,
) -> Result<Value> {
    let before = proc_stats()?;
    #[cfg(feature = "hsm-scale-test")]
    let journal_before = curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
    let start = Instant::now();
    let mut samples: Vec<(f64, u64)> = stream::iter(0..count)
        .map(|i| async move {
            let start = Instant::now();
            let bytes = action(fs, &paths[i % paths.len()], op).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                start.elapsed().as_secs_f64() * 1000.,
                bytes,
            ))
        })
        .buffer_unordered(concurrency)
        .try_collect()
        .await?;
    let elapsed = start.elapsed().as_secs_f64();
    let after = proc_stats()?;
    let bytes: u64 = samples.iter().map(|s| s.1).sum();
    samples.sort_by(|a, b| a.0.total_cmp(&b.0));
    let p = |q: f64| samples[((count as f64 * q).ceil() as usize).saturating_sub(1)].0;
    let report = json!({"name":name,"concurrency":concurrency,"operations":count,"errors":0,"seconds":elapsed,"ops_per_sec":count as f64/elapsed,"bytes":bytes,"mib_per_sec":bytes as f64/1048576./elapsed,"latency_ms":{"mean":samples.iter().map(|s|s.0).sum::<f64>()/count as f64,"p50":p(0.5),"p95":p(0.95),"p99":p(0.99),"max":samples.last().unwrap().0},"process_before":before,"process_after":after});
    eprintln!("RESULT {}", report);
    #[cfg(feature = "hsm-scale-test")]
    let report = {
        let mut report = report;
        report["journal_before"] = journal_before;
        report["journal_after"] =
            curvine_master::master::Master::get_metrics()?.hsm_test_commit_stats();
        report
    };
    Ok(report)
}
async fn metadata(
    fs: &CurvineFileSystem,
    prefix: &str,
    count: usize,
    concurrency: usize,
) -> Result<Vec<Value>> {
    let root = Path::from_str(prefix)?;
    fs.mkdir(&root, true).await?;
    let paths = (0..count)
        .map(|i| Path::from_str(format!("{prefix}/{i}")))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut result = vec![];
    result.push(
        measure(
            fs,
            &paths,
            Action::Write(0),
            count,
            concurrency,
            "create_close_empty",
        )
        .await?,
    );
    result.push(
        measure(
            fs,
            &paths,
            Action::Stat,
            count * 4,
            concurrency,
            "get_status",
        )
        .await?,
    );
    result.push(
        measure(
            fs,
            &paths,
            Action::Open,
            count,
            concurrency,
            "open_close_empty",
        )
        .await?,
    );
    result.push(
        measure(
            fs,
            &[root.clone()],
            Action::List,
            64,
            concurrency,
            "list_children",
        )
        .await?,
    );
    result.push(measure(fs, &paths, Action::Rename, count, concurrency, "rename").await?);
    let renamed = paths
        .iter()
        .map(|p| Path::from_str(format!("{}.renamed", p.path())))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    result.push(measure(fs, &renamed, Action::Delete, count, concurrency, "delete").await?);
    fs.delete(&root, true).await?;
    Ok(result)
}
async fn data_io(
    fs: &CurvineFileSystem,
    prefix: &str,
    count: usize,
    size: usize,
    concurrency: usize,
) -> Result<Vec<Value>> {
    let root = Path::from_str(prefix)?;
    fs.mkdir(&root, true).await?;
    let paths = (0..count)
        .map(|i| Path::from_str(format!("{prefix}/{i}")))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let write = measure(
        fs,
        &paths,
        Action::Write(size),
        count,
        concurrency,
        if size < 1048576 {
            "write_4k"
        } else {
            "write_64m"
        },
    )
    .await?;
    let read = measure(
        fs,
        &paths,
        Action::Read(size),
        count * 3,
        concurrency,
        if size < 1048576 {
            "read_4k"
        } else {
            "read_64m"
        },
    )
    .await?;
    fs.delete(&root, true).await?;
    Ok(vec![write, read])
}
#[cfg(feature = "hsm-scale-test")]
async fn control(fs: &CurvineFileSystem, value: Value) -> Result<Value> {
    let response = fs
        .hsm_control(curvine_proto::HsmControlRequest {
            protocol_version: 1,
            payload: serde_json::to_vec(&value)?,
        })
        .await?;
    let value: Value = serde_json::from_slice(&response.payload)?;
    if value["kind"] == "error" {
        return Err(format!("HSM control {value}").into());
    }
    Ok(value)
}
#[cfg(feature = "hsm-scale-test")]
async fn scan_comparison(fs: &CurvineFileSystem, count: usize, seed_count: usize) -> Result<Value> {
    let paths = (0..seed_count)
        .map(|i| Path::from_str(format!("/scan-seed/{i}")))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    fs.mkdir(&Path::from_str("/scan-seed")?, true).await?;
    measure(
        fs,
        &paths,
        Action::Write(0),
        paths.len(),
        16,
        "scan_seed_untimed_setup",
    )
    .await?;
    let policy = json!({"id":"overhead-zero","revision":1,"enabled":true,"prefix":"/","min_size":u64::MAX,"max_size":null,"min_residency_ms":0,"action":"release","backend":"local","selection":{"scan_interval_ms":1000,"scan_budget":65536,"max_candidates":16,"access_age_weight":1,"min_access_age_ms":0,"min_modify_age_ms":0,"exclude_prefixes":[],"owner":null,"group":null,"capacity":null}});
    control(fs,json!({"op":"policy_put","context":{"operation_id":"overhead-policy","controller_epoch":1,"attempt_epoch":1},"policy":policy,"expected_revision":null})).await?;
    let idle = metadata(fs, "/scan-idle", count, 16).await?;
    let scan_fs = fs.clone();
    let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
    let scan = async move {
        let started = Instant::now();
        let mut pages = 0u64;
        let mut examined = 0u64;
        let mut cycles = 0u64;
        let mut longest = 0f64;
        let mut cursor: Option<String> = None;
        loop {
            let begin = Instant::now();
            let reply=control(&scan_fs,json!({"op":"policy_scan","context":{"operation_id":format!("overhead-scan-{pages}"),"controller_epoch":1,"attempt_epoch":1},"id":"overhead-zero","after":cursor,"limit":256})).await?;
            longest = longest.max(begin.elapsed().as_secs_f64() * 1000.);
            let page = &reply["data"];
            if !page["admitted"]
                .as_array()
                .ok_or("missing admitted")?
                .is_empty()
                || !page["to_seal"]
                    .as_array()
                    .ok_or("missing to_seal")?
                    .is_empty()
            {
                return Err("zero-match policy admitted work".into());
            }
            examined += page["examined"].as_u64().ok_or("missing examined")?;
            pages += 1;
            cursor = page["next"].as_str().map(str::to_owned);
            if stopped.try_recv().is_ok() {
                break;
            }
            if cursor.is_none() {
                cycles += 1;
                tokio::select! {_=&mut stopped=>break,_=tokio::time::sleep(Duration::from_secs(1))=>{}}
            }
        }
        Ok::<Value, Box<dyn std::error::Error + Send + Sync>>(
            json!({"pages":pages,"examined":examined,"complete_cycles":cycles,"max_page_ms":longest,"seconds":started.elapsed().as_secs_f64(),"scan_interval_ms":1000,"seed_files":seed_count}),
        )
    };
    let foreground = async {
        let result = metadata(fs, "/scan-active", count, 16).await;
        let _ = stop.send(());
        result
    };
    let (stats, active) = tokio::join!(scan, foreground);
    Ok(json!({"idle":idle,"active":active?,"scan":stats?}))
}
fn main() -> Result {
    let root = PathBuf::from(std::env::var_os("BENCH_ROOT").ok_or("BENCH_ROOT required")?);
    if root.exists() && root.read_dir()?.next().is_some() {
        return Err("BENCH_ROOT must be new/empty".into());
    }
    fs::create_dir_all(&root)?;
    let mode = std::env::var("BENCH_MODE")?;
    if !["original", "off", "on"].contains(&mode.as_str()) {
        return Err("mode must be original/off/on".into());
    }
    let smoke = std::env::var_os("BENCH_SMOKE").is_some();
    let io_only = std::env::var_os("BENCH_IO_ONLY").is_some();
    let large_files = std::env::var("BENCH_LARGE_FILES")
        .ok()
        .map(|v| v.parse::<usize>())
        .transpose()?
        .unwrap_or(if smoke { 2 } else { 8 });
    if !(1..=1024).contains(&large_files) {
        return Err("BENCH_LARGE_FILES must be 1..1024".into());
    }
    let metadata_count = if smoke { 16 } else { 256 };
    let master_connections = std::env::var("BENCH_MASTER_CONN_POOL_SIZE")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(1);
    if !(1..=64).contains(&master_connections) {
        return Err("BENCH_MASTER_CONN_POOL_SIZE must be 1..64".into());
    }
    let mut conf = ClusterConf::format();
    conf.testing = false;
    conf.journal.enable = true;
    conf.journal.writer_channel_size = 0;
    conf.journal.leader_create_snapshot = Some(false);
    conf.journal.snapshot_entries = u64::MAX;
    conf.journal.snapshot_interval = "24h".into();
    conf.journal.raft_tick_interval_ms = 100;
    conf.journal.raft_election_tick = 100;
    conf.journal.raft_min_election_ticks = 100;
    conf.journal.raft_max_election_ticks = 150;
    conf.master.meta_dir = root.join("meta").to_string_lossy().into_owned();
    conf.journal.journal_dir = root.join("journal").to_string_lossy().into_owned();
    conf.worker.data_dir = vec![root.join("worker").to_string_lossy().into_owned()];
    conf.client.short_circuit = false;
    conf.client.master_conn_pool_size = master_connections;
    conf.client.replicas = 1;
    conf.client.rpc_timeout_ms = 120000;
    conf.client.rpc_retry_max_duration_ms = 120000;
    conf.log.log_dir = "stderr".into();
    conf.log.level = "WARN".into();
    conf.master.log = conf.log.clone();
    conf.worker.log = conf.log.clone();
    let mut value = serde_json::to_value(conf)?;
    value["master"]["hsm_enabled"] = json!(mode == "on");
    let conf: ClusterConf = serde_json::from_value(value)?;
    let start = Instant::now();
    let cluster = MiniCluster::with_num(&conf, 3, 1);
    cluster.start_cluster();
    fs::write(
        root.join("cluster.json"),
        serde_json::to_vec_pretty(&(cluster.master_conf.clone(), cluster.worker_conf.clone()))?,
    )?;
    let filesystem = cluster.new_fs();
    let results = cluster.clone_client_rt().block_on(async {
        #[cfg(feature = "hsm-scale-test")]
        if mode == "on" {
            control(&filesystem, json!({"op":"namespace"})).await?;
        }
        #[cfg(not(feature = "hsm-scale-test"))]
        if mode == "on" {
            return Err("HSM build requires --features hsm-scale-test".into());
        }
        metadata(&filesystem, "/warmup", 16, 4).await?;
        data_io(&filesystem, "/warmup-data", 4, 4096, 4).await?;
        let mut reports = vec![];
        if !io_only {
            for concurrency in [1, 16] {
                reports.extend(
                    metadata(
                        &filesystem,
                        &format!("/meta-{concurrency}"),
                        metadata_count,
                        concurrency,
                    )
                    .await?,
                );
            }
            for concurrency in [1, 16] {
                reports.extend(
                    data_io(
                        &filesystem,
                        &format!("/small-{concurrency}"),
                        if smoke { 8 } else { 128 },
                        4096,
                        concurrency,
                    )
                    .await?,
                );
            }
        }
        for concurrency in [1, 4] {
            reports.extend(
                data_io(
                    &filesystem,
                    &format!("/large-{concurrency}"),
                    large_files,
                    if smoke { 1048576 } else { 64 * 1048576 },
                    concurrency,
                )
                .await?,
            );
        }
        let mut scan = Value::Null;
        #[cfg(feature = "hsm-scale-test")]
        if mode == "on" && !io_only {
            scan =
                scan_comparison(&filesystem, metadata_count, if smoke { 32 } else { 2048 }).await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
            json!({"mode":mode,"results":reports,"policy_interference":scan}),
        )
    })?;
    let mut report = results;
    report["elapsed_seconds"] = json!(start.elapsed().as_secs_f64());
    report["process_final"] = proc_stats()?;
    report["topology"] = json!({"masters":3,"workers":1,"server_and_client_processes":1,"replicas":1,"ufs":false,"short_circuit":false,"master_connections":master_connections,"metadata_files_per_suite":metadata_count,"smoke":smoke,"io_only":io_only,"large_files":large_files});
    fs::write(
        root.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("REPORT {}", root.join("report.json").display());
    // MiniCluster owns long-lived service threads; each invocation exits and the
    // driver waits for exit before starting the next fresh isolated cluster.
    std::process::exit(0);
}
