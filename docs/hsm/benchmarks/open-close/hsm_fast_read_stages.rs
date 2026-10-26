//! Identical paired ordinary SDK open+complete workload for upstream and HSM.
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
async fn drain(filesystem: &CurvineFileSystem) -> Result {
    #[cfg(feature = "benchmark-hsm")]
    filesystem.flush_hsm_read_closes().await?;
    #[cfg(not(feature = "benchmark-hsm"))]
    let _ = filesystem;
    Ok(())
}
#[cfg(feature = "benchmark-hsm")]
static METRICS_PORT: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
fn stage_counters() -> Value {
    #[cfg(feature = "benchmark-hsm")]
    {
        let url = format!("http://127.0.0.1:{}/metrics", METRICS_PORT.get().unwrap());
        let response = std::process::Command::new("curl")
            .args(["--noproxy", "*", "-sf", "--max-time", "3", &url])
            .output()
            .unwrap();
        assert!(response.status.success(), "metrics capture failed");
        let raw = String::from_utf8(response.stdout).unwrap();
        let mut output = serde_json::Map::new();
        for stage in [
            "capture",
            "encode",
            "commit",
            "apply_preflight",
            "apply_write",
        ] {
            let mut count = 0.;
            let mut sum = 0.;
            for line in raw.lines().filter(|line| {
                line.contains("access_checkpoint") && line.contains(&format!("stage=\"{stage}\""))
            }) {
                if let Some(value) = line
                    .split_whitespace()
                    .last()
                    .and_then(|value| value.parse::<f64>().ok())
                {
                    if line.starts_with("hsm_stage_duration_count{") {
                        count = value;
                    }
                    if line.starts_with("hsm_stage_duration_sum{") {
                        sum = value;
                    }
                }
            }
            output.insert(stage.into(), json!({"count":count,"sum_us":sum}));
        }
        Value::Object(output)
    }
    #[cfg(not(feature = "benchmark-hsm"))]
    Value::Null
}
async fn measure(
    filesystem: &CurvineFileSystem,
    paths: &[Path],
    concurrency: usize,
    cold: bool,
    seconds: f64,
) -> Result<Value> {
    drain(filesystem).await?;
    let stages_before = stage_counters();
    let started = Instant::now();
    let end = started + Duration::from_secs_f64(seconds);
    let count = if cold { paths.len() } else { usize::MAX };
    let mut latencies = stream::iter(0..count)
        .take_while(|_| futures::future::ready(cold || Instant::now() < end))
        .map(|index| async move {
            let started = Instant::now();
            let mut reader = filesystem.open(&paths[index % paths.len()]).await?;
            reader.complete().await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                started.elapsed().as_secs_f64() * 1000.,
            )
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let foreground = started.elapsed().as_secs_f64();
    let closing = Instant::now();
    drain(filesystem).await?;
    let close_drain_seconds = closing.elapsed().as_secs_f64();
    let barrier = Path::from_str("/access-drain-barrier")?;
    let mkdir_started = Instant::now();
    filesystem.mkdir(&barrier, false).await?;
    let mkdir_seconds = mkdir_started.elapsed().as_secs_f64();
    let delete_started = Instant::now();
    filesystem.delete(&barrier, false).await?;
    let delete_seconds = delete_started.elapsed().as_secs_f64();
    let elapsed = started.elapsed().as_secs_f64();
    latencies.sort_by(|a, b| a.total_cmp(b));
    let len = latencies.len();
    let result = json!({"close_drain_seconds":close_drain_seconds,"mkdir_seconds":mkdir_seconds,"delete_seconds":delete_seconds,"stages_before":stages_before,"stages_after":stage_counters(),"kind":if cold {"cold_pass"}else{"steady"},"concurrency":concurrency,"operations":len,"seconds":elapsed,"foreground_seconds":foreground,"drain_seconds":elapsed-foreground,"ops_per_sec":len as f64/elapsed,"latency_ms":{"mean":latencies.iter().sum::<f64>()/len as f64,"p50":latencies[len/2],"p99":latencies[(len*99/100).min(len-1)],"max":latencies[len-1]},"errors":0});
    eprintln!("RESULT {result}");
    Ok(result)
}
fn main() -> Result {
    let root = PathBuf::from(std::env::var_os("BENCH_ROOT").ok_or("BENCH_ROOT required")?);
    if root.exists() && root.read_dir()?.next().is_some() {
        return Err("fresh BENCH_ROOT required".into());
    }
    fs::create_dir_all(&root)?;
    let mode = std::env::var("BENCH_MODE")?;
    let files = std::env::var("BENCH_FILES")
        .unwrap_or("2048".into())
        .parse::<usize>()?;
    let seconds = std::env::var("BENCH_SECONDS")
        .unwrap_or("6".into())
        .parse::<f64>()?;
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
    conf.client.master_conn_pool_size = 1;
    conf.client.replicas = 1;
    conf.client.block_size_str = "64MB".into();
    conf.client.rpc_timeout_ms = 120000;
    conf.client.rpc_retry_max_duration_ms = 120000;
    conf.log.log_dir = "stderr".into();
    conf.log.level = "WARN".into();
    conf.master.log = conf.log.clone();
    conf.worker.log = conf.log.clone();
    let mut value = serde_json::to_value(conf)?;
    value["master"]["hsm_enabled"] = json!(mode == "on");
    let conf: ClusterConf = serde_json::from_value(value)?;
    let cluster = MiniCluster::with_num(&conf, 3, 1);
    cluster.start_cluster();
    #[cfg(feature = "benchmark-hsm")]
    METRICS_PORT
        .set(cluster.master_conf[0].master.web_port)
        .unwrap();
    fs::write(
        root.join("cluster.json"),
        serde_json::to_vec_pretty(&(cluster.master_conf.clone(), cluster.worker_conf.clone()))?,
    )?;
    let filesystem = cluster.new_fs();
    let result=cluster.clone_client_rt().block_on(async {
        let mut results=vec![];
        for size in [0,4096] {
            let prefix=format!("/files-{size}");filesystem.mkdir(&Path::from_str(&prefix)?,false).await?;
            let paths=(0..files).map(|i|Path::from_str(format!("{prefix}/{i}"))).collect::<std::result::Result<Vec<_>,_>>()?;
            stream::iter(&paths).map(|path|async {
                let mut writer=filesystem.create(path,false).await?;
                if size>0 {writer.write(&vec![0x5a;size]).await?;}
                writer.complete().await?;
                Ok::<_,Box<dyn std::error::Error+Send+Sync>>(())
            }).buffer_unordered(16).try_collect::<Vec<_>>().await?;
            for concurrency in [1,16] {
                for cold in [true,false] {
                    let mut row=measure(&filesystem,&paths,concurrency,cold,seconds).await?;
                    row["file_bytes"]=json!(size);results.push(row);
                }
            }
            if size>0 {
                let mut reader=filesystem.open(&paths[0]).await?;
                let mut bytes=vec![0;size];let mut offset=0;
                while offset<size {let n=reader.read(&mut bytes[offset..]).await?;if n==0 {return Err("short read".into());}offset+=n;}
                if bytes.iter().any(|byte|*byte!=0x5a) {return Err("payload checksum failed".into());}
                reader.complete().await?;drain(&filesystem).await?;
            }
            // A real stable mutation drains/persists the access tail before
            // teardown. Report its cost separately from ordinary close ACKs.
            let started=Instant::now();filesystem.delete(&Path::from_str(&prefix)?,true).await?;
            eprintln!("FINAL_ACCESS_DRAIN {}",json!({"file_bytes":size,"seconds":started.elapsed().as_secs_f64()}));
        }
        Ok::<_,Box<dyn std::error::Error+Send+Sync>>(json!({"mode":mode,"files":files,"steady_seconds":seconds,"masters":3,"workers":1,"master_connections":1,"short_circuit":false,"results":results}))
    })?;
    fs::write(
        root.join("report.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    Ok(())
}
