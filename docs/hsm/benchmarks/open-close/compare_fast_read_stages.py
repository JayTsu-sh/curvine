#!/usr/bin/env python3
"""Paired fresh-process open/complete benchmark, including close and access drains."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import threading
import time

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def summarize(records):
    result=[]
    keys=sorted({(r['file_bytes'],r['kind'],r['concurrency']) for r in records})
    for size,kind,concurrency in keys:
        cases={}
        for case in ['upstream','hsm']:
            rows=[r for r in records if (r['file_bytes'],r['kind'],r['concurrency'],r['case'])==(size,kind,concurrency,case)]
            cases[case]={
                'rounds':len(rows),
                'ops_per_sec':statistics.median(r['ops_per_sec'] for r in rows),
                'ops_range':[min(r['ops_per_sec'] for r in rows),max(r['ops_per_sec'] for r in rows)],
                'p99_ms':statistics.median(r['latency_ms']['p99'] for r in rows),
                'mean_ms':statistics.median(r['latency_ms']['mean'] for r in rows),
                'drain_ms':statistics.median(r['drain_seconds']*1000 for r in rows),
            }
        result.append({'file_bytes':size,'kind':kind,'concurrency':concurrency,**cases,'ratio':cases['hsm']['ops_per_sec']/cases['upstream']['ops_per_sec']})
    return result

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--upstream',type=Path,required=True)
    p.add_argument('--hsm',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--rounds',type=int,default=3)
    p.add_argument('--files',type=int,default=4096)
    p.add_argument('--seconds',type=float,default=6)
    args=p.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    binaries={'upstream':args.upstream.resolve(),'hsm':args.hsm.resolve()}
    manifest={'binaries':{case:{'path':str(binary),'sha256':digest(binary)} for case,binary in binaries.items()},
              'rounds':args.rounds,'files':args.files,'steady_seconds':args.seconds,'concurrency':[1,16],
              'file_bytes':[0,4096],'harness_sha256':digest(Path(__file__).with_name('hsm_fast_read_stages.rs')),
              'topology':'3 Masters + 1 Worker, TCP/Raft, one Master connection, one fresh process per case; no UFS or short circuit',
              'timing':'foreground open+complete, FIFO client close drain and the same two namespace RPCs in both cases; HSM namespace admission persists the access tail',
              'cold_pass':'one open per file after a stable-operation cache invalidation; RocksDB/OS caches may be warm'}
    (args.output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    records=[]
    for round_no in range(1,args.rounds+1):
        order=['upstream','hsm'] if round_no%2 else ['hsm','upstream']
        for case in order:
            name=f'r{round_no}-{case}'
            root=(args.output/name).resolve()
            env={k:v for k,v in os.environ.items() if not k.startswith('BENCH_')}
            env.update(BENCH_ROOT=str(root),BENCH_MODE='on' if case=='hsm' else 'original',BENCH_FILES=str(args.files),BENCH_SECONDS=str(args.seconds))
            with (args.output/f'{name}.log').open('w') as log:
                child=subprocess.Popen([str(binaries[case])],env=env,stdout=log,stderr=subprocess.STDOUT)
                watchdog=threading.Timer(300,child.kill);watchdog.start()
                started=time.monotonic()
                try:
                    code=child.wait()
                finally:
                    watchdog.cancel()
                if code:
                    raise RuntimeError(f'{name} exited {code}; inspect its log')
            report=json.loads((root/'report.json').read_text())
            assert len(report['results'])==8
            assert all(r['errors']==0 for r in report['results'])
            records.extend([{**row,'round':round_no,'case':case} for row in report['results']])
            (args.output/'results.json').write_text(json.dumps(records,indent=2)+'\n')
            print(name,f'{time.monotonic()-started:.1f}s',json.dumps({f'{r["file_bytes"]}B/{r["kind"]}/C{r["concurrency"]}':round(r['ops_per_sec'],1) for r in report['results']}),flush=True)
    summary=summarize(records)
    (args.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    for row in summary:
        print(row['file_bytes'],row['kind'],row['concurrency'],f'{row["ratio"]:.3%}',flush=True)

if __name__=='__main__':main()
