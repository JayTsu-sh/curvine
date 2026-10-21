#!/usr/bin/env python3
"""Run identical ordinary SDK workloads against pre-HSM and current binaries.

Build curvine-server/examples/hsm_overhead.rs in each source tree first. All
clusters are isolated children with fresh data roots; no PVE service is touched.
"""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import subprocess
import time
from datetime import datetime, timezone


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--original-binary', type=Path, required=True)
    parser.add_argument('--hsm-binary', type=Path, required=True)
    parser.add_argument('--original-rev', required=True)
    parser.add_argument('--hsm-rev', required=True)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error('--rounds must be positive')
    if args.output.exists() and any(args.output.iterdir()):
        parser.error('--output must be new or empty')
    args.output.mkdir(parents=True, exist_ok=True)
    binaries = {'original': args.original_binary.resolve(), 'off': args.hsm_binary.resolve(), 'on': args.hsm_binary.resolve()}
    for binary in set(binaries.values()):
        if not binary.is_file():
            parser.error(f'binary does not exist: {binary}')
    manifest = {
        'started_utc': datetime.now(timezone.utc).isoformat(),
        'platform': platform.platform(),
        'cpu_affinity': sorted(os.sched_getaffinity(0)),
        'clock_ticks_per_second': os.sysconf('SC_CLK_TCK'),
        'memory_total_kib': int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemTotal:'))),
        'original_rev': args.original_rev,
        'hsm_rev': args.hsm_rev,
        'binaries': {mode: {'sha256': sha256(binary), 'bytes': binary.stat().st_size} for mode, binary in binaries.items()},
        'rounds': args.rounds,
        'smoke': args.smoke,
        'profile': 'release (both builds; see build commands)',
        'cache': 'fresh process and DB per invocation; no OS page cache purge',
        'execution': 'sequential clusters; rotated order; shared VM; loopback RPC; no physical node fault isolation',
    }
    (args.output / 'environment.json').write_text(json.dumps(manifest, indent=2) + '\n')
    reports = []
    modes = ['original', 'off', 'on']
    for round_index in range(args.rounds):
        order = modes[round_index % 3:] + modes[:round_index % 3]
        for mode in order:
            run = args.output / f'r{round_index+1}-{mode}'
            env = os.environ.copy()
            env.update(BENCH_ROOT=str(run.resolve()), BENCH_MODE=mode)
            env.pop('BENCH_SMOKE', None)
            if args.smoke:
                env['BENCH_SMOKE'] = '1'
            print(f'START r{round_index+1} {mode}', flush=True)
            start = time.monotonic()
            max_rss = 0
            with (args.output / f'{run.name}.log').open('w') as log:
                child = subprocess.Popen([str(binaries[mode])], env=env, stdout=log, stderr=subprocess.STDOUT)
                try:
                    while child.poll() is None:
                        try:
                            for line in Path(f'/proc/{child.pid}/status').read_text().splitlines():
                                if line.startswith('VmRSS:'):
                                    max_rss = max(max_rss, int(line.split()[1]))
                        except (FileNotFoundError, ProcessLookupError):
                            pass
                        if time.monotonic() - start > 7200:
                            raise TimeoutError(f'{run.name} exceeded 7200 s')
                        time.sleep(0.1)
                    if child.returncode:
                        raise RuntimeError(f'{run.name}: exit {child.returncode}; see {run.name}.log')
                finally:
                    if child.poll() is None:
                        child.kill()
                        child.wait()
            report = json.loads((run / 'report.json').read_text())
            assert report['mode'] == mode
            assert all(item['errors'] == 0 and item['operations'] > 0 for item in report['results'])
            report.update(round=round_index+1, sampled_peak_rss_kib=max_rss, driver_elapsed_seconds=time.monotonic()-start)
            reports.append(report)
            (args.output / 'results.json').write_text(json.dumps(reports, indent=2) + '\n')
            print(f'PASS r{round_index+1} {mode}: {report["driver_elapsed_seconds"]:.2f}s', flush=True)
    print(f'COMPLETE {len(reports)} runs: {args.output / "results.json"}', flush=True)


if __name__ == '__main__':
    main()
