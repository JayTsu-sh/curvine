#!/usr/bin/env python3
"""Build once, then compare HSM metadata algorithms using real RocksDB.

This does not create a Curvine namespace or exercise Raft/data movement.
Use a dedicated output directory; existing measurements are never overwritten.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--sizes', nargs='+', type=int, default=[10_000, 100_000, 1_000_000])
    parser.add_argument('--repeats', type=int, default=1)
    parser.add_argument('--updates', type=int, default=50_000)
    parser.add_argument('--jobs', type=int, default=6)
    args = parser.parse_args()
    if args.repeats < 1 or args.updates < 0 or args.jobs < 1:
        parser.error('repeats/jobs must be positive; updates must be nonnegative')
    if not args.sizes or any(n < 1 or n > 10_000_000 for n in args.sizes):
        parser.error('sizes must be between 1 and 10000000')
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('output directory must be empty')
    env = os.environ.copy()
    env.setdefault('CXXFLAGS', '-g0')
    command = ['cargo', 'test', '-p', 'curvine-master', '--locked', '--lib', '--no-run',
               '--message-format=json', '-j', str(args.jobs)]
    executable = None
    with subprocess.Popen(command, cwd=root, env=env, stdout=subprocess.PIPE, text=True) as process:
        for line in process.stdout:
            message = json.loads(line)
            if message.get('reason') == 'compiler-artifact' and message.get('executable'):
                if message['target']['name'] == 'curvine_master':
                    executable = message['executable']
        if process.wait() != 0:
            raise SystemExit('benchmark build failed')
    if executable is None:
        raise SystemExit('curvine-master test executable was not reported by Cargo')
    metadata = {
        'kind': 'real-rocksdb-metadata-only', 'platform': platform.platform(),
        'cpus': os.cpu_count(), 'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
        'base_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        'dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True)),
        'sizes': args.sizes, 'repeats': args.repeats, 'updates': args.updates,
        'initialization': 'identical optimized seed for both variants; seed_ms is not a before/after comparison',
        'cache': 'scans follow seeding and updates in a fresh temporary DB; no OS cache purge',
        'path_callbacks': 'counts only; native namespace traversal is not benchmarked',
    }
    (output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    results = []
    for size in args.sizes:
        for repeat in range(1, args.repeats + 1):
            variants = ['baseline', 'optimized'] if repeat % 2 else ['optimized', 'baseline']
            for variant in variants:
                name = f'{size}-{variant}-r{repeat}'
                print(time.strftime('%FT%T'), 'START', name, flush=True)
                run_env = env | {'HSM_SCALE_RECORDS': str(size), 'HSM_SCALE_UPDATES': str(args.updates),
                                 'HSM_SCALE_VARIANT': variant, 'HSM_SCALE_OUTPUT': str(output / (name + '.json'))}
                with (output / (name + '.log')).open('w') as log:
                    subprocess.run([executable, '--ignored', '--exact',
                                    'master::hsm::scale_tests::hsm_metadata_scale_benchmark', '--nocapture'],
                                   cwd=root, env=run_env, stdout=log, stderr=subprocess.STDOUT, check=True)
                result = json.loads((output / (name + '.json')).read_text())
                assert result['records'] == size and result['variant'] == variant
                results.append(result)
                (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                print(time.strftime('%FT%T'), 'PASS', name, flush=True)


if __name__ == '__main__':
    main()
