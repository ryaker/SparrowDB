#!/usr/bin/env python3
"""
bench-compare.py — Compare cargo bench output between main and PR branch.
Fails with exit code 1 if any benchmark regresses beyond the threshold.

Usage:
    python3 bench-compare.py main.txt pr.txt --threshold 0.10

Parses cargo bench --output-format bencher output:
    test bench_name ... bench: 1,234 ns/iter (+/- 56)
"""

import re
import sys
import argparse

BENCH_RE = re.compile(r'^test (\S+)\s+\.\.\. bench:\s+([\d,]+) ns/iter')


def parse(path):
    results = {}
    try:
        with open(path) as f:
            for line in f:
                m = BENCH_RE.match(line.strip())
                if m:
                    name = m.group(1)
                    ns = int(m.group(2).replace(',', ''))
                    results[name] = ns
    except FileNotFoundError:
        pass
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('baseline')
    parser.add_argument('candidate')
    parser.add_argument('--threshold', type=float, default=0.10,
                        help='Regression threshold (0.10 = 10%%)')
    args = parser.parse_args()

    base = parse(args.baseline)
    pr   = parse(args.candidate)

    regressions = []
    improvements = []
    new_benches = []

    all_names = sorted(set(base) | set(pr))
    rows = []

    for name in all_names:
        if name not in base:
            new_benches.append(name)
            rows.append(f'  NEW  {name}: {pr[name]:,} ns/iter')
            continue
        if name not in pr:
            rows.append(f'  ???  {name}: missing in PR')
            continue

        delta = (pr[name] - base[name]) / base[name]
        sign = '+' if delta >= 0 else ''
        row = f'  {sign}{delta*100:.1f}%  {name}: {base[name]:,} → {pr[name]:,} ns/iter'

        if delta > args.threshold:
            regressions.append((name, delta))
            row = '🔴' + row
        elif delta < -0.05:
            improvements.append((name, delta))
            row = '🟢' + row
        else:
            row = '⚪' + row

        rows.append(row)

    print('\n'.join(rows) or '  (no benchmarks found)')
    print()

    if improvements:
        print(f'Improvements: {len(improvements)}')
    if new_benches:
        print(f'New benchmarks: {len(new_benches)}')
    if regressions:
        print(f'\n❌ REGRESSIONS (>{args.threshold*100:.0f}% slower):')
        for name, delta in regressions:
            print(f'   {name}: +{delta*100:.1f}%')
        sys.exit(1)
    else:
        print('✅ No regressions detected')


if __name__ == '__main__':
    main()
