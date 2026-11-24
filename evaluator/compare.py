#!/usr/bin/env python3
"""Compare several evaluation result files:  python compare.py base_1.json base_2.json lora_1.json ...
Files are grouped by the part of the name before the last '_' (base_1.json -> group 'base')."""
import json, sys, os, statistics as st
groups = {}
for p in sys.argv[1:]:
    name = os.path.splitext(os.path.basename(p))[0]
    g = name.rsplit("_", 1)[0] if "_" in name else name
    groups.setdefault(g, []).append(json.load(open(p)))
if not groups:
    sys.exit(__doc__)
ids = [r["id"] for r in next(iter(groups.values()))[0]["results"]]
print(f"{'group':<12}{'runs':>5}{'mean':>8}{'min':>7}{'max':>7}{'stdev':>8}")
for g, runs in groups.items():
    sc = [r["score"] for r in runs]
    print(f"{g:<12}{len(runs):>5}{st.mean(sc):>8.1f}{min(sc):>7.1f}{max(sc):>7.1f}{(st.pstdev(sc) if len(sc) > 1 else 0):>8.1f}")
print("\nper-check pass rate (share of runs that passed):")
print(f"{'check':<28}" + "".join(f"{g:>10}" for g in groups))
for cid in ids:
    row = f"{cid:<28}"
    for g, runs in groups.items():
        ok = sum(1 for r in runs for x in r["results"] if x["id"] == cid and x["passed"])
        row += f"{ok / len(runs):>10.0%}"
    print(row)
print("\nper-area (mean passed weight / total weight):")
areas = next(iter(groups.values()))[0]["areas"]
print(f"{'area':<12}" + "".join(f"{g:>10}" for g in groups))
for a, (w, _, _, _) in areas.items():
    row = f"{a:<12}"
    for g, runs in groups.items():
        row += f"{st.mean(r['areas'][a][1] for r in runs) / w:>10.0%}"
    print(row)
