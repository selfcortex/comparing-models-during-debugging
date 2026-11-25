# Comparing Models During Debugging

A benchmark of LLMs on a Python debugging task. Each model receives
[`workspace/taskflow.py`](workspace/taskflow.py) — a small dependency-aware task execution
engine (rate limiter, TTL cache, event bus, DAG, retry policy, transactional store,
serialization with schema migrations, and an `Engine` that glues it all together) that has
been deliberately broken with **dozens of bugs**: logic errors, edge cases, ordering of side
effects, data copying, cross-component interactions, recursion depth, and asymptotics.

Task rules ([`workspace/TASK.md`](workspace/TASK.md)):

- **Docstrings are the contract.** When code and a docstring disagree, the code is wrong —
  fix the code.
- The public API (class names, methods, signatures, event and report formats) must not change.
- Standard library only, no new dependencies.
- Fix the cause, not the symptom; no patching specific inputs.
- The result is graded by an independent hidden suite of checks the model never sees.

Every run is graded by the **hidden evaluator** — [`evaluator/evaluate.py`](evaluator/evaluate.py):
**49 weighted checks** (total weight 149), including randomized differential (fuzz) tests run
against a reference model. The final score is the weighted share of passed checks, 0–100.

> **Status: not the final version.** More models are on the way; the tables below will grow.

---

## Summary

| # | Model | Setup | Time | Score |
|:-:|---|---|---:|---:|
| 1 | **Gemini 4 (Hydrogen)** | API | ~3.5 min | **100.0 / 100** (49/49) |
| 2 | **Gemini 3.8 Flash** | API | ~27 min | **100.0 / 100** (49/49) |
| 3 | **DeepSeek v4.1 Flash** | API | ~6 min | **100.0 / 100** (49/49) |
| 4 | **Qwen4Max** | API | ~8 min | **100.0 / 100** (49/49) |
| 5 | **Qwen 3.8 27B + LoRA** | local, < 30 t/s | ~20 min | **97.3 / 100** (48/49) |
| 6 | **ChatGPT** | API | ~20 min | **91.9 / 100** (46/49) |
| — | Qwen 3.8 27B (no LoRA) | local | hit the limit | **failed — no code produced** |

Notes:

- **Qwen 3.8 27B without the LoRA adapter** ran out of budget before writing anything: the
  returned `taskflow.py` was byte-identical to the original (verified by hash), so there is no
  `results/*.json` for it.
- **Qwen 3.8 27B + LoRA** ran locally at under 30 t/s and took ~20 minutes, but missed only
  one check (`engine.rate_limit`). The adapter made the difference between a total failure
  and a 97.3 score.
- **Gemini 3.8 Flash** hit its thinking budget twice mid-run and had to be prompted to
  continue; the ~27 minutes include those stalls.
- **ChatGPT** missed three checks, all in the same theme: deletions inside transactions in the
  `Store` (which also broke the engine's attempt atomicity and the store fuzz test).

## Score by area (weight passed / total weight)

| Area | Total wt | Gemini 4 (Hydrogen) | Gemini 3.8 Flash | DeepSeek v4.1 Flash | Qwen4Max | Qwen 3.8 27B + LoRA | ChatGPT |
|---|---:|---:|---:|---:|---:|---:|---:|
| bucket (TokenBucket) | 6 | 6/6 | 6/6 | 6/6 | 6/6 | 6/6 | 6/6 |
| cache (TTLCache) | 14 | 14/14 | 14/14 | 14/14 | 14/14 | 14/14 | 14/14 |
| bus (EventBus) | 10 | 10/10 | 10/10 | 10/10 | 10/10 | 10/10 | 10/10 |
| dag (DAG) | 20 | 20/20 | 20/20 | 20/20 | 20/20 | 20/20 | 20/20 |
| retry (RetryPolicy) | 7 | 7/7 | 7/7 | 7/7 | 7/7 | 7/7 | 7/7 |
| store (Store) | 14 | 14/14 | 14/14 | 14/14 | 14/14 | 14/14 | **10/14** |
| serial (serialization & migrations) | 15 | 15/15 | 15/15 | 15/15 | 15/15 | 15/15 | 15/15 |
| engine (Engine, integration) | 45 | 45/45 | 45/45 | 45/45 | 45/45 | **41/45** | **41/45** |
| fuzz (differential tests) | 18 | 18/18 | 18/18 | 18/18 | 18/18 | 18/18 | **14/18** |
| **Total** | **149** | **100.0/100** | **100.0/100** | **100.0/100** | **100.0/100** | **97.3/100** | **91.9/100** |

## Per-check results

Cells show the pass rate of the check for that model (single run: 100% = passed, **0%** =
failed). **Wt** is the check weight.

| Check | Area | Wt | Gemini 4 (Hydrogen) | Gemini 3.8 Flash | DeepSeek v4.1 Flash | Qwen4Max | Qwen 3.8 27B + LoRA | ChatGPT |
|---|---|--:|---:|---:|---:|---:|---:|---:|
| `bucket.fractions` | bucket | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `bucket.idle` | bucket | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `bucket.time_until` | bucket | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `cache.ttl_boundary` | cache | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `cache.ttl_zero` | cache | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `cache.lru_get` | cache | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `cache.replace` | cache | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `cache.purge_first` | cache | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `cache.live_view` | cache | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `bus.order` | bus | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `bus.patterns` | bus | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `bus.mutation` | bus | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `bus.once_errors` | bus | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `dag.dup_deps` | dag | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `dag.self_loop` | dag | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `dag.tiebreak` | dag | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `dag.cycle_path` | dag | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `dag.deep_chain` | dag | 4 | 100% | 100% | 100% | 100% | 100% | 100% |
| `dag.levels` | dag | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `dag.targets` | dag | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `retry.delay` | retry | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `retry.overflow` | retry | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `retry.should_retry` | retry | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `store.nesting` | store | 4 | 100% | 100% | 100% | 100% | 100% | 100% |
| `store.delete` | store | 4 | 100% | 100% | 100% | 100% | 100% | **0%** |
| `store.isolation` | store | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `store.transaction_cm` | store | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `ser.roundtrip` | serial | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `ser.canonical` | serial | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `ser.migrate_v1` | serial | 4 | 100% | 100% | 100% | 100% | 100% | 100% |
| `ser.migrate_v2` | serial | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `ser.versions` | serial | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.dataflow` | engine | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.skip_propagation` | engine | 4 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.retry_timing` | engine | 4 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.store_atomicity` | engine | 4 | 100% | 100% | 100% | 100% | 100% | **0%** |
| `engine.timeout` | engine | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.cache_hits` | engine | 4 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.cache_keys` | engine | 4 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.injected_cache` | engine | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.input_isolation` | engine | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.cancel` | engine | 3 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.rate_limit` | engine | 4 | 100% | 100% | 100% | 100% | **0%** | 100% |
| `engine.fail_fast` | engine | 4 | 100% | 100% | 100% | 100% | 100% | 100% |
| `engine.events` | engine | 2 | 100% | 100% | 100% | 100% | 100% | 100% |
| `fuzz.cache` | fuzz | 4 | 100% | 100% | 100% | 100% | 100% | 100% |
| `fuzz.store` | fuzz | 4 | 100% | 100% | 100% | 100% | 100% | **0%** |
| `fuzz.dag` | fuzz | 5 | 100% | 100% | 100% | 100% | 100% | 100% |
| `fuzz.engine` | fuzz | 5 | 100% | 100% | 100% | 100% | 100% | 100% |

### First observations

- The task separates the field well: from a total failure (local model without LoRA, no
  output) to perfect 100 scores.
- The hardest parts for the models were the transactional store with nested commit/rollback,
  per-attempt atomicity in the `Engine`, and rate limiting — exactly what the two imperfect
  models got wrong.
- Four API models reached a perfect score; among them Gemini 4 (Hydrogen) was the fastest
  (~3.5 min).
- The local Qwen 3.8 27B + LoRA run came within one check of a perfect score at < 30 t/s,
  while the same model without the adapter produced nothing.

---

## Repository layout

```
workspace/               the task (original, never modified)
  TASK.md                task statement
  taskflow.py            the buggy original module

evaluator/               hidden evaluator (never shown to the models)
  evaluate.py            49 weighted checks, score 0..100, --json output
  compare.py             compare several result files (groups by file name)
  new_run.sh             create a fresh workspace copy for a new run
  reference_taskflow.py  the fixed reference module

runs/                    model runs (the model's produced code)
  27b_lora/              Qwen 3.8 27B + LoRA (taskflow.py + own tests in tests/)
  chatgpt/               ChatGPT
  deepseek v4.1 flash/   DeepSeek v4.1 Flash
  gemini 4 hydrogen/     Gemini 4 (Hydrogen)
  gemini_3.8_flash/      Gemini 3.8 Flash
  qwen4max/              Qwen4Max

results/                 evaluator output, one JSON per run
  gemini_4_hydrogen.json      100.0
  gemini_3.8_flash.json       100.0
  deepseek_v4.1_flash.json    100.0
  qwen4max.json               100.0
  27b_lora.json               97.3
  chatgpt.json                91.9
```

## How to run a new model

```bash
# 1. create a fresh copy of the task for the new run
./evaluator/new_run.sh <run_name>          # e.g. ./evaluator/new_run.sh claude_1

# 2. hand the model the runs/<run_name>/ directory (TASK.md + taskflow.py) and wait

# 3. grade the result
python3 evaluator/evaluate.py runs/<run_name>/taskflow.py --json results/<run_name>.json

# 4. compare against previous runs
python3 evaluator/compare.py results/27b_lora.json results/chatgpt.json results/<run_name>.json
```

`compare.py` groups files by the name part before the last `_` (`lora_1.json` → group
`lora`) and prints mean/min/max/stdev of the score, per-check pass rate, and per-area
weight share.

## Methodology

- Every check is isolated: its own fixtures, a wall-clock timeout per check, and a global
  recursion limit of 1000 (the task requires iterative implementations).
- Fuzz checks run random programs against a naive reference model to catch hard-coding of
  specific inputs.
- Score = weighted share of passed checks, 0–100.
- Times in the summary table are wall-clock durations of the model run (local runs also
  note the generation speed in t/s, since it affects comparability).
