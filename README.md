# Guarded Admission and Release

Code and data for temporary resource protection at an Industry 4.0 gateway in
future 6G sliced networks. Guarded admission and release (GAR) checks a delayed
resource-schedule update and a new service request together, preserving existing
commitments under the stated traffic and service conditions. GAR-H is the
planning heuristic evaluated with this mechanism. Author: Rana Tariq.

Repository: https://github.com/rtqayyum/gar-industry4-6g

This repository contains one final implementation of each experiment, its
configuration, recorded results, and verification tests. Recorded measurements
retain their original run identifiers and source metadata.

## Setup

Use Python 3.12 and a C++17 compiler.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python run.py check
```

## Experiments

| Command | Study |
| --- | --- |
| `matched` | Additional commitments enabled by certified calendar replacement |
| `online` | GAR-H and joint, sequential, and event-driven planning |
| `reserve` | Incumbent protection and borrowing unused reserve |
| `provisioning` | Minimum integer resource-floor calculations |
| `gateway` | Transaction-handler tests and guard-component counterexamples |
| `formal` | Calendar feasibility and staged-payload verification |
| `analyze` | Rebuild recorded-data plotting summaries and packet timing analysis |
| `plot` | Recompute plotting data and create Python figures |

Run a study into a new directory; recorded results are not overwritten:

```sh
python run.py matched --output runs/matched
python run.py online --output runs/online --workers 2
python run.py reserve --output runs/reserve
python run.py formal --output runs/formal
python run.py plot --output runs/figures
```

The same `--output` syntax applies to `gateway`, `provisioning`, and `analyze`.
Replays use the packaged evaluation inputs; host execution times may differ.

## Packet records

Packet data cover the 40 complete-pair historical ledgers used in the reported
comparison and all 48 controlled trials plus 12 service probes. The historical
66-attempt registry retains the two timeouts. These are separate measured
cohorts, not additional simulation repetitions.

```sh
python run.py packet-verify
python run.py packet-extract --output runs/packet-records
python run.py packet-analyze --records runs/packet-records --output runs/packet-analysis
```

These commands inspect or reanalyze recorded measurements. Live packet runners
are separate scripts under `experiments/packet/`; no live traffic is launched
by the commands above.

## Experiment configuration and evidence

Paths below are relative to the repository root. Each study has its own
workload, execution model, and statistical unit. Parameters must not be
transferred between studies. `release.json` records the package scope and
repository location.

| Experiment | Configuration and implementation | Recorded evidence |
| --- | --- | --- |
| Implementation and finite-model checks | `experiments/gateway/verify.py`; `experiments/formal/verify_calendar.py`; `experiments/formal/verify_payload.py` | `results/gateway/`; `results/formal/` |
| Matched commitments | `experiments/matched/protocol.json`; `generate()` and execution in `experiments/matched/run.py`; guard interface in `experiments/matched/guard_adapter.cpp` | `results/matched/workloads.jsonl.gz`, `outcomes.jsonl.gz`, `summary.json`, and `verification.json` |
| Online planning | `data/online/protocol.json`; `data/online/contexts.json`; `experiments/online/workload.hpp`; `experiments/online/simulator.cpp` | `results/online/main_rows.csv`, `policy_summary.csv`, `offer_trace_audit_main.csv`, and context-level files described below |
| Incumbent protection | `experiments/reserve/protocol.json`; `experiments/reserve/model.py`; aggregation in `experiments/reserve/run.py` | `results/reserve/workloads.jsonl.gz`, `runs.jsonl`, `outcomes.jsonl.gz`, `traces.jsonl.gz`, `summary.json`, and `validation.json` |
| Initial provisioning | `experiments/reserve/provisioning.py` | `results/reserve/provisioning.json` |
| Packet execution | `data/packet/manifest.json`; historical `COHORT.json` and `PROTOCOL.json` in `results/packet/historical/`; `experiments/packet/prepare_targeted.py`; `experiments/packet/engine/` | `results/packet/historical/`; `results/packet/controlled/`; compressed records indexed by the packet manifest |

### Matched initial states

The matched study uses 128 evaluation seeds, four boundary strata, and four
snapshots per stratum: 2,048 independently reset states. Each policy receives
the same pending critical work, installed calendar, older booking, and new
request. `generate()` specifies the fallback positions, pending-work draws,
booking boundaries, and staged payload construction. The protocol records the
arrival contracts, resource ranges, release modes, observation horizon, and
evaluation seeds. Initial-state checks and rejected cases remain recorded;
the benchmark is not a sequential booking history.

### Online traffic and execution

The nominal workload has 100 sources. Periods below are generator parameters,
not fixed realized interarrival times: load, context scaling, critical bursts,
and timing variation modify arrivals.

| Source | Count | Bytes per release | Nominal period (ms) | Original deadline (ms) |
| --- | ---: | ---: | ---: | ---: |
| Robot | 5 | 96 | 20 | 20 |
| PLC | 10 | 128 | 50 | 50 |
| AGV | 10 | 256 | 100 | 100 |
| Alarm | 2 | Eight 128-byte packets | 2000 | 50 |
| Meter | 55 | 128 | 1000 | 500 |
| Monitoring | 16 | 256 | 200 | 200 |
| Video/data | 2 | 1200 | 10 | 100 |

The `deadline_mix` family changes robot/PLC/AGV deadlines to 15/60/120 ms,
alarm to 60 ms, and meter/monitoring/video deadlines to 150/80/50 ms.
`workload.hpp` specifies all arrival and family-specific rules, including
release rounding and synchronized background bursts. `protocol.json` specifies
the four workload families, 12 evaluation contexts, three seeds, six traffic
cells, shared controller settings, and the scope of each literature-derived
adapter. Calibration and evaluation contexts are separate within this benchmark;
the records do not assert that evaluation contexts were untouched throughout
all earlier method development.

Critical burst allowances are (480, 3328, 2560) bytes. At loads 0.4, 0.8, and
1.4, the three critical envelope rates are (38, 42, 40), (75, 83, 80), and
(131, 145, 140) bytes/ms, respectively. Ingress shaping has a 5-ms allowance;
the simulation uses no additional execution guardband.

The simulator uses 1-ms byte-preemptive service, 106 abstract units, and
650 bytes/ms; offers last 5 s, followed by a 1001-ms drain. The queue limit is
16 MiB per class. EDF is used within each class. Guaranteed tenant and class
quotas are served first, and surplus is shared among nonempty original
critical/video/monitoring classes with weights 20:65:15. The main comparison
discards expired background work and counts it as failed. All planning methods
share the same certificate, ledger, offered traces, and headroom factor of one.

### Incumbent reserve

| Traffic | Maximum bytes per job | Period (ms) | Deadline (ms) | Nominal allocation |
| --- | ---: | ---: | ---: | --- |
| Critical baseline | 500 | 5 | 8 | 21 units |
| Additional critical source | 950 | 5 | 8 | Combined critical peak: 49 units |
| Video | ceil(2400 * video load) | 10 | 15 | ceil(40 * video load) units |
| Monitoring | 100 | 10 | 20 | 5 units |
| Best effort | Uniform integer 1500-2500 | 5 | 50 | Unprotected |

Protected job sizes are uniform integers from ceil(0.85 * maximum) through
the maximum. Each source has an independent integer phase in [0, period).
Video loads are 0.6, 0.8, 1, 1.2, and 1.4; service factors are 1, 0.95, and
0.9. Service is `floor(650 * factor * units / 106)` bytes per slot, evaluated
with exact decimal-rational arithmetic. Incumbent allocations are
`ceil(nominal_units / factor)`. The no-video-contract diagnostic instead uses
27 nominal video units and is not a protected operating mode.

The critical update is requested at 390 ms, applies at 400 ms, and returns to
baseline at 810 ms. Additional critical releases occur during [410, 800) ms.
Tenant offers occur each millisecond in [100, 1050) with probability
`video_load / 18`, use 4/8/12/16/20 units and 40/80/120-ms windows, and have a
10-ms activation lead. Offered payload is 65%, 85%, or 100% of the nominal
integer-byte quota times the window length, rounded down. Payload is released
upfront, in thirds, or in halves with the second half at 90% of the window.
The release certificate determines acceptance. Reduced delivered service does
not reduce offered tenant payload or requested units.

Guaranteed quotas are supplied first; unused capacity is redistributed by
global EDF. Expired unsent work is discarded and counted as failed. Offers
stop before 1200 ms, and execution lasts 1250 ms. Primary efficiency credits
only complete timely items, divided by the nominal capacity of
650 * 1250 bytes, including reduced-service cases. The protocol specifies
128 evaluation seeds, separate from eight development seeds. Infeasible
protected configurations remain explicit provisioning rejections.

### Context-level comparisons and figure uncertainty

`results/online/context_points.jsonl` retains 36 context-level comparisons:
three adapters against GAR-H across 12 contexts, each averaging three seeds
and six traffic cells. `context_cluster_summary.csv`, `context_contrasts.json`,
and `paired_effects.csv` in the same directory retain the associated summaries
and paired inference. Stored contrasts use adapter minus GAR-H; reverse the
signs and interval endpoints when presenting GAR-H minus adapter.

Reserve mean-series error bars are pointwise 95% seed-bootstrap intervals.
Workload records are paired across policies, but intervals for differences
are computed separately from matched seed-level differences. These are stored
under `paired_comparisons` in `results/reserve/summary.json`. The 9.21-point
nominal borrowing gain is calculated before rounding the displayed means.

`python run.py analyze --output runs/analysis` reconstructs recorded-data
plotting summaries and packet timing analysis and compares them against the
packaged outputs. It does not rerun experiments or recompute the online
context-bootstrap inference; the full `online` command produces that inference.
`python run.py plot --output runs/figures` additionally generates quantitative
plots from the recorded results as PNG and SVG. The default online figure
shows aggregate means; context-level evidence remains available in the files
above.

### Packet configuration and transport

After extraction, the exact recorded controlled-study protocol and inputs are
under `runs/packet-records/protocol/PROTOCOL.json` and
`runs/packet-records/protocol/inputs/`. Files in `data/packet/config/` are base
templates, not complete controlled-trial configurations. The generator applies
the declared overrides, including a 20-ms admission/actuation lead.

Control messages use Unix-domain datagrams. Original-source ingress and
pacer-to-sink delivery use loopback UDP; the gateway-to-pacer payload handoff
and acknowledgments use Unix-domain datagrams. Historical and controlled
packet cohorts remain separate. Absolute paths preserved in historical
metadata identify original provenance and are not current execution paths.

## Layout

`experiments/` contains the final implementations and protocols; `data/` holds
inputs and compressed packet records; `results/` holds the reported outcomes;
`analysis/` contains statistics and Python plotting code; `tests/` contains
regression checks. Each study has its own workload and statistical unit.
Rejected requests, infeasible configurations, and unsuccessful packet outcomes
remain in the evidence. The resource units describe an abstract gateway budget.
The original 13-million-row telemetry corpus is not bundled; the current
simulation studies run entirely from the supplied generators and inputs.

`python run.py verify` checks the file manifest. After an intentional code or
metadata update, regenerate it with `python run.py manifest`, then run
`python run.py check`. Keep new experiments in new output directories.

No software or data license is assigned. Contact the author regarding reuse.
