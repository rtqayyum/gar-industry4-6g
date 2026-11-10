# GAR revision experiment campaign

This revision adds a continuous booking-history study using the released production certificate, controller, request planner and timed executor. The earlier recorded cohorts remain intact. Source and output identities are recorded separately for every new campaign.

## Experimental blocks

| Block | Executable evidence | Scope |
|---|---|---|
| E0 | Repository checks, matched and reserve replays, formal and gateway checks, recorded analysis, packet archive verification | Reproduction and recorded evidence |
| E1 | 3600 continuous episodes, five methods, four generated families, three loads, two request pressures, thirty independent seed blocks | Byte-preemptive single gateway |
| E2 | Development-only selection at 1, 3 and 5 percent empirical background failure targets, then held-out evaluation | Four literature-inspired adapters under the original online model |
| E3 | Parameter sweeps and delay-duration and burst-supply interactions | Sensitivity; no unsafe shortening of the certificate horizon |
| E4 | Queued work, classes, horizon, candidate floors and booking-count fixtures | Computation and Linux process memory |
| E5 | Controller interruptions during sustained operation and the released gateway transaction checks | Does not establish durable gateway restart recovery |
| E6 | Declared lower supply bounds and unannounced service drops | Supply abstraction; no physical wireless or general multi-hop validation |
| E7 | Verification of recorded packet archives in E0 | New live packet repair and strict host supply validation remain open |

E1 uses 300 seconds of offers followed by a common drain. Its new generators enforce three token-bucket critical contracts and periodic video/monitoring contracts. The workload family names derive from the earlier suite, but this is a new bounded generator, not a replay of measured factory packets. Critical deadlines are 15/45/95 ms except the deadline-mix residual deadlines 10/55/115 ms. Video uses a maximum of ceil(2400 times load) bytes per 10 ms with a 15 ms deadline; monitoring uses at most 100 bytes per 10 ms with a 20 ms deadline. Best-effort work has no reserved contract.

All E1 methods receive identical offered work, release patterns and resource budgets. The methods are peak reservation, keep-calendar, an event-based minimum feasible floor, GAR, and GAR with a causal headroom screen. With a constant initial baseline calendar the first two methods coincide; that equality is reported, not treated as independent evidence. The minimum-floor method uses the same full future-demand, activation-prefix, tail and earlier-booking checks. No unsafe comparator is presented as an equal-reliability alternative.

Temporary requests use 4/8/16/24 nominal units and 20/100/500/2000 ms windows, with heavier sizes/windows in the heavy-tenant family. Upfront, half-window and late staged releases remain explicit; requests violating availability are rejected and retained in the offered denominator. Resource quantization is documented when N changes. Command preference is scaled with N in the granularity diagnostic. The optional forecast history is varied over 50/100/200/500 ms, independently of the sufficient certificate horizon. Service-drop diagnostics change delivered capacity halfway through the episode. In bounded cases the conservative lower supply bound is declared from the start; unannounced cases retain the nominal bound and report its violation.

The model uses nominal C=650 bytes/ms and N=106 abstract units. The critical tail is provisioned from the production certificate. Protected background floors are calculated from their actual periodic contracts and the declared lower service bound. Initial infeasibility is recorded without resampling. Unused rights may be borrowed, but never become new irrevocable rights.

## Statistics and evidence

Development and test namespaces are separate. Primary test blocks are 1000001 through 1000030; pilot and calibration use other namespaces. The four workload families are fixed study conditions, not globally unseen application families. Thirty blocks is a fixed budget assumption, not a guarantee of a desired confidence width.

The collector computes paired differences at the seed-block level, with 3000 bootstrap draws and 20000 sign-flip draws. Nine endpoint-by-control contrasts receive Holm adjustment. Packets, requests, traffic cells and repeated timings are not independent samples. Full offered denominators, provisioning rejections, failed tasks, unannounced supply deficits and adverse outcomes are retained. Empirical target attainment is reported on test data, including whether the upper bootstrap interval meets the target.

Every accepted E1 interval is audited with an independent Python capacity sweep and staged-release check. Original-job trace audits are retained for the first primary block and all pilot episodes. All episodes retain compressed accepted-tenant and calendar records. SHA-256 identifies configurations, files and binaries; a labelled FNV fingerprint additionally checks common E1 offered inputs. This audit does not replace the conditional general proof.

## HPC workflow

The bootstrap downloads and validates the pinned GitHub archive on a longq compute job. It creates Python 3.12.12 with pinned uv 0.9.5, installs the repository requirements and records the environment. No Git executable or GPU is needed. Simulation workers use at most 40 CPUs per job; six array elements run at most four at a time. Scaling runs serially within its allocation and reports descriptive host timing.

The dependency chain is acquire, pilot, calibrate, experiment array and collector. Reproduction and scaling have their own dependent jobs. Invalid dependencies are cancelled automatically. The collector uses afterany so missing/failed work produces an incomplete report. Duplicate submissions of the same commit are prevented unless --new is explicit. Partial submission failure cancels only the newly created jobs.

The command prints job IDs, logs, the report path and a Check_revision command. The final report is report/SUMMARY.md with metrics, paired effects, reliability-target results, coverage and report_bundle.zip. Keep raw tasks for audit.

## Completion boundary

Numerical completion does not automatically resolve every review concern. The report explicitly tracks industrial data, gateway crash durability, multi-hop scope, the live packet executor, proof integration and the exact response matrix. No journal submission is performed. The full research plan also requires a revised manuscript and a point-by-point response.
