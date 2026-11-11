# Matched snapshot controls

This replay uses the original 2,048 held-out snapshots, offers, and execution model. Each baseline keeps the booking start, duration, traffic, and prior commitments fixed.

| Policy or control | Accepted offers |
|---|---:|
| Peak reservation | 743 |
| Keep calendar | 845 |
| Certified constant floor after actual activation | 1,119 |
| Full GAR with certified temporary allocation | 2,029 |
| Minimum pulse searched as if activation were immediate | 2,011 |

The constant-floor control keeps a feasible installed calendar when possible and otherwise tries the certified continuing floor of 49 units after the actual activation lead. It accounts for 274 of the 1,184 admissions beyond keeping the calendar. Certified temporary allocation enables the other 910. In the noncrossing stratum the constant-floor control and keep calendar each admit 288 offers, and full GAR admits 506.

The zero-lead search keeps the actual booking start and duration. We replay its decisions with the installed 4- or 8-ms prefix and count certificate and ledger violations and incumbent, old-tenant, and new-tenant deadline misses. All five counts are zero in this case bank.

[Case-level CSV](snapshot_diagnostics.csv) and [JSON summary](snapshot_diagnostics.json) report all offers and the breakdown by stratum. From the repository root, reproduce them with Python 3 and a C++17 compiler on `PATH`.

```sh
python3 -m experiments.revision.snapshot_controls --output runs/snapshot-controls
```
