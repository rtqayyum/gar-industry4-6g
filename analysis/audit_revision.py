#!/usr/bin/env python3
"""Reconcile the frozen GAR revision bundle and summarize the measured outcomes."""
import argparse
import hashlib
import importlib.util
import io
import itertools
import json
from pathlib import Path
import random
import statistics
import zipfile

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "b132708b398c68402b2e8135693dd8d7ea6e39c8"
ENDPOINTS = ["admission_pct", "timely_payload_efficiency_pct", "timely_tenant_bytes_per_offer", "commands"]
FAILURES = ["critical_failed", "tenant_failed", "background_failure_pct"]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load_campaign():
    spec = importlib.util.spec_from_file_location("revision_campaign", ROOT / "experiments/revision/campaign.py")
    campaign = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(campaign)
    return campaign


def validate_manifests(campaign, rows, coverage, lock, selected):
    evaluation = {}
    for label, setting in selected.items():
        for family, load, seed in itertools.product(campaign.FAMILIES, [.8, 1.0], range(1000001, 1000031)):
            task = campaign.task("online", "E2_EVALUATION", dict(policy=label.split(":")[0],
                eta=setting["eta"], step=setting["step"], family=family, load=load, seed=seed, duration=300000))
            evaluation[task["id"]] = task
    manifests = dict(manifest_sha256=campaign.primary() + campaign.diagnostics(),
                     evaluation_sha256=sorted(evaluation.values(), key=lambda t: t["id"]),
                     scaling_sha256=campaign.scaling())
    for key, tasks in manifests.items():
        content = json.dumps(tasks, indent=2, sort_keys=True, allow_nan=False) + "\n"
        require(hashlib.sha256(content.encode()).hexdigest() == lock[key], f"Frozen manifest differs: {key}")
    expected = list(itertools.chain.from_iterable(manifests.values())) + campaign.pilot_tasks() + campaign.calibration_tasks()
    by_id = {t["id"]: t for t in expected}
    require(len(by_id) == len(expected) == len(rows) == rows.id.nunique(), "Task count or uniqueness differs")
    require(set(by_id) == set(rows.id), "Frozen task IDs differ")
    require(len(rows) == coverage["expected"] == coverage["received"], "Coverage count differs")
    require(not any(coverage[k] for k in ["missing", "failed", "input_mismatches", "missing_production_manifests"]),
            "Campaign has incomplete or mismatched evidence")
    for record in rows.to_dict("records"):
        task = by_id[record["id"]]
        require(record["study"] == task["study"] and record["kind"] == task["kind"], "Task metadata differs")
        require(all(record[k] == v for k, v in task["config"].items()), "Task configuration differs")
    for study in ["E1", "E2_EVALUATION"]:
        group = rows[rows.study == study]
        keys = ["family", "load", "seed"] + (["rate"] if study == "E1" else [])
        require(group.input_fingerprint.notna().all() and group.groupby(keys).input_fingerprint.nunique().eq(1).all(),
                f"Offered input fingerprints differ: {study}")
    require(set(rows[rows.study == "E2_CALIBRATION"].seed).isdisjoint(rows[rows.study == "E2_EVALUATION"].seed),
            "Development and evaluation seeds overlap")
    require(set(rows.status) == {"COMPLETE", "PROVISIONING_REJECTION"}, "Unexpected task outcome")
    require(int(rows.status.eq("PROVISIONING_REJECTION").sum()) == coverage["provisioning_rejections"],
            "Provisioning rejection count differs")


def reliability_points(rows, selection, targets):
    development = rows[rows.study == "E2_CALIBRATION"]
    evaluation = rows[rows.study == "E2_EVALUATION"]
    points = []
    for method, target in itertools.product(sorted(development.policy.unique()), [1, 3, 5]):
        label = f"{method}:{target}"
        candidates = []
        for (eta, step), sample in development[development.policy == method].groupby(["eta", "step"]):
            failure = statistics.mean(sample.background_failure_pct)
            if failure <= target:
                candidates.append((statistics.mean(sample.timely_payload_efficiency_pct), -failure, -eta, -step))
        if not candidates:
            require(label in selection["unattainable"], f"Unattainable target differs: {label}")
            continue
        _, failure, eta, step = max(candidates)
        setting = selection["selected"][label]
        require((-eta, -step) == (setting["eta"], setting["step"]), f"Development selection differs: {label}")
        require(abs(-failure - setting["calibration_failure_pct"]) < 1e-9, f"Development mean differs: {label}")
        sample = evaluation[(evaluation.policy == method) & (evaluation.eta == -eta) & (evaluation.step == -step)]
        values = [statistics.mean(block.background_failure_pct) for _, block in sample.groupby("seed", sort=False)]
        require(len(values) == 30 and len(sample) == 240, f"Evaluation coverage differs: {label}")
        rng = random.Random(400)
        upper = sorted(statistics.mean(rng.choices(values, k=len(values))) for _ in range(3000))[2924]
        mean = statistics.mean(values)
        original = targets[(targets.method == method) & (targets.target_pct == target)].iloc[0]
        np.testing.assert_allclose([mean, upper], [original.test_failure_pct, original.test_ci95_upper], atol=1e-9, rtol=0)
        points.append(dict(method=method, target_pct=target, eta=-eta, step_ms=-step,
                           failure_pct=mean, ci95_upper_pct=upper, admission_pct=sample.admission_pct.mean(),
                           timely_payload_efficiency_pct=sample.timely_payload_efficiency_pct.mean(),
                           mean_commands=sample.commands.mean(), seed_blocks=len(values)))
    return pd.DataFrame(points)


def figure(means, path):
    means = means.loc[["peak", "minimum_floor", "gar", "gar_h"]]
    labels = ["Fixed\ncalendar", "Minimum\nfloor", "GAR", "GAR-H"]
    colors = ["#87919e", "#208579", "#27548a", "#a77325"]
    fig, axes = plt.subplots(1, 3, figsize=(11.5, 3.7))
    for ax, field, title in zip(axes, [ENDPOINTS[i] for i in [0, 1, 3]],
            ["Admissions (%)", "Timely payload / capacity (%)", "Control commands per episode"]):
        values = means[field].to_numpy()
        ax.bar(labels, values, color=colors, width=.64)
        ax.set_title(title, fontsize=11, pad=13)
        ax.set_ylim(0, max(values)*1.18)
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", alpha=.2)
        ax.set_axisbelow(True)
        for i, value in enumerate(values):
            ax.text(i, value + max(values)*.023, f"{value:,.0f}" if field == "commands" else f"{value:.2f}",
                    ha="center", fontsize=9)
    fig.suptitle("Sustained study across 30 independent seed blocks", fontsize=13)
    fig.text(.5, .02, "Means across 24 equally weighted conditions. Peak and keep-calendar gave identical results.",
             ha="center", fontsize=8)
    fig.tight_layout(rect=[0, .065, 1, .92])
    fig.savefig(path, dpi=200)
    plt.close(fig)


def audit(bundle, output):
    require(not output.exists(), "Choose a new output directory")
    campaign = load_campaign()
    with zipfile.ZipFile(bundle) as archive:
        require(archive.testzip() is None, "Archive integrity check failed")
        identity = json.loads(archive.read("source_identity.json"))
        lock = json.loads(archive.read("protocol_lock.json"))
        coverage = json.loads(archive.read("report/coverage.json"))
        selection = json.loads(archive.read("selected.json"))
        rows = pd.read_csv(io.BytesIO(archive.read("report/metrics.csv")), float_precision="round_trip",
                           dtype={"id": str, "input_fingerprint": str})
        effects = pd.read_csv(io.BytesIO(archive.read("report/paired_effects.csv")))
        targets = pd.read_csv(io.BytesIO(archive.read("report/reliability_targets.csv")))
    require(identity["commit"] == SOURCE, "Unexpected experiment source commit")
    validate_manifests(campaign, rows, coverage, lock, selection["selected"])
    e1 = rows[rows.study == "E1"]
    require(len(e1) == 3600 and e1.groupby("policy").size().eq(720).all(), "E1 balance differs")
    reproduced = pd.DataFrame(campaign.paired(e1.to_dict("records")))
    effect_keys = ["endpoint", "method", "control"]
    columns = ["seed_blocks", "difference", "ci95_low", "ci95_high", "p", "holm_p"]
    pd.testing.assert_frame_equal(reproduced.set_index(effect_keys)[columns].sort_index(),
                                  effects.set_index(effect_keys)[columns].sort_index(), atol=1e-9, rtol=0)
    cells = e1.groupby(["family", "load", "rate", "seed", "policy"])[ENDPOINTS].mean().unstack("policy")
    for field in ENDPOINTS:
        require(cells[field].peak.eq(cells[field].keep_calendar).all(), "Fixed calendars differ")
    for row in effects.itertuples():
        difference = cells[row.endpoint][row.method] - cells[row.endpoint][row.control]
        point = difference.groupby("seed").mean().mean()
        require(abs(point - row.difference) < 1e-9, "Independent primary point estimate differs")
    e1_means = e1.groupby("policy")[ENDPOINTS + FAILURES].mean().reindex(campaign.METHODS)
    conforming = rows[(rows.kind == "sustained") & (rows.status == "COMPLETE") & (rows.in_contract == 1)]
    require(conforming[FAILURES].eq(0).all().all(), "Observed failure within declared sustained assumptions")
    rejected = rows[rows.status == "PROVISIONING_REJECTION"]
    diagnostics = rows[rows.study == "E3"]
    diagnostics_reference = dict(campaign.BASE, duration=60000, load=.8, rate=100, trace=0)
    condition_keys = list(diagnostics_reference) + ["family", "policy", "seed"]
    duplicates = diagnostics[diagnostics.duplicated(condition_keys, keep=False)]
    for _, block in duplicates.groupby(condition_keys):
        require(block[["status", "input_fingerprint"] + ENDPOINTS + FAILURES].nunique(dropna=False).eq(1).all(),
                "Numerically identical E3 configurations have different recorded outcomes")
    distinct_diagnostics = diagnostics.drop_duplicates(condition_keys)
    sensitivity = []
    for factor in ["delay", "step", "price", "history_window"]:
        subset = distinct_diagnostics
        for field, value in diagnostics_reference.items():
            if field != factor:
                subset = subset[subset[field] == value]
        for (value, method), sample in subset.groupby([factor, "policy"]):
            sensitivity.append(dict(factor=factor, value=value, policy=method, runs=len(sample),
                                    **sample[ENDPOINTS].mean().to_dict()))
    scaling = rows[rows.study == "E4"].groupby(["jobs", "classes", "horizon", "floors", "bookings"]).agg(
        median_p95_us=("p95_us", "median"), max_us=("max_us", "max"),
        median_construction_us=("construction_us", "median"), median_max_rss_kib=("max_rss_kb", "median"),
        runs_above_5ms=("p95_us", lambda x: int(x.gt(5000).sum())), fixtures=("id", "size"))
    points = reliability_points(rows, selection, targets)
    reports = {
        "e1_policy_means.csv": e1_means,
        "e1_condition_means.csv": e1.groupby(["family", "load", "rate", "policy"])[ENDPOINTS].mean(),
        "e2_operating_points.csv": points,
        "e3_provisioning_rejections.csv": rejected.groupby(["capacity", "load", "family", "policy"]).size().rename("rejected_cases"),
        "e3_sensitivity.csv": pd.DataFrame(sensitivity),
        "e4_scaling.csv": scaling,
        "e5_controller_outages.csv": rows[rows.study == "E5_CONTROLLER_ONLY"].groupby(["outage_length", "policy"])[ENDPOINTS+FAILURES].mean(),
        "e6_service_conditions.csv": rows[rows.study == "E6_SERVICE_ABSTRACTION"].groupby(["actual_service", "service", "policy"])[ENDPOINTS+FAILURES].mean(),
    }
    summary = dict(source_commit=SOURCE, bundle_sha256=hashlib.sha256(bundle.read_bytes()).hexdigest(),
        expected_tasks=len(rows), complete=int(rows.status.eq("COMPLETE").sum()), provisioning_rejections=len(rejected),
        frozen_manifest_hashes={key: lock[key] for key in ["manifest_sha256", "evaluation_sha256", "scaling_sha256"]},
        primary_contrasts_reproduced=len(reproduced), reliability_points_reproduced=len(points),
        sustained_conforming_episodes=len(conforming), sustained_conforming_failures=0,
        e3_numeric_duplicate_tasks=len(diagnostics)-len(distinct_diagnostics),
        gar_commands_vs_minimum_floor=float(e1_means.loc["gar", "commands"] / e1_means.loc["minimum_floor", "commands"]))
    output.mkdir(parents=True)
    for name, report in reports.items():
        report.to_csv(output/name, index=not isinstance(report.index, pd.RangeIndex))
    (output/"audit.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    figure(e1_means, output/"primary_comparison.png")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    audit(args.bundle, args.output)
