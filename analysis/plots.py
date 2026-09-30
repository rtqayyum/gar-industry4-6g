#!/usr/bin/env python3
"""Draw experimental plots from the recorded results.

Panels retain the study-specific units, denominators and uncertainty estimates.
No measured values, intervals or rejected configurations are imputed here.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
COLORS = ["#28786e", "#365b83", "#b7802f", "#925944"]
POLICIES = ["borrowable", "dedicated_idle", "no_tenant_control", "no_video_contract"]
LABELS = ["Borrowable", "Dedicated idle", "No tenants", "No video contract"]
RESERVE_COLORS = [COLORS[0], COLORS[2], COLORS[1], COLORS[3]]


def table(path):
    return np.genfromtxt(path, names=True)


def finish(fig, output, name):
    fig.savefig(output / (name + ".png"), dpi=220)
    fig.savefig(output / (name + ".svg"))
    plt.close(fig)


def certificate(data, output):
    calendar = table(data / "matched/certificate_calendar.dat")
    slack = table(data / "matched/certificate_slack.dat")
    margin = table(data / "matched/certificate_margin.dat")
    fig, axes = plt.subplots(1, 3, figsize=(11.5, 3.2), layout="constrained")
    for field, label, color in [("old", "Old", COLORS[2]), ("new", "Committed", COLORS[0])]:
        style = "--" if field == "old" else "-"
        axes[0].step(calendar["n"], calendar[field], where="post", label=label, color=color, ls=style)
        axes[1].semilogx(slack["b"], slack[field], label=label, color=color, ls=style)
    for field, label, color in [("old_plus_request", "Old + request", COLORS[3]),
                               ("new", "Committed", COLORS[0])]:
        axes[2].step(margin["n"], margin[field], where="post", label=label, color=color,
                     ls="--" if field == "old_plus_request" else "-")
    axes[0].set(xlim=(0, 20), ylim=(0, 75), xticks=[0, 4, 6, 9, 16, 20], yticks=[0, 21, 49, 65],
                xlabel="Time (ms)", ylabel="Critical resource units")
    axes[0].plot([0, 16], [4, 4], color=COLORS[1], lw=2)
    axes[0].text(11, 6, "Older booking (time span)", ha="center", fontsize=7)
    axes[1].set(xlim=(1, 256), ylim=(0, 440), yticks=[0, 128, 300, 400],
                xlabel="Interval end (ms; log scale)", ylabel="Minimum service slack (bytes)")
    axes[1].set_xticks([1, 4, 16, 64, 256], ["1", "4", "16", "64", "256"])
    axes[2].set(xlim=(0, 20), xticks=[0, 4, 9, 16, 20], xlabel="Time (ms)", ylabel="Resource margin (units)")
    axes[2].annotate("-3", xy=(5, -3), xytext=(5, 5), ha="center", color=COLORS[3], fontsize=8,
                     arrowprops={"arrowstyle": "->", "color": COLORS[3], "lw": .7})
    axes[2].axhline(0, color="0.35", lw=.8)
    for axis in (axes[0], axes[2]):
        axis.axvspan(4, 9, color="0.85", alpha=.45, zorder=-2)
    for axis, title in zip(axes, ["(a) Committed critical calendar", "(b) All-interval deadline margin", "(c) Resource margin"]):
        axis.set_title(title, fontsize=10)
        axis.legend(fontsize=7, frameon=False)
    finish(fig, output, "certified_replacement")


def admission(data, output):
    effects = table(data / "matched/guard_effects.dat")
    counts = table(data / "matched/guard_nested.dat")
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.3), layout="constrained")
    position = np.arange(4)
    axes[0].barh(position, effects["gain_pp"], color=COLORS[0], height=.55)
    axes[0].errorbar(effects["gain_pp"], position,
                    xerr=[effects["minus"], effects["plus"]], fmt="none", color="black", capsize=3)
    axes[0].set(yticks=position, yticklabels=["Prefix fallback", "Window fallback", "Booking boundary", "No crossing"],
                xlabel="GAR - keep-calendar (percentage points)", xlim=(0, 85),
                title="(a) Paired admission gain")
    axes[0].invert_yaxis()
    for y, value in zip(position, effects["gain_pp"]):
        axes[0].text(value - 6, y, f"{value:.1f}", ha="right", va="center", color="white")
    if len(counts) != 4 or not np.array_equal(counts["count"], [743, 102, 1184, 19]):
        raise ValueError("Expected the four disjoint nested admission categories")
    bars = axes[1].bar(position, counts["count"], color=COLORS[1], width=.6)
    axes[1].bar_label(bars, labels=[f"{int(value):,}" for value in counts["count"]], padding=3)
    axes[1].set(xticks=position, xticklabels=["Peak", "Keep-calendar\nonly", "GAR only", "Rejected"],
                ylabel="Offers (of 2,048)", ylim=(0, 1370), title="(b) Nested admission decisions")
    finish(fig, output, "matched_admission")


def online(data, output):
    rows = table(data / "online/policy_overview.dat")
    fig, axes = plt.subplots(1, 2, figsize=(9, 3), layout="constrained")
    for axis, metric, label in zip(axes, ("efficiency", "background_failure"),
                                   ("Timely payload efficiency (%)", "Background deadline failures (%)")):
        bars = axis.barh(np.arange(4), rows[metric], color=COLORS, height=.6)
        axis.set(yticks=np.arange(4), yticklabels=["GAR-H", "Joint (L-J)", "Sequential (L-S)", "Event-driven (Y-E)"],
                 xlabel=label, xlim=(0, max(rows[metric]) * 1.2))
        axis.invert_yaxis()
        axis.bar_label(bars, fmt="%.2f", padding=3, fontsize=8)
    axes[0].set_title("(a) Timely payload efficiency", fontsize=10)
    axes[1].set_title("(b) Background deadline failures", fontsize=10)
    finish(fig, output, "online_policies")


def reserve(data, output):
    values = json.loads((data / "reserve/plot_data.json").read_text())
    fig, axes = plt.subplots(2, 2, figsize=(10, 7), layout="constrained")
    loads = [.6, .8, 1, 1.2, 1.4]
    positions = np.arange(len(loads))
    width = .18
    for index, (policy, label, color) in enumerate(zip(POLICIES, LABELS, RESERVE_COLORS)):
        rows = {r["load"]: r for r in values["load_line"] if r["factor"] == 1 and r["policy"] == policy}
        mean = [100 * rows[x]["efficiency"]["mean"] if x in rows else np.nan for x in loads]
        low = [100 * rows[x]["efficiency"]["low"] if x in rows else np.nan for x in loads]
        high = [100 * rows[x]["efficiency"]["high"] if x in rows else np.nan for x in loads]
        errors = [np.array(mean) - low, np.array(high) - mean]
        if any(np.any(np.isfinite(error) & (error < 0)) for error in errors):
            raise ValueError(f"Efficiency mean outside its interval: {policy}")
        axes[0, 0].bar(positions + (index - 1.5) * width, mean, width=width,
                        yerr=errors, capsize=2, label=label, color=color,
                        error_kw={"elinewidth": .7, "capthick": .7})
        if policy in ("borrowable", "no_video_contract"):
            offset = -.19 if policy == "borrowable" else .19
            failure = [rows[x]["video_failure_percent"] if x in rows else np.nan for x in loads]
            bars = axes[0, 1].bar(positions + offset, failure, width=.32, color=color, label=label)
            axes[0, 1].bar_label(bars,
                                labels=[("0" if value == 0 else f"{value:.1f}") if np.isfinite(value) else ""
                                        for value in failure], padding=3, fontsize=8, color=color)
        if policy not in ("borrowable", "no_tenant_control"):
            continue
        epochs = {r["epoch"]: r for r in values["epoch_bars"]
                  if r["factor"] == 1 and r["load"] == 1 and r["policy"] == policy and r["kind"] == "video"}
        offset = -.15 if policy == "borrowable" else .15
        axes[1, 0].bar(np.arange(3) + offset,
                        [epochs[e]["p99_ms_mean"] for e in ("before", "during", "after")],
                        width=.27, color=color, label=label)
    pairs = {}
    for row in values["seed_scatter"]:
        if row["load"] == 1 and row["factor"] == 1:
            pairs.setdefault(row["seed"], {})[row["policy"]] = row["timely_payload_efficiency_nominal"] * 100
    if len(pairs) != 128:
        raise ValueError("Expected all 128 paired reserve seeds")
    x = [pairs[seed]["dedicated_idle"] for seed in sorted(pairs)]
    y = [pairs[seed]["borrowable"] for seed in sorted(pairs)]
    axes[1, 1].scatter(x, y, s=13, alpha=.65, color=COLORS[0])
    lower, upper = 55, 85
    if min(x + y) < lower or max(x + y) > upper:
        raise ValueError("Reserve scatter values exceed the configured display range")
    axes[1, 1].plot([lower, upper], [lower, upper], "--", color="0.5")
    axes[1, 1].set(xlim=(lower, upper), ylim=(lower, upper), xlabel="Dedicated idle (%)", ylabel="Borrowable (%)",
                   title="(d) Paired efficiency, 128 seeds")
    axes[1, 0].axhline(15, color="black", ls="--", lw=1, label="Video deadline")
    axes[1, 0].set(xticks=np.arange(3), xticklabels=["Before", "During", "After"], ylabel="Mean per-run video p99 (ms)",
                   ylim=(0, 17), title="(c) Video through the event")
    axes[0, 0].set(xlabel="Video load multiplier", ylabel="Efficiency (%)", ylim=(0, 100),
                   title="(a) Complete timely payload")
    axes[0, 1].set(xlabel="Video load multiplier", ylabel="All-offered video failures (%)", ylim=(-3, 90),
                   title="(b) All-offered video failures")
    for axis in axes[0]:
        axis.set_xticks(positions, [f"{load:g}" for load in loads])
    # Hatch only the protected configurations: the diagnostic at 1.4 ran.
    axes[0, 0].axvspan(4 - 2 * width, 4 + width, facecolor="none", edgecolor="0.75", hatch="///", zorder=-2)
    axes[0, 0].text(4 - width / 2, 97, "Protected:\nnot provisioned", ha="center", va="top", fontsize=8)
    axes[0, 1].axvspan(4 - .35, 4 - .03, facecolor="none", edgecolor="0.75", hatch="///", zorder=-2)
    axes[0, 1].text(4 - .19, 44, "Not provisioned", ha="center", va="center", rotation=90, fontsize=8)
    axes[0, 0].legend(fontsize=8, frameon=False, loc="lower center", bbox_to_anchor=(.5, 1.13), ncol=2)
    axes[0, 1].legend(fontsize=8, frameon=False, loc="upper left")
    axes[1, 0].legend(fontsize=8, frameon=False, loc="upper center", bbox_to_anchor=(.5, .87), ncol=2)
    finish(fig, output, "incumbent_reserve")


def packets(output):
    evidence = json.loads((ROOT / "results/packet/historical/deadline_outcomes.json").read_text())
    methods = ["fixed_h", "predictive_h", "gar_h", "residual_priority_h"]
    outcomes = {r["policy"]: r for r in evidence["outcomes"] if r["traffic_group"] == "critical"}
    with (ROOT / "results/packet/controlled/reservation_provisioning.csv").open(newline="") as stream:
        jobs = [r for r in csv.DictReader(stream) if r["accepted"] == "True"]
    if len(jobs) != 96:
        raise ValueError("Expected 96 accepted controlled requests")
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.4), layout="constrained")
    x = np.arange(4)
    late = np.array([outcomes[p]["late"] for p in methods])
    absent = np.array([outcomes[p]["absent"] for p in methods])
    offered = np.array([outcomes[p]["offered"] for p in methods])
    if not np.all(offered == 255212):
        raise ValueError("Expected 255,212 all-offered critical packets per historical policy")
    late_percent, absent_percent = 100 * late / offered, 100 * absent / offered
    axes[0].barh(x, late_percent, color=COLORS[2], height=.55, label="Late")
    axes[0].barh(x, absent_percent, left=late_percent, color=COLORS[3], height=.55, label="No receipt")
    for i, total in enumerate(late + absent):
        axes[0].text((late_percent[i] + absent_percent[i]) / 2, i, str(int(total)), ha="center", va="center")
    axes[0].set(yticks=x, yticklabels=["Fixed + gate", "Predictive + gate", "GAR-H", "Residual + gate"],
                xlabel="All-offered critical failures (%)", xlim=(0, .04), xticks=[0, .01, .02, .03, .04],
                title="(a) Historical critical deadline failures")
    axes[0].invert_yaxis()
    axes[0].legend(fontsize=8, frameon=False, loc="lower right")
    samples = [[float(r["broker_to_booking_commit_ms"]) for r in jobs if r["policy"] == p] for p in methods]
    axes[1].boxplot(samples, positions=x, widths=.5, whis=(0, 100), showfliers=False,
                     medianprops={"color": "black"})
    for i, policy in enumerate(methods):
        if len(samples[i]) != 24:
            raise ValueError(f"Expected 24 accepted controlled requests: {policy}")
        for condition, marker in [("controller_present", "o"), ("controller_killed", "D")]:
            values = [float(r["broker_to_booking_commit_ms"]) for r in jobs if r["policy"] == policy and r["condition"] == condition]
            jitter = np.linspace(-.12, .12, len(values))
            axes[1].scatter(i + jitter, values, marker=marker, s=17, alpha=.8,
                            edgecolors=COLORS[i], facecolors=COLORS[i] if condition == "controller_present" else "none",
                            label=("Controller present" if condition == "controller_present" else "Controller-stop trial") if i == 0 else None)
    axes[1].set(ylabel="Booking-commit time (ms)", ylim=(0, 8), title="(b) Controlled booking-commit times")
    axes[1].set_xticks(x, ["Fixed\n+ gate", "Predictive\n+ gate", "GAR-H", "Residual priority\n+ gate"])
    axes[1].legend(fontsize=8, frameon=False, loc="upper center", ncol=2)
    finish(fig, output, "packet_execution")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    plt.rcParams.update({"font.family": "serif", "font.size": 9, "axes.spines.top": False,
                         "axes.spines.right": False, "axes.axisbelow": True,
                         "axes.grid": True, "grid.color": "0.9", "grid.linewidth": .6})
    for draw in (certificate, admission, online, reserve):
        draw(args.data, args.output)
    packets(args.output)
    print(json.dumps({"status": "PASS", "figures": 5, "output": str(args.output)}))


if __name__ == "__main__":
    main()
