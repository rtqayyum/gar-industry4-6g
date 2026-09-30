#!/usr/bin/env python3
"""Run the final experiments, analyses, and repository checks."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
IGNORE = {".git", ".venv", "__pycache__", ".DS_Store", "runs", ".pytest_cache"}
COMMANDS = ("verify", "check", "manifest", "matched", "online", "reserve",
            "provisioning", "gateway", "formal", "analyze", "plot",
            "packet-verify", "packet-extract", "packet-analyze")


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def files():
    for path in sorted(ROOT.rglob("*")):
        relative = path.relative_to(ROOT)
        if any(part in IGNORE for part in relative.parts):
            continue
        if path.is_symlink():
            raise ValueError(f"Linked repository file: {relative}")
        if path.is_file() and relative.as_posix() != "manifest.json":
            yield path


def inventory():
    return [{"path": p.relative_to(ROOT).as_posix(), "bytes": p.stat().st_size,
             "sha256": digest(p)} for p in files()]


def verify():
    saved = json.loads((ROOT / "manifest.json").read_text())
    actual = inventory()
    if saved["files"] != actual:
        old = {r["path"]: r for r in saved["files"]}
        new = {r["path"]: r for r in actual}
        changed = [key for key in sorted(old.keys() | new.keys()) if old.get(key) != new.get(key)]
        raise ValueError("Changed repository files: " + ", ".join(changed[:10]))
    return {"status": "PASS", "files": len(actual), "manifest_sha256": digest(ROOT / "manifest.json")}


def environment():
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONOPTIMIZE", None)
    return env


def call(script, *arguments):
    subprocess.run([sys.executable, str(ROOT / script), *map(str, arguments)],
                   cwd=ROOT, env=environment(), check=True)


def new_output(value):
    if value is None:
        raise ValueError("This command requires --output NEW_DIRECTORY")
    destination = value.expanduser().resolve()
    if destination.exists() or ROOT.is_relative_to(destination):
        raise ValueError("Choose a new output directory, not the repository or its ancestor")
    if destination.is_relative_to(ROOT) and not destination.is_relative_to(ROOT / "runs"):
        raise ValueError("Outputs inside the repository must be below runs/")
    return destination


def check():
    total, suites = 0, {}
    for test in sorted((ROOT / "tests").glob("test_*.py")):
        result = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests",
                                 "-p", test.name, "-v"], cwd=ROOT, env=environment(),
                                capture_output=True, text=True)
        output = result.stdout + result.stderr
        if result.returncode:
            raise RuntimeError(output)
        match = re.search(r"Ran (\d+) tests?", output)
        if not match or int(match.group(1)) == 0:
            raise RuntimeError(f"No tests executed: {test.name}")
        suites[test.stem] = int(match.group(1))
        total += suites[test.stem]
    return {"status": "PASS", "tests": total, "suites": suites}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--records", type=Path, help="Extracted packet records for packet-analyze")
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive")
    if args.command == "manifest":
        rows = inventory()
        target = ROOT / "manifest.json"
        target.write_text(json.dumps({"files": rows}, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": "PASS", "files": len(rows), "manifest_sha256": digest(target)}))
        return
    before = verify()
    if args.command == "verify":
        print(json.dumps(before, indent=2))
        return
    if args.command == "check":
        result = check()
    elif args.command == "packet-verify":
        call("experiments/packet/records.py", "verify")
        result = {"status": "PASS"}
    else:
        try:
            output = new_output(args.output)
        except ValueError as error:
            parser.error(str(error))
        output.parent.mkdir(parents=True, exist_ok=True)
        if args.command in {"matched", "online", "reserve"}:
            extra = ["--workers", args.workers] if args.command == "online" else []
            call(f"experiments/{args.command}/run.py", "--output", output, *extra)
        elif args.command == "gateway":
            call("experiments/gateway/verify.py", "--output", output)
        elif args.command == "provisioning":
            output.mkdir(parents=True)
            call("experiments/reserve/provisioning.py", "--output", output / "provisioning.json")
        elif args.command == "formal":
            output.mkdir(parents=True)
            call("experiments/formal/verify_calendar.py", "--output", output / "calendar")
            call("experiments/formal/verify_payload.py", "--output", output / "payload")
        elif args.command in {"analyze", "plot"}:
            call("analysis/run.py", "--output", output,
                 *(["--plots"] if args.command == "plot" else []))
        elif args.command == "packet-extract":
            call("experiments/packet/records.py", "extract", "--output", output)
        elif args.command == "packet-analyze":
            if args.records is None:
                parser.error("packet-analyze requires --records EXTRACTED_DIRECTORY")
            call("experiments/packet/records.py", "analyze", "--records", args.records.resolve(),
                 "--output", output)
        result = {"status": "PASS", "command": args.command, "output": str(output)}
    if verify() != before:
        raise RuntimeError("Packaged code or evidence changed during execution")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
