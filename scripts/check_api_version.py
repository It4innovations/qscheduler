#!/usr/bin/env python3
"""Checks that the workspace version is bumped enough for the API changes since the last release.

The baseline is `openapi.json` at the latest `vX.Y.Z` tag. API changes are classified with
oasdiff (https://github.com/oasdiff/oasdiff):

- breaking (any oasdiff `error`): 0.y.z -> bump y, x.y.z (x >= 1) -> bump x
- additive (any other change):    0.y.z -> bump z, x.y.z (x >= 1) -> bump y
- none:                            any version >= the tag

Usage: scripts/check_api_version.py [--base-spec FILE --base-version X.Y.Z] [--spec FILE]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parent.parent


def parse_version(text):
    m = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", text.strip())
    if not m:
        sys.exit(f"Error: not a X.Y.Z version: {text!r}")
    return tuple(int(x) for x in m.groups())


def fmt(version):
    return ".".join(str(x) for x in version)


def git(*args):
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True, check=False
    )


def latest_release():
    """Returns (version, spec text) of the latest release tag, or None if there is none."""
    tag = git("describe", "--tags", "--abbrev=0", "--match", "v[0-9]*")
    if tag.returncode != 0:
        return None
    tag = tag.stdout.strip()
    spec = git("show", f"{tag}:openapi.json")
    if spec.returncode != 0:
        sys.exit(f"Error: release tag {tag} has no openapi.json")
    return parse_version(tag), spec.stdout


def without_version(spec_text, directory, name):
    """Writes the spec without `info.version`, so a bump alone is not reported as a change."""
    spec = json.loads(spec_text)
    spec.get("info", {}).pop("version", None)
    path = Path(directory, name)
    path.write_text(json.dumps(spec))
    return str(path)


def classify(base_text, spec_text, oasdiff):
    """Returns ("breaking" | "additive" | "none", list of change descriptions)."""
    with tempfile.TemporaryDirectory() as tmp:
        base = without_version(base_text, tmp, "base.json")
        cur = without_version(spec_text, tmp, "current.json")
        out = subprocess.run(
            [oasdiff, "changelog", base, cur, "--format", "json"],
            capture_output=True,
            text=True,
            check=False,
        )
    if out.returncode != 0:
        sys.exit(f"Error: oasdiff failed:\n{out.stderr}")
    # oasdiff's own version rule is replaced by the bump rules of this script
    changes = [
        c
        for c in json.loads(out.stdout or "[]")
        if c.get("id") != "api-version-not-bumped"
    ]
    # oasdiff levels: 1 = info, 2 = warning, 3 = error (breaking)
    levels = {1: "info", 2: "warning", 3: "error"}
    lines = [
        f"{levels.get(c.get('level'), '?')} {c.get('operation', '')} "
        f"{c.get('path', '')}: {c.get('text', c.get('id', ''))}"
        for c in changes
    ]
    if any(c.get("level") == 3 for c in changes):
        return "breaking", lines
    if changes:
        return "additive", lines
    return "none", lines


def required_version(base, change):
    major, minor, patch = base
    if change == "breaking":
        return (0, minor + 1, 0) if major == 0 else (major + 1, 0, 0)
    if change == "additive":
        return (0, minor, patch + 1) if major == 0 else (major, minor + 1, 0)
    return base


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--spec", type=Path, help="current spec (default: openapi.json)"
    )
    parser.add_argument(
        "--base-spec", type=Path, help="baseline spec (default: from tag)"
    )
    parser.add_argument("--base-version", help="baseline version (default: from tag)")
    parser.add_argument("--oasdiff", default=os.environ.get("OASDIFF", "oasdiff"))
    args = parser.parse_args()

    spec_path = args.spec or ROOT / "openapi.json"
    spec_text = spec_path.read_text()
    version = parse_version(json.loads(spec_text)["info"]["version"])
    if args.spec is None:
        cargo = tomllib.loads((ROOT / "Cargo.toml").read_text())
        cargo_version = parse_version(cargo["workspace"]["package"]["version"])
        if version != cargo_version:
            sys.exit(
                f"Error: openapi.json has version {fmt(version)}, Cargo.toml has "
                f"{fmt(cargo_version)}; regenerate the snapshot "
                "(UPDATE_OPENAPI=1 cargo test -p service --test openapi_snapshot)"
            )

    if (args.base_spec is None) != (args.base_version is None):
        sys.exit("Error: --base-spec and --base-version must be given together")
    if args.base_spec is not None:
        base_version, base_text = (
            parse_version(args.base_version),
            args.base_spec.read_text(),
        )
    else:
        release = latest_release()
        if release is None:
            print("No release tag (vX.Y.Z) found; skipping the API version check.")
            return
        base_version, base_text = release

    change, lines = classify(base_text, spec_text, args.oasdiff)
    needed = required_version(base_version, change)
    for line in lines:
        print(f"  {line}")
    print(
        f"API change since {fmt(base_version)}: {change}; "
        f"version must be >= {fmt(needed)}, is {fmt(version)}"
    )
    if version < needed:
        sys.exit("Error: version bump is too small for the API changes above")
    print("OK")


if __name__ == "__main__":
    main()
