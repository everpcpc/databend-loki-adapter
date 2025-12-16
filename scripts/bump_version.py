#!/usr/bin/env python3
import argparse
import pathlib
import re
import subprocess


ROOT = pathlib.Path(__file__).resolve().parents[1]
CARGO_TOML = ROOT / "Cargo.toml"


def read_package_version():
    lines = CARGO_TOML.read_text().splitlines()
    in_package = False
    version_line_index = None
    current_version = None
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped != "[package]":
            in_package = False
        if stripped == "[package]":
            in_package = True
            continue
        if in_package and stripped.startswith("version"):
            match = re.search(r'"([^"]+)"', line)
            if not match:
                raise SystemExit("Failed to parse version in Cargo.toml")
            current_version = match.group(1)
            version_line_index = idx
            break
    if current_version is None or version_line_index is None:
        raise SystemExit("Could not find [package] version in Cargo.toml")
    return current_version, lines, version_line_index


def ensure_clean_state() -> None:
    result = subprocess.run(
        ["git", "status", "--porcelain", "Cargo.toml", "Cargo.lock"],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=True,
    )
    if result.stdout.strip():
        raise SystemExit(
            "Cargo.toml or Cargo.lock has uncommitted changes. Commit or discard "
            "them before running the bump script."
        )


def compute_next_version(version: str, bump: str) -> str:
    parts = version.split(".")
    if len(parts) != 3:
        raise SystemExit(f"Unsupported version format: {version}")
    major, minor, patch = (int(p) for p in parts)
    if bump == "major":
        major += 1
        minor = 0
        patch = 0
    elif bump == "minor":
        minor += 1
        patch = 0
    elif bump == "patch":
        patch += 1
    else:
        raise SystemExit(f"Unknown bump type: {bump}")
    return f"{major}.{minor}.{patch}"


def write_cargo_toml(lines, index: int, new_version: str) -> None:
    target_line = lines[index]
    updated_line = re.sub(r'"[^"]+"', f'"{new_version}"', target_line, count=1)
    lines[index] = updated_line
    CARGO_TOML.write_text("\n".join(lines) + "\n")


def refresh_lockfile() -> None:
    subprocess.run(
        ["cargo", "check"],
        cwd=ROOT,
        check=True,
    )


def create_commit(new_version: str) -> None:
    subprocess.run(
        ["git", "add", "Cargo.toml", "Cargo.lock"],
        check=True,
        cwd=ROOT,
    )
    subprocess.run(
        ["git", "commit", "-m", f"chore: bump version v{new_version}"],
        check=True,
        cwd=ROOT,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Bump crate version and create commit."
    )
    parser.add_argument(
        "bump",
        choices=["major", "minor", "patch"],
        help="Which part of the version to increment",
    )
    args = parser.parse_args()

    ensure_clean_state()
    current_version, lines, version_index = read_package_version()
    new_version = compute_next_version(current_version, args.bump)
    write_cargo_toml(lines, version_index, new_version)
    refresh_lockfile()
    create_commit(new_version)
    print(f"Bumped version: {current_version} -> {new_version}")


if __name__ == "__main__":
    main()
