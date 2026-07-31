"""Upload extracted BCB raw JSON files to a Unity Catalog Volume.

The script wraps the Databricks CLI so the upload step is reproducible and
versioned with the project. It expects the CLI to be installed and authenticated
locally with `databricks auth login` or another supported authentication method.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


EXPECTED_FILES = ("selic.json", "ipca.json")
DEFAULT_LOCAL_DIR = Path("data/raw")
DEFAULT_VOLUME_PATH = "/Volumes/desafio_bcb/default/raw_files"


class UploadError(Exception):
    """Raised when the upload cannot be completed."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload selic.json and ipca.json to a Databricks UC Volume."
    )
    parser.add_argument(
        "--local-dir",
        type=Path,
        default=DEFAULT_LOCAL_DIR,
        help="Directory containing selic.json and ipca.json.",
    )
    parser.add_argument(
        "--volume-path",
        default=DEFAULT_VOLUME_PATH,
        help="Target Unity Catalog Volume path, e.g. /Volumes/catalog/schema/volume.",
    )
    parser.add_argument(
        "--cli-path",
        default="databricks",
        help="Databricks CLI executable path. Defaults to 'databricks' from PATH.",
    )
    parser.add_argument(
        "--profile",
        help="Optional Databricks CLI profile from ~/.databrickscfg.",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Do not overwrite existing files in the target Volume.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands without executing them.",
    )
    return parser.parse_args()


def normalize_volume_path(volume_path: str) -> str:
    """Return a Databricks CLI compatible UC Volume path."""

    clean_path = volume_path.rstrip("/")

    if clean_path.startswith("dbfs:/Volumes/"):
        return clean_path

    if clean_path.startswith("/Volumes/"):
        return f"dbfs:{clean_path}"

    raise UploadError(
        "Volume path must start with '/Volumes/' or 'dbfs:/Volumes/'. "
        f"Received: {volume_path}"
    )


def validate_local_file(path: Path) -> None:
    """Validate that a local raw JSON file exists and has a non-empty payload."""

    if not path.exists():
        raise UploadError(f"Local file not found: {path}")

    if path.stat().st_size == 0:
        raise UploadError(f"Local file is empty: {path}")

    try:
        payload: Any = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise UploadError(f"Invalid JSON file {path}: {exc}") from exc

    if not isinstance(payload, list) or not payload:
        raise UploadError(f"Expected a non-empty JSON list in {path}")


def resolve_cli(cli_path: str) -> str:
    """Resolve the Databricks CLI executable from a path or PATH lookup."""

    explicit_path = Path(cli_path)
    if explicit_path.exists():
        return str(explicit_path)

    resolved = shutil.which(cli_path)
    if resolved:
        return resolved

    raise UploadError(
        "Databricks CLI was not found. Install it or pass --cli-path with the "
        "full path to databricks.exe."
    )


def build_base_command(cli_executable: str, profile: str | None) -> list[str]:
    command = [cli_executable]
    if profile:
        command.extend(["--profile", profile])
    return command


def run_command(command: list[str], dry_run: bool) -> None:
    printable_command = " ".join(command)
    logging.info("Running: %s", printable_command)

    if dry_run:
        return

    result = subprocess.run(
        command,
        check=False,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()

    if stdout:
        logging.info(stdout)

    if result.returncode != 0:
        raise UploadError(
            f"Command failed with exit code {result.returncode}: "
            f"{printable_command}\n{stderr}"
        )


def upload_files(args: argparse.Namespace) -> None:
    local_dir = args.local_dir
    cli_executable = resolve_cli(args.cli_path)
    target_dir = normalize_volume_path(args.volume_path)
    base_command = build_base_command(cli_executable, args.profile)

    for file_name in EXPECTED_FILES:
        validate_local_file(local_dir / file_name)

    run_command(base_command + ["fs", "mkdir", target_dir], args.dry_run)

    for file_name in EXPECTED_FILES:
        source_path = str((local_dir / file_name).resolve())
        target_path = f"{target_dir}/{file_name}"
        command = base_command + ["fs", "cp", source_path, target_path]

        if not args.no_overwrite:
            command.append("--overwrite")

        run_command(command, args.dry_run)
        if args.dry_run:
            logging.info("Prepared upload from %s to %s", source_path, target_path)
        else:
            logging.info("Uploaded %s to %s", source_path, target_path)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    try:
        upload_files(args)
    except UploadError as exc:
        logging.error("Upload failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())