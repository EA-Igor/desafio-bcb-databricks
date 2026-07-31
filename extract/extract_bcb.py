"""Extract raw SGS series from Banco Central do Brasil.

This script is intended to run locally, outside Databricks. It downloads the
raw JSON payloads required by the challenge and writes them to disk so they can
be uploaded to a Unity Catalog Volume.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BCB_SERIES = {
    "selic": (
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
        "?formato=json&dataInicial=01/01/2020&dataFinal=31/12/2024"
    ),
    "ipca": (
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"
        "?formato=json&dataInicial=01/01/2020&dataFinal=31/12/2024"
    ),
}


class ExtractionError(Exception):
    """Raised when a series cannot be extracted with a valid payload."""


@dataclass(frozen=True)
class RetryConfig:
    """Configuration for retrying transient extraction failures."""

    max_attempts: int
    initial_backoff_seconds: float
    timeout_seconds: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download raw SELIC and IPCA JSON files from BCB SGS."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory where selic.json and ipca.json will be written.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=5,
        help="Maximum number of attempts per series.",
    )
    parser.add_argument(
        "--initial-backoff-seconds",
        type=float,
        default=1.0,
        help="Initial wait before retrying. Doubles after each failed attempt.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="HTTP timeout for each request.",
    )
    return parser.parse_args()


def fetch_json(url: str, retry_config: RetryConfig) -> list[dict[str, Any]]:
    """Fetch and validate a non-empty JSON list from an HTTP endpoint."""

    last_error: Exception | None = None

    for attempt in range(1, retry_config.max_attempts + 1):
        try:
            request = Request(url, headers={"User-Agent": "bcb-sgs-extractor/1.0"})

            with urlopen(request, timeout=retry_config.timeout_seconds) as response:
                status_code = response.getcode()
                raw_body = response.read().decode("utf-8")

            if status_code != 200:
                raise ExtractionError(f"unexpected HTTP status {status_code}")

            if not raw_body.strip():
                raise ExtractionError("empty response body")

            payload = json.loads(raw_body)
            validate_payload(payload)
            return payload

        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError,
                ExtractionError) as exc:
            last_error = exc
            if attempt == retry_config.max_attempts:
                break

            wait_seconds = retry_config.initial_backoff_seconds * (2 ** (attempt - 1))
            logging.warning(
                "Attempt %s/%s failed. Retrying in %.1fs. Error: %s",
                attempt,
                retry_config.max_attempts,
                wait_seconds,
                exc,
            )
            time.sleep(wait_seconds)

    raise ExtractionError(
        f"API did not return a valid payload after "
        f"{retry_config.max_attempts} attempts: {last_error}"
    )


def validate_payload(payload: Any) -> None:
    """Validate the raw SGS payload shape without changing its values."""

    if not isinstance(payload, list):
        raise ExtractionError("payload is not a JSON list")

    if not payload:
        raise ExtractionError("payload is an empty list")

    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ExtractionError(f"item {index} is not an object")

        missing_fields = {"data", "valor"} - set(item)
        if missing_fields:
            raise ExtractionError(
                f"item {index} is missing fields: {sorted(missing_fields)}"
            )


def write_json_atomic(payload: list[dict[str, Any]], output_path: Path) -> None:
    """Write a JSON file atomically to avoid leaving partial files behind."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=output_path.parent,
        delete=False,
        suffix=".tmp",
    ) as temp_file:
        json.dump(payload, temp_file, ensure_ascii=False, indent=2)
        temp_file.write("\n")
        temp_path = Path(temp_file.name)

    temp_path.replace(output_path)


def run_extraction(output_dir: Path, retry_config: RetryConfig) -> None:
    for series_name, url in BCB_SERIES.items():
        output_path = output_dir / f"{series_name}.json"
        logging.info("Extracting %s from %s", series_name.upper(), url)

        payload = fetch_json(url, retry_config)
        write_json_atomic(payload, output_path)

        logging.info(
            "Saved %s records to %s",
            len(payload),
            output_path.resolve(),
        )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args = parse_args()

    if args.max_attempts < 1:
        logging.error("--max-attempts must be greater than zero")
        return 2

    if args.initial_backoff_seconds <= 0:
        logging.error("--initial-backoff-seconds must be greater than zero")
        return 2

    if args.timeout_seconds <= 0:
        logging.error("--timeout-seconds must be greater than zero")
        return 2

    retry_config = RetryConfig(
        max_attempts=args.max_attempts,
        initial_backoff_seconds=args.initial_backoff_seconds,
        timeout_seconds=args.timeout_seconds,
    )

    try:
        run_extraction(args.output_dir, retry_config)
    except ExtractionError as exc:
        logging.error("Extraction failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
