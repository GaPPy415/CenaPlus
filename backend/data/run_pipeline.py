"""Run the full data pipeline: scrape → categorize → extract features → group."""
from __future__ import annotations
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.data.run_scrapers import PARTIAL_FAILURE_EXIT_CODE, find_scraper_scripts

SCRIPTS = [
    "run_scrapers.py",
    "categorize_products.py",
    "extract_features.py",
    "group_products.py",
]

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the full CenaPlus data pipeline.")
    parser.add_argument(
        "--force-scrape",
        action="store_true",
        help="Ignore per-market source freshness markers and scrape every market",
    )
    args = parser.parse_args(argv)

    data_dir = Path(__file__).parent.resolve()
    failed = []
    warnings = []

    num_scrapers = len(find_scraper_scripts(data_dir / "scrapers"))

    for script in SCRIPTS:
        path = data_dir / script
        if not path.exists():
            print(f"Script not found: {path}")
            failed.append(script)
            continue

        print(f"\n{'='*60}")
        print(f"Running {script}...")
        print(f"{'='*60}")
        start = time.perf_counter()
        env = os.environ.copy()
        env["PYTHONPATH"] = PROJECT_ROOT

        cmd = [sys.executable, str(path)]
        if script == "run_scrapers.py":
            if num_scrapers > 0:
                cmd += ["--parallel", str(num_scrapers)]
            cmd += ["--no-refresh"]
            if args.force_scrape:
                cmd += ["--force"]
        if script == "group_products.py":
            cmd += ["--apply"]

        result = subprocess.run(
            cmd,
            cwd=str(data_dir),
            env=env,
        )
        elapsed = round(time.perf_counter() - start, 2)

        if script == "run_scrapers.py" and result.returncode == PARTIAL_FAILURE_EXIT_CODE:
            print(
                f"{script} finished with partial scraper failures after {elapsed}s "
                f"— continuing pipeline with the markets that succeeded."
            )
            warnings.append(script)
            continue

        if result.returncode != 0:
            print(f"{script} failed (exit code {result.returncode}) after {elapsed}s")
            failed.append(script)
            break
        print(f"{script} completed in {elapsed}s")

    print(f"\n{'='*60}")
    if failed:
        print(f"Pipeline stopped. Failed: {', '.join(failed)}")
        return 1
    if warnings:
        print(f"Pipeline completed with partial warnings: {', '.join(warnings)}")
        return 0
    print("Pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

