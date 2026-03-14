"""Run the full data pipeline: scrape → categorize → embed → group."""
from __future__ import annotations
import os
import subprocess
import sys
import time
from pathlib import Path

from backend.data.run_scrapers import find_scraper_scripts

PROJECT_ROOT = str(Path(__file__).resolve().parents[2])

SCRIPTS = [
    "run_scrapers.py",
    "categorize_products.py",
    "embed_products.py",
    "group_products.py",
]

def main() -> int:
    data_dir = Path(__file__).parent.resolve()
    failed = []

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
        if script == "run_scrapers.py" and num_scrapers > 0:
            cmd += ["--parallel", str(num_scrapers)]

        result = subprocess.run(
            cmd,
            cwd=str(data_dir),
            env=env,
        )
        elapsed = round(time.perf_counter() - start, 2)

        if result.returncode != 0:
            print(f"{script} failed (exit code {result.returncode}) after {elapsed}s")
            failed.append(script)
            break
        print(f"{script} completed in {elapsed}s")

    print(f"\n{'='*60}")
    if failed:
        print(f"Pipeline stopped. Failed: {', '.join(failed)}")
        return 1
    print("Pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

