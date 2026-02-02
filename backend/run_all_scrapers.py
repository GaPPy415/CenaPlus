from __future__ import annotations
import argparse
import json
import sys
import subprocess
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any


def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def find_scraper_scripts(scraper_dir: Path) -> List[Path]:
    # Match files ending with _scraper.py; skip helpers like *_funcs.py and __init__.py
    scripts = [
        p for p in scraper_dir.glob("*_scraper.py")
        if p.name.lower() != "__init__.py"
        and not p.name.lower().endswith("_funcs.py")
    ]
    # Some scrapers may not have the exact suffix pattern; include Kam_scraper.py specifically
    kam = scraper_dir / "Kam_scraper.py"
    if kam.exists() and kam not in scripts:
        scripts.append(kam)
    # Sort for stable order
    scripts.sort(key=lambda p: p.name.lower())
    return scripts


def run_script(script_path: Path, logs_dir: Path, stream: bool = True) -> Dict[str, Any]:
    start = time.perf_counter()
    log_path = logs_dir / f"{script_path.stem}-{timestamp()}.log"
    logs_dir.mkdir(parents=True, exist_ok=True)

    result: Dict[str, Any] = {
        "script": str(script_path),
        "name": script_path.name,
        "start": datetime.now().isoformat(timespec="seconds"),
        "end": None,
        "duration_seconds": None,
        "exit_code": None,
        "status": None,
        "log_file": str(log_path),
    }

    with open(log_path, "w", encoding="utf-8", errors="replace") as log:
        header = (
            f"=== Running {script_path.name} ===\n"
            f"Started: {result['start']}\n"
            f"CWD: {script_path.parent}\n"
            f"Python: {sys.executable}\n"
            f"===============================\n"
        )
        log.write(header)
        log.flush()

        try:
            # Run with cwd at the script's folder to satisfy relative imports/paths
            proc = subprocess.Popen(
                [sys.executable, str(script_path)],
                cwd=str(script_path.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                log.write(line)
                if stream:
                    print(f"[{script_path.name}] {line}", end="")
            proc.wait()
            exit_code = proc.returncode
        except Exception as e:
            exit_code = 1
            err = f"Exception while running {script_path.name}: {e}\n"
            log.write(err)
            if stream:
                print(f"[{script_path.name}] {err}", end="")

    end = time.perf_counter()
    duration = round(end - start, 2)

    result["end"] = datetime.now().isoformat(timespec="seconds")
    result["duration_seconds"] = duration
    result["exit_code"] = exit_code
    result["status"] = "passed" if exit_code == 0 else "failed"
    return result


def run_parallel(scripts: List[Path], logs_dir: Path, max_workers: int) -> List[Dict[str, Any]]:
    # Simpler parallel execution: capture to logs only; print minimal status to console
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results: List[Dict[str, Any]] = []
    print(f"Running {len(scripts)} scripts in parallel with {max_workers} workers...")

    def _runner(p: Path) -> Dict[str, Any]:
        return run_script(p, logs_dir, stream=False)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_to_script = {ex.submit(_runner, p): p for p in scripts}
        for fut in as_completed(fut_to_script):
            p = fut_to_script[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {
                    "script": str(p),
                    "name": p.name,
                    "start": None,
                    "end": None,
                    "duration_seconds": None,
                    "exit_code": 1,
                    "status": "failed",
                    "error": str(e),
                    "log_file": str((logs_dir / f"{p.stem}-{timestamp()}.log")),
                }
            results.append(res)
            print(f"Finished {p.name}: {res['status']} in {res.get('duration_seconds')}s")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Run all scraper scripts with logging and summary.")
    parser.add_argument("--scraper-dir", default=str(Path(__file__).parent / "scraper"), help="Path to scraper folder")
    parser.add_argument("--logs-dir", default=str(Path(__file__).parent / "logs"), help="Where to store logs")
    parser.add_argument("--parallel", type=int, default=0, help="Run in parallel with N workers (0 = sequential)")
    parser.add_argument("--dry-run", action="store_true", help="List scripts without executing")
    args = parser.parse_args()

    scraper_dir = Path(args.scraper_dir).resolve()
    logs_dir = Path(args.logs_dir).resolve()

    if not scraper_dir.exists():
        print(f"Scraper directory not found: {scraper_dir}")
        return 2

    scripts = find_scraper_scripts(scraper_dir)
    if not scripts:
        print("No scraper scripts found.")
        return 1

    print("Discovered scripts (execution order):")
    for p in scripts:
        print(f" - {p.name}")

    if args.dry_run:
        print("Dry-run mode: not executing any scripts.")
        return 0

    summary: List[Dict[str, Any]] = []

    if args.parallel and args.parallel > 1:
        summary = run_parallel(scripts, logs_dir, max_workers=args.parallel)
    else:
        print("Running sequentially...")
        for p in scripts:
            res = run_script(p, logs_dir, stream=True)
            summary.append(res)
            print(f"--> {p.name}: {res['status']} in {res['duration_seconds']}s (log: {res['log_file']})")

    # Write summary JSON
    run_id = timestamp()
    summary_path = logs_dir / f"run_summary-{run_id}.json"
    logs_dir.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # Console summary
    passed = sum(1 for r in summary if r["status"] == "passed")
    failed = len(summary) - passed
    total_time = round(sum((r.get("duration_seconds") or 0) for r in summary), 2)

    print("\n=== Summary ===")
    for r in summary:
        print(f"{r['name']:<25} {r['status']:<6} {r['duration_seconds']!s:>6}s  log: {r['log_file']}")
    print(f"Total: {len(summary)}, Passed: {passed}, Failed: {failed}, Accumulated time: {total_time}s")
    print(f"Summary file: {summary_path}")

    # Return non-zero if any failed
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
