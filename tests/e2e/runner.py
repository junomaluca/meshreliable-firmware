#!/usr/bin/env python3
"""
E2E Autonomous Test Runner

Continuously runs the E2E test suite and reports results.
Designed to run unattended during development for regression detection.

Usage:
    python3 tests/e2e/runner.py                # continuous mode
    python3 tests/e2e/runner.py --once         # single run
    python3 tests/e2e/runner.py --interval 60  # custom interval (seconds)
"""

import os
import sys
import time
import json
import subprocess
import argparse
from datetime import datetime
from pathlib import Path


def run_tests(test_dir: str, verbose: bool = False) -> dict:
    """Run pytest and return structured results."""
    report_file = f"/tmp/e2e_results_{int(time.time())}.json"
    cmd = [
        sys.executable, "-m", "pytest",
        test_dir,
        f"--json-report-file={report_file}",
        "--json-report",
        "-v" if verbose else "-q",
        "--tb=short",
        "-x",  # Stop on first failure for faster feedback
        "-m", "not slow",  # Skip slow tests in continuous mode
    ]

    print(f"\n{'='*60}")
    print(f"  E2E Test Run — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    result = subprocess.run(
        cmd,
        capture_output=not verbose,
        text=True,
        timeout=600,  # 10 minute max per run
    )

    # Parse results
    summary = {
        "timestamp": datetime.now().isoformat(),
        "returncode": result.returncode,
        "passed": 0,
        "failed": 0,
        "skipped": 0,
        "errors": 0,
        "failures": [],
    }

    if os.path.exists(report_file):
        try:
            with open(report_file) as f:
                report = json.load(f)
            summary_data = report.get("summary", {})
            summary["passed"] = summary_data.get("passed", 0)
            summary["failed"] = summary_data.get("failed", 0)
            summary["skipped"] = summary_data.get("skipped", 0)
            summary["errors"] = summary_data.get("error", 0)

            # Extract failure details
            for test in report.get("tests", []):
                if test.get("outcome") == "failed":
                    summary["failures"].append({
                        "name": test.get("nodeid", "unknown"),
                        "message": test.get("call", {}).get("crash", {}).get("message", ""),
                    })
        except (json.JSONDecodeError, KeyError):
            pass
        finally:
            os.unlink(report_file)

    if not verbose and result.stdout:
        print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)

    return summary


def print_summary(summary: dict):
    """Print a concise summary of test results."""
    total = summary["passed"] + summary["failed"] + summary["skipped"]
    status = "PASS" if summary["failed"] == 0 and summary["errors"] == 0 else "FAIL"

    print(f"\n  Result: {status}")
    print(f"  Passed: {summary['passed']}/{total}  "
          f"Failed: {summary['failed']}  "
          f"Skipped: {summary['skipped']}")

    if summary["failures"]:
        print(f"\n  Failures:")
        for f in summary["failures"]:
            print(f"    - {f['name']}")
            if f["message"]:
                print(f"      {f['message'][:100]}")

    print()


def save_results(summary: dict, results_file: str):
    """Append results to a JSON-lines log file."""
    with open(results_file, "a") as f:
        f.write(json.dumps(summary) + "\n")


def main():
    parser = argparse.ArgumentParser(description="E2E Autonomous Test Runner")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--interval", type=int, default=300, help="Seconds between runs (default: 300)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    test_dir = os.path.dirname(os.path.abspath(__file__))
    results_file = os.path.join(test_dir, "run_history.jsonl")

    print("MeshReliable E2E Test Runner")
    print(f"Test directory: {test_dir}")
    print(f"Results log: {results_file}")

    if args.once:
        summary = run_tests(test_dir, verbose=args.verbose)
        print_summary(summary)
        save_results(summary, results_file)
        sys.exit(0 if summary["failed"] == 0 else 1)

    # Continuous mode
    print(f"Running continuously (interval: {args.interval}s)")
    print("Press Ctrl+C to stop\n")

    consecutive_passes = 0
    try:
        while True:
            summary = run_tests(test_dir, verbose=args.verbose)
            print_summary(summary)
            save_results(summary, results_file)

            if summary["failed"] == 0 and summary["errors"] == 0:
                consecutive_passes += 1
                print(f"  Consecutive passes: {consecutive_passes}")
                print(f"  Next run in {args.interval}s...")
                time.sleep(args.interval)
            else:
                consecutive_passes = 0
                # On failure, wait shorter to allow quick re-test after fix
                wait = min(args.interval, 60)
                print(f"  Failures detected. Re-running in {wait}s...")
                time.sleep(wait)

    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
