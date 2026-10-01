"""
Root offline test runner for Bybit Cutover Final Draft suite.
Executes test suites in isolated SUBPROCESSES with sys.executable
to avoid cached listener / env test cross contamination and count actual assert checks.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VPS_TESTS_DIR = os.path.join(BASE_DIR, "vps-listener", "tests")


def run_test_suite_subprocess(path: str) -> int:
    rel_path = os.path.relpath(path, BASE_DIR)
    print(f"\n==================================================")
    print(f"RUNNING TEST SUITE (SUBPROCESS): {rel_path}")
    print(f"==================================================")

    res = subprocess.run(
        [sys.executable, path],
        capture_output=True,
        text=True,
        cwd=os.path.dirname(path)
    )

    print(res.stdout, end="")
    if res.stderr:
        print(res.stderr, file=sys.stderr, end="")

    if res.returncode != 0:
        print(f"ERROR: Subprocess for {rel_path} exited with code {res.returncode}", file=sys.stderr)
        raise RuntimeError(f"Test suite {rel_path} failed")

    # Parse count of checks passed from output e.g., "ALL 20 RULES CHECKS PASSED" or "ALL 13 CUTOVER CHECKS PASSED"
    matches = re.findall(r"ALL\s+(\d+)\s+.*CHECKS PASSED", res.stdout, re.IGNORECASE)
    if matches:
        passed = int(matches[-1])
    else:
        # Fallback count of "ok - " lines
        passed = len(re.findall(r"^\s*ok\s+-", res.stdout, re.MULTILINE))

    print(f"Completed {rel_path} -> {passed} checks passed.")
    return passed


def main() -> int:
    test_files = [
        os.path.join(VPS_TESTS_DIR, "test_rules.py"),
        os.path.join(VPS_TESTS_DIR, "test_cutover.py"),
        os.path.join(VPS_TESTS_DIR, "test_regressions.py"),
        os.path.join(VPS_TESTS_DIR, "test_profiles.py"),
    ]

    total_passed = 0
    suites_run = 0

    for tf in test_files:
        if not os.path.exists(tf):
            print(f"ERROR: Test file not found: {tf}", file=sys.stderr)
            return 1
        passed = run_test_suite_subprocess(tf)
        total_passed += passed
        suites_run += 1

    print("\n" + "=" * 50)
    print(f"SUMMARY: {suites_run} test suites completed successfully.")
    print(f"TOTAL OFFLINE CHECKS PASSED: {total_passed}")
    print("=" * 50)
    return 0


if __name__ == "__main__":
    sys.exit(main())
