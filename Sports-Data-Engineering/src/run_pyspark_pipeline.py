
"""
STRIDE PERFORMANCE LAB - MASTER PIPELINE RUNNER

Usage:
    python src/run_pyspark_pipeline.py --drop 1
    python src/run_pyspark_pipeline.py --drop 2
    python src/run_pyspark_pipeline.py --drop 3
"""

import argparse
import sys
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def run_step(name, script, *args):
    print(f"\n{'=' * 60}", flush=True)
    print(f"RUNNING: {name}", flush=True)
    print(f"{'=' * 60}", flush=True)

    subprocess.run(
        [PYTHON, str(ROOT / script), *args],
        cwd=str(ROOT),
        check=True
    )

    print(f"COMPLETED: {name}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--drop",
        type=int,
        choices=[1, 2, 3],
        default=3,
        help="Latest cumulative device drop to process"
    )
    args = parser.parse_args()

    print(
        "\nSTRIDE PERFORMANCE LAB - PYSPARK PIPELINE",
        flush=True
    )

    steps = [
        (
            "1. Bronze Ingestion",
            "src/bronze/ingest_all.py",
            ["--drop", str(args.drop)]
        ),
        (
            "2. Silver Transformations",
            "src/silver/process_silver.py",
            []
        ),
        (
            "3. Athlete Baseline",
            "src/gold/mart_athlete_baseline.py",
            []
        ),
        (
            "4. Gold Business Marts",
            "src/gold/build_marts.py",
            []
        ),
        (
            "5. Data Quality Tests",
            "src/tests/test_pipeline.py",
            []
        )
    ]

    for name, script, step_args in steps:
        try:
            run_step(name, script, *step_args)
        except subprocess.CalledProcessError as error:
            print(f"\nPIPELINE FAILED: {name}", flush=True)
            print(f"Exit code: {error.returncode}", flush=True)
            sys.exit(error.returncode)

    print("\n" + "=" * 60)
    print("ALL PIPELINE STAGES COMPLETED SUCCESSFULLY")
    print("Gold outputs: data/gold_marts/")
    print("=" * 60)


if __name__ == "__main__":
    main()
