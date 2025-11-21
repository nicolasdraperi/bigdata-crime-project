import os
import subprocess
import sys
import time
from pathlib import Path


def run_cmd(cmd, desc):
    """
    Run a system command, print description, stop pipeline on error.
    """
    print(f"\n=== {desc} ===")
    print("Command:", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"ERROR during: {desc}", file=sys.stderr)
        sys.exit(result.returncode)
    else:
        print(f"OK: {desc}")


def wait_for_hdfs(max_tries=20, delay_seconds=5):
    """
    Wait until HDFS namenode is ready by trying a simple 'hdfs dfs -ls /'.
    """
    for attempt in range(1, max_tries + 1):
        print(f"Checking HDFS (try {attempt}/{max_tries})...")
        result = subprocess.run(
            ["docker", "compose", "exec", "namenode", "hdfs", "dfs", "-ls", "/"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            print("HDFS is ready.")
            return
        time.sleep(delay_seconds)

    print("HDFS is not ready after several attempts.", file=sys.stderr)
    sys.exit(1)


def main():
    # Go to project root (where docker-compose.yml lives)
    project_root = Path(__file__).resolve().parents[2]
    os.chdir(project_root)
    print("Project root:", project_root)

    # 0) Build and start containers
    run_cmd(
        ["docker", "compose", "build"],
        "Build Docker images",
    )

    run_cmd(
        ["docker", "compose", "up", "-d"],
        "Start Docker containers (cluster + dashboard)",
    )

    # 0.1) Wait for HDFS to be ready
    wait_for_hdfs()

    # 1) Download raw data locally (host)
    run_cmd(
        [sys.executable, "src/ingestion/download_crime_data.py"],
        "Download raw crime data (local CSV)",
    )

    # 2) Push raw CSV into the namenode container, then into HDFS (RAW)
    local_raw_path = "data/raw/crime/crime_raw.csv"
    container_tmp_path = "/tmp/crime_raw.csv"
    hdfs_raw_dir = "/datalake/raw/crime"
    hdfs_raw_file = f"{hdfs_raw_dir}/crime_raw.csv"

    # Copy file into container
    run_cmd(
        ["docker", "compose", "cp", local_raw_path, f"namenode:{container_tmp_path}"],
        "Copy raw CSV into namenode container",
    )

    # Create HDFS RAW directory
    run_cmd(
        ["docker", "compose", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_raw_dir],
        "Create RAW directory in HDFS",
    )

    # Put file into HDFS (overwrite if exists)
    run_cmd(
        [
            "docker",
            "compose",
            "exec",
            "namenode",
            "hdfs",
            "dfs",
            "-put",
            "-f",
            container_tmp_path,
            hdfs_raw_file,
        ],
        "Upload raw CSV into HDFS (RAW zone)",
    )

    # 3) Clean data: RAW -> CURATED
    run_cmd(
        [
            "docker",
            "compose",
            "exec",
            "spark-master",
            "/spark/bin/spark-submit",
            "/app/src/processing/clean_crime_data.py",
        ],
        "Clean data (RAW -> CURATED)",
    )

    # 4) Aggregations: CURATED -> ANALYTICS
    run_cmd(
        [
            "docker",
            "compose",
            "exec",
            "spark-master",
            "/spark/bin/spark-submit",
            "/app/src/processing/aggregate_crime_stats.py",
        ],
        "Compute analytics aggregations",
    )

    # 5) High-risk context detection (IA-like)
    run_cmd(
        [
            "docker",
            "compose",
            "exec",
            "spark-master",
            "/spark/bin/spark-submit",
            "/app/src/processing/detect_high_risk_contexts.py",
        ],
        "Detect high-risk contexts",
    )

    print("\nPipeline finished successfully.")
    print("Dashboard available at: http://localhost:8501")


if __name__ == "__main__":
    main()
