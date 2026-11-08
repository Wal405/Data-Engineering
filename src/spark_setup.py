"""
================================================================================
PYSPARK SESSION HELPER (Beginner-Friendly)
================================================================================
Configures the local SparkSession for Windows with Java 17 and local Hadoop winutils.
================================================================================
"""

import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

def init_spark(app_name="StridePerformanceLab"):
    """Creates a local beginner-friendly PySpark SparkSession."""
    # Detect Java 17 from Eclipse Adoptium or Microsoft or environment
    adoptium_dir = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
    ms_dir = r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"

    if Path(adoptium_dir).exists():
        os.environ["JAVA_HOME"] = adoptium_dir
    elif Path(ms_dir).exists():
        os.environ["JAVA_HOME"] = ms_dir

    if "JAVA_HOME" in os.environ:
        java_bin = str(Path(os.environ["JAVA_HOME"]) / "bin")
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = java_bin + ";" + os.environ.get("PATH", "")

    # Local Hadoop winutils for Windows
    hadoop_dir = str(BASE_DIR / "hadoop")
    if Path(hadoop_dir).exists():
        os.environ["HADOOP_HOME"] = hadoop_dir
        hadoop_bin = str(Path(hadoop_dir) / "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + ";" + os.environ.get("PATH", "")

    # Point PySpark worker processes to current Python interpreter
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "Asia/Karachi")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    # Only show ERROR messages in console so output is clean and easy to read
    spark.sparkContext.setLogLevel("ERROR")
    return spark
