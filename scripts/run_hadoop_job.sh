#!/usr/bin/env bash
# ==============================================================================
# Run Hadoop MapReduce Streaming Job on YARN
# Course: CSE3712 — Big Data Analytics
# Usage: ./scripts/run_hadoop_job.sh [--mode sample|full]
# ==============================================================================
set -e

MODE="sample"
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --mode) MODE="$2"; shift ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

echo "======================================================================"
echo " Running Apache Hadoop Streaming Job (Mode: ${MODE^^})"
echo "======================================================================"

# Check HADOOP_HOME
if [ -z "$HADOOP_HOME" ]; then
    if [ -d "$HOME/hadoop-3.3.6" ]; then
        export HADOOP_HOME="$HOME/hadoop-3.3.6"
        export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
    else
        echo "Error: HADOOP_HOME is not set and $HOME/hadoop-3.3.6 not found."
        echo "Please run ./scripts/setup_hadoop_wsl.sh first."
        exit 1
    fi
fi

# Locate Streaming JAR
STREAMING_JAR=$(find "$HADOOP_HOME" -name "hadoop-streaming*.jar" | head -n 1)
if [ -z "$STREAMING_JAR" ]; then
    echo "Error: hadoop-streaming JAR not found under $HADOOP_HOME."
    exit 1
fi
echo "[1/6] Found Hadoop Streaming JAR: $STREAMING_JAR"

# Paths
HDFS_RAW_DIR="/data/realestate/raw/$MODE"
HDFS_CLEAN_DIR="/data/realestate/clean/$MODE"
LOCAL_OUT_DIR="data/clean/$MODE"
mkdir -p "$LOCAL_OUT_DIR"

# 2. Upload Raw Data to HDFS
echo "[2/6] Preparing and uploading raw data to HDFS at $HDFS_RAW_DIR..."
hdfs dfs -mkdir -p "$HDFS_RAW_DIR"

if [ "$MODE" == "sample" ]; then
    if [ ! -f "data/sample/raw/sample_raw.csv" ]; then
        echo "Generating sample dataset first..."
        python3 scripts/create_sample_data.py
    fi
    hdfs dfs -put -f data/sample/raw/sample_raw.csv "$HDFS_RAW_DIR/"
else
    if [ ! -f "data/raw/pp-2022.csv" ] || [ ! -f "data/raw/pp-2023.csv" ]; then
        echo "Error: pp-2022.csv or pp-2023.csv not found in data/raw/."
        exit 1
    fi
    hdfs dfs -put -f data/raw/pp-2022.csv "$HDFS_RAW_DIR/"
    hdfs dfs -put -f data/raw/pp-2023.csv "$HDFS_RAW_DIR/"
fi

echo "[3/6] Listing raw input files in HDFS:"
hdfs dfs -ls "$HDFS_RAW_DIR"

# 3. Clean previous output directory to make reruns safe
echo "[4/6] Resetting HDFS output directory: $HDFS_CLEAN_DIR..."
hdfs dfs -rm -r -f "$HDFS_CLEAN_DIR" || true

# 4. Submit Hadoop Streaming Job to YARN
echo "[5/6] Submitting MapReduce Streaming Job to YARN cluster..."
START_TIME=$(date +%s)

LOG_FILE="/tmp/hadoop_streaming_${MODE}.log"

hadoop jar "$STREAMING_JAR" \
    -files scripts/mapper.py,scripts/reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "$HDFS_RAW_DIR/*" \
    -output "$HDFS_CLEAN_DIR" \
    2>&1 | tee "$LOG_FILE"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "======================================================================"
echo " Hadoop Streaming Job Finished in ${ELAPSED}s!"
echo "======================================================================"

# 5. Preview Clean Output in HDFS
echo ""
echo "[6/6] Previewing top 5 clean records in HDFS ($HDFS_CLEAN_DIR):"
hdfs dfs -cat "$HDFS_CLEAN_DIR/part-*" | head -n 5

# Export Clean Data to local directory for Dashboard and ML Model
LOCAL_TSV="$LOCAL_OUT_DIR/clean_sales.tsv"
echo ""
echo "Exporting clean records from HDFS to local file: $LOCAL_TSV..."
hdfs dfs -getmerge "$HDFS_CLEAN_DIR" "$LOCAL_TSV"

# Parse Counters from Log
RAW_ROWS=$(grep "DataCleaning" "$LOG_FILE" | grep "raw_rows" | awk -F '=' '{print $2}' | tr -d ' ' || echo "0")
REJ_ROWS=$(grep "DataCleaning" "$LOG_FILE" | grep "rejected_rows" | awk -F '=' '{print $2}' | tr -d ' ' || echo "0")
DUP_ROWS=$(grep "DataCleaning" "$LOG_FILE" | grep "duplicate_rows_removed" | awk -F '=' '{print $2}' | tr -d ' ' || echo "0")
CONF_ROWS=$(grep "DataCleaning" "$LOG_FILE" | grep "conflicting_rows_excluded" | awk -F '=' '{print $2}' | tr -d ' ' || echo "0")
CLEAN_ROWS=$(grep "DataCleaning" "$LOG_FILE" | grep "final_clean_rows" | awk -F '=' '{print $2}' | tr -d ' ' || echo "0")

echo ""
echo "======================================================================"
echo " HADOOP MAPREDUCE DATA CLEANING COUNTER SUMMARY"
echo "======================================================================"
echo " Mode:                      ${MODE^^}"
echo " Total Raw Input Rows:      $RAW_ROWS"
echo " [-] Rejected Rows:         $REJ_ROWS"
echo " [-] Duplicate Rows:        $DUP_ROWS"
echo " [-] Conflicting Rows:      $CONF_ROWS"
echo " [=] Final Clean Rows:      $CLEAN_ROWS"
echo " Output TSV:                $LOCAL_TSV"
echo "======================================================================"
