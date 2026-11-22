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

# Check HADOOP_HOME and Java
if [ -z "$JAVA_HOME" ]; then
    JAVA_BIN=$(readlink -f /usr/bin/java 2>/dev/null || true)
    if [ -n "$JAVA_BIN" ]; then
        export JAVA_HOME=$(dirname $(dirname "$JAVA_BIN"))
    else
        export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
    fi
fi

if [ -z "$HADOOP_HOME" ]; then
    if [ -d "$HOME/hadoop-3.3.6" ]; then
        export HADOOP_HOME="$HOME/hadoop-3.3.6"
    else
        echo "Error: HADOOP_HOME is not set and $HOME/hadoop-3.3.6 not found."
        echo "Please run ./scripts/setup_hadoop_wsl.sh first."
        exit 1
    fi
fi
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH"

# Ensure cluster daemons are active
if ! jps | grep -q "NameNode"; then
    echo "Starting HDFS daemons..."
    "$HADOOP_HOME/bin/hdfs" --daemon start namenode
    "$HADOOP_HOME/bin/hdfs" --daemon start datanode
    "$HADOOP_HOME/bin/hdfs" --daemon start secondarynamenode
    sleep 3
fi

if ! jps | grep -q "ResourceManager"; then
    echo "Starting YARN daemons..."
    "$HADOOP_HOME/bin/yarn" --daemon start resourcemanager
    "$HADOOP_HOME/bin/yarn" --daemon start nodemanager
    sleep 3
fi

# Ensure safe mode is off
hdfs dfsadmin -safemode leave 2>/dev/null || true

# Locate Streaming JAR
STREAMING_JAR=$(find "$HADOOP_HOME/share/hadoop/tools/lib" -name "hadoop-streaming-*.jar" | head -n 1)
if [ -z "$STREAMING_JAR" ] || [ ! -f "$STREAMING_JAR" ]; then
    STREAMING_JAR=$(find "$HADOOP_HOME" -path "*/tools/lib/*" -name "hadoop-streaming*.jar" | head -n 1)
fi
if [ -z "$STREAMING_JAR" ]; then
    echo "Error: hadoop-streaming JAR not found under $HADOOP_HOME/share/hadoop/tools/lib."
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

# Extract YARN Application & Job Details
APP_ID=$(grep -oE "application_[0-9]+_[0-9]+" "$LOG_FILE" | head -n 1 || echo "N/A")
JOB_ID=$(grep -oE "job_[0-9]+_[0-9]+" "$LOG_FILE" | head -n 1 || echo "N/A")
if grep -q "completed successfully" "$LOG_FILE" || grep -q "Job .* completed successfully" "$LOG_FILE"; then
    JOB_STATUS="SUCCEEDED"
else
    JOB_STATUS="FINISHED"
fi

# 5. Preview Clean Output in HDFS
echo ""
echo "[6/6] Previewing top 5 clean records in HDFS ($HDFS_CLEAN_DIR):"
hdfs dfs -cat "$HDFS_CLEAN_DIR/part-*" 2>/dev/null | head -n 5 || true

# Export Clean Data to local directory for Dashboard and ML Model
LOCAL_TSV="$LOCAL_OUT_DIR/clean_sales.tsv"
echo ""
echo "Exporting clean records from HDFS to local file: $LOCAL_TSV..."
hdfs dfs -cat "$HDFS_CLEAN_DIR"/part-* > "$LOCAL_TSV"

# Parse Counters from Log
RAW_ROWS=$(grep -E '^\s*raw_rows=' "$LOG_FILE" | awk -F '=' '{print $2}' | tr -d ' ' | head -n 1 || echo "0")
REJ_ROWS=$(grep -E '^\s*rejected_rows=' "$LOG_FILE" | awk -F '=' '{print $2}' | tr -d ' ' | head -n 1 || echo "0")
DUP_ROWS=$(grep -E '^\s*duplicate_rows_removed=' "$LOG_FILE" | awk -F '=' '{print $2}' | tr -d ' ' | head -n 1 || echo "0")
CONF_ROWS=$(grep -E '^\s*conflicting_rows_excluded=' "$LOG_FILE" | awk -F '=' '{print $2}' | tr -d ' ' | head -n 1 || echo "0")
CLEAN_ROWS=$(grep -E '^\s*final_clean_rows=' "$LOG_FILE" | awk -F '=' '{print $2}' | tr -d ' ' | head -n 1 || echo "0")

REJ_MISSING=$(grep -E '^\s*rejected_missing_field=' "$LOG_FILE" | awk -F '=' '{print $2}' | tr -d ' ' | head -n 1 || echo "0")
REJ_PRICE=$(grep -E '^\s*rejected_invalid_price=' "$LOG_FILE" | awk -F '=' '{print $2}' | tr -d ' ' | head -n 1 || echo "0")
REJ_RANGE=$(grep -E '^\s*rejected_out_of_range_price=' "$LOG_FILE" | awk -F '=' '{print $2}' | tr -d ' ' | head -n 1 || echo "0")
REJ_DATE=$(grep -E '^\s*rejected_invalid_date=' "$LOG_FILE" | awk -F '=' '{print $2}' | tr -d ' ' | head -n 1 || echo "0")
REJ_CAT=$(grep -E '^\s*rejected_unsupported_category=' "$LOG_FILE" | awk -F '=' '{print $2}' | tr -d ' ' | head -n 1 || echo "0")

# If grep didn't find them via standard text, fallback to counting lines in local TSV
if [ -z "$CLEAN_ROWS" ] || [ "$CLEAN_ROWS" -eq 0 ]; then
    CLEAN_ROWS=$(wc -l < "$LOCAL_TSV")
fi

echo ""
echo "======================================================================"
echo " HADOOP YARN JOB SUMMARY & DATA CLEANING AUDIT"
echo "======================================================================"
echo " Mode:                      ${MODE^^}"
echo " YARN Application ID:       $APP_ID"
echo " MapReduce Job ID:          $JOB_ID"
echo " Final Job Status:          $JOB_STATUS"
echo " HDFS Input Path:           $HDFS_RAW_DIR"
echo " HDFS Output Path:          $HDFS_CLEAN_DIR"
echo " Total Raw Input Rows:      $RAW_ROWS"
echo " [-] Rejected Rows:         $REJ_ROWS"
echo "     - Missing fields:      ${REJ_MISSING:-0}"
echo "     - Invalid prices:      ${REJ_PRICE:-0}"
echo "     - Out-of-range prices: ${REJ_RANGE:-0}"
echo "     - Invalid dates:       ${REJ_DATE:-0}"
echo "     - Unsupported cat:     ${REJ_CAT:-0}"
echo " [-] Duplicate Rows:        $DUP_ROWS"
echo " [-] Conflicting Rows:      $CONF_ROWS"
echo " [=] Final Clean Rows:      $CLEAN_ROWS"
echo " Output TSV:                $LOCAL_TSV"
echo "======================================================================"

# Write Audit Summary JSON for Streamlit Dashboard
python3 -c "
import json
data = {
    'mode': '${MODE}',
    'execution_engine': 'Apache Hadoop Streaming on YARN',
    'application_id': '${APP_ID}',
    'job_id': '${JOB_ID}',
    'job_status': '${JOB_STATUS}',
    'elapsed_seconds': ${ELAPSED},
    'hdfs_input_dir': '${HDFS_RAW_DIR}',
    'hdfs_output_dir': '${HDFS_CLEAN_DIR}',
    'counters': {
        'raw_rows': int('${RAW_ROWS}' or 0),
        'rejected_rows': int('${REJ_ROWS}' or 0),
        'duplicate_rows_removed': int('${DUP_ROWS}' or 0),
        'conflicting_rows_excluded': int('${CONF_ROWS}' or 0),
        'final_clean_rows': int('${CLEAN_ROWS}' or 0)
    },
    'reconciliation': {
        'formula': 'raw_rows = rejected_rows + duplicate_rows_removed + conflicting_rows_excluded + final_clean_rows',
        'is_balanced': int('${RAW_ROWS}' or 0) == (int('${REJ_ROWS}' or 0) + int('${DUP_ROWS}' or 0) + int('${CONF_ROWS}' or 0) + int('${CLEAN_ROWS}' or 0)),
        'reconciled_sum': (int('${REJ_ROWS}' or 0) + int('${DUP_ROWS}' or 0) + int('${CONF_ROWS}' or 0) + int('${CLEAN_ROWS}' or 0))
    },
    'rejection_breakdown': {
        'missing_required_field': int('${REJ_MISSING}' or 0),
        'invalid_price': int('${REJ_PRICE}' or 0),
        'out_of_range_price': int('${REJ_RANGE}' or 0),
        'invalid_date': int('${REJ_DATE}' or 0),
        'unsupported_category': int('${REJ_CAT}' or 0)
    },
    'output_file': '${LOCAL_TSV}'
}
with open('${LOCAL_OUT_DIR}/cleaning_summary.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2)
print('Audit summary written to: ${LOCAL_OUT_DIR}/cleaning_summary.json')
"
