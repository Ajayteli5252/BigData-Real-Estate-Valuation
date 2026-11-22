"""
Local Execution Runner for Hadoop Streaming Pipeline
CSE3712 — Big Data Analytics
Simulates the exact Hadoop Streaming MapReduce lifecycle locally:
Input -> mapper.py -> Key-based Sort -> reducer.py -> Output TSV + Counters
Supports both Sample Mode (classroom demo) and Full Mode.
Reconciles all Hadoop counters:
raw_rows = rejected_rows + duplicate_rows_removed + conflicting_rows_excluded + final_clean_rows
"""
import os
import sys
import time
import json
import subprocess
import argparse
import tempfile

def run_pipeline(mode="sample"):
    start_time = time.time()
    print("=" * 70)
    print(f"Hadoop MapReduce Streaming Pipeline — Mode: {mode.upper()}")
    print("=" * 70)

    # Determine paths
    if mode == "sample":
        input_file = os.path.join("data", "sample", "raw", "sample_raw.csv")
        output_dir = os.path.join("data", "clean", "sample")
        if not os.path.exists(input_file):
            print(f"[PREP] Sample raw file not found. Generating now...")
            subprocess.run([sys.executable, os.path.join("scripts", "create_sample_data.py")], check=True)
        input_paths = [input_file]
    else:
        input_paths = [
            os.path.join("data", "raw", "pp-2022.csv"),
            os.path.join("data", "raw", "pp-2023.csv")
        ]
        output_dir = os.path.join("data", "clean", "full")
        for p in input_paths:
            if not os.path.exists(p):
                print(f"Error: Missing raw file {p}. Run scripts/01_ingest_data.py.")
                sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    output_tsv = os.path.join(output_dir, "clean_sales.tsv")
    summary_json = os.path.join(output_dir, "cleaning_summary.json")

    mapper_script = os.path.join("scripts", "mapper.py")
    reducer_script = os.path.join("scripts", "reducer.py")

    # Temp files for MapReduce stage isolation
    mapper_out_path = os.path.join(output_dir, "_temp_mapper_out.tsv")
    sorted_path = os.path.join(output_dir, "_temp_sorted.tsv")

    print(f"[STAGE 1] Running Hadoop Streaming Mapper: {mapper_script}")
    print(f"          Reading: {', '.join(input_paths)}")
    t_map = time.time()

    mapper_counters = {}
    with open(mapper_out_path, "w", encoding="utf-8") as map_out:
        # Run mapper sequentially over input files and append output
        for p in input_paths:
            with open(p, "r", encoding="utf-8", errors="ignore") as in_f:
                proc = subprocess.Popen(
                    [sys.executable, mapper_script],
                    stdin=in_f,
                    stdout=map_out,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8"
                )
                _, stderr_out = proc.communicate()
                for line in stderr_out.splitlines():
                    if line.startswith("reporter:counter:DataCleaning,"):
                        parts = line.strip().split(",")
                        c_name = parts[1]
                        c_val = int(parts[2])
                        mapper_counters[c_name] = mapper_counters.get(c_name, 0) + c_val

    print(f"          Mapper completed in {time.time() - t_map:.2f}s.")
    print(f"          Raw rows processed: {mapper_counters.get('raw_rows', 0):,}")
    print(f"          Valid rows emitted: {mapper_counters.get('mapper_emitted_rows', 0):,}")
    print(f"          Rejected rows:      {mapper_counters.get('rejected_rows', 0):,}")

    # 2. MapReduce Shuffle & Sort Phase (Sort by transaction_id)
    print("\n[STAGE 2] Hadoop MapReduce Shuffle & Sort Phase...")
    t_sort = time.time()
    with open(mapper_out_path, "r", encoding="utf-8") as f_in:
        lines = f_in.readlines()
    print(f"          Sorting {len(lines):,} mapped records by transaction_id...")
    lines.sort(key=lambda x: x.split("\t")[0] if "\t" in x else "")
    with open(sorted_path, "w", encoding="utf-8") as f_out:
        f_out.writelines(lines)
    print(f"          Shuffle & Sort completed in {time.time() - t_sort:.2f}s.")

    # 3. Run Reducer
    print(f"\n[STAGE 3] Running Hadoop Streaming Reducer: {reducer_script}")
    t_red = time.time()
    reducer_counters = {}
    with open(sorted_path, "r", encoding="utf-8") as sorted_f, open(output_tsv, "w", encoding="utf-8") as out_f:
        proc = subprocess.Popen(
            [sys.executable, reducer_script],
            stdin=sorted_f,
            stdout=out_f,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8"
        )
        _, stderr_out = proc.communicate()
        for line in stderr_out.splitlines():
            if line.startswith("reporter:counter:DataCleaning,"):
                parts = line.strip().split(",")
                c_name = parts[1]
                c_val = int(parts[2])
                reducer_counters[c_name] = reducer_counters.get(c_name, 0) + c_val

    print(f"          Reducer completed in {time.time() - t_red:.2f}s.")

    # Cleanup temp intermediate files
    if os.path.exists(mapper_out_path):
        os.remove(mapper_out_path)
    if os.path.exists(sorted_path):
        os.remove(sorted_path)

    elapsed_time = time.time() - start_time

    # Consolidate Accounting
    raw_rows = mapper_counters.get("raw_rows", 0)
    rejected_rows = mapper_counters.get("rejected_rows", 0)
    duplicate_rows_removed = reducer_counters.get("duplicate_rows_removed_total", 0)
    conflicting_rows_excluded = reducer_counters.get("conflicting_rows_excluded_total", 0)
    final_clean_rows = reducer_counters.get("final_clean_rows_total", 0)

    # Mathematical Reconciliation Check
    reconciled_sum = rejected_rows + duplicate_rows_removed + conflicting_rows_excluded + final_clean_rows
    reconciled = (raw_rows == reconciled_sum)

    summary = {
        "mode": mode,
        "execution_engine": "Hadoop Streaming (Local Python Simulation)",
        "elapsed_seconds": round(elapsed_time, 2),
        "counters": {
            "raw_rows": raw_rows,
            "rejected_rows": rejected_rows,
            "duplicate_rows_removed": duplicate_rows_removed,
            "conflicting_rows_excluded": conflicting_rows_excluded,
            "final_clean_rows": final_clean_rows
        },
        "reconciliation": {
            "formula": "raw_rows = rejected_rows + duplicate_rows_removed + conflicting_rows_excluded + final_clean_rows",
            "is_balanced": reconciled,
            "reconciled_sum": reconciled_sum
        },
        "rejection_breakdown": {
            "missing_required_field": mapper_counters.get("rejected_missing_field", 0),
            "invalid_price": mapper_counters.get("rejected_invalid_price", 0),
            "out_of_range_price": mapper_counters.get("rejected_out_of_range_price", 0),
            "invalid_date": mapper_counters.get("rejected_invalid_date", 0),
            "unsupported_category": mapper_counters.get("rejected_unsupported_category", 0)
        },
        "output_file": output_tsv
    }

    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # Print Report
    print("\n" + "=" * 70)
    print("HADOOP MAPREDUCE DATA CLEANING RECONCILIATION AUDIT")
    print("=" * 70)
    print(f"  Mode:                         {mode.upper()}")
    print(f"  Total Raw Input Rows:         {raw_rows:,}")
    print(f"  [-] Rejected Rows (Mapper):   {rejected_rows:,}")
    print(f"      - Missing fields:         {mapper_counters.get('rejected_missing_field', 0):,}")
    print(f"      - Invalid prices:         {mapper_counters.get('rejected_invalid_price', 0):,}")
    print(f"      - Out-of-range prices:    {mapper_counters.get('rejected_out_of_range_price', 0):,}")
    print(f"      - Invalid dates:          {mapper_counters.get('rejected_invalid_date', 0):,}")
    print(f"      - Unsupported categories: {mapper_counters.get('rejected_unsupported_category', 0):,}")
    print(f"  [-] Duplicate Rows Removed:   {duplicate_rows_removed:,}")
    print(f"  [-] Conflicting Rows Excluded:{conflicting_rows_excluded:,}")
    print(f"  [=] Final Clean Output Rows:  {final_clean_rows:,}")
    print("-" * 70)
    print(f"  Accounting Balance Check:     {'PASSED (EXACT MATCH)' if reconciled else 'FAILED'}")
    print(f"  Calculation: {raw_rows:,} == {rejected_rows:,} + {duplicate_rows_removed:,} + {conflicting_rows_excluded:,} + {final_clean_rows:,}")
    print(f"  Output TSV:                   {output_tsv}")
    print(f"  Summary JSON:                 {summary_json}")
    print(f"  Total Job Latency:            {elapsed_time:.2f}s")
    print("=" * 70)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Hadoop Streaming Cleaning Pipeline")
    parser.add_argument("--mode", choices=["sample", "full"], default="sample", help="Run mode: sample (demo) or full")
    args = parser.parse_args()
    run_pipeline(mode=args.mode)
