#!/usr/bin/env python3
"""
Hadoop Streaming Reducer for UK Real Estate Deduplication & Conflict Filtering
CSE3712 — Big Data Analytics
Reads sorted key-value pairs from sys.stdin grouped by transaction_id.
Handles duplicates (identical records -> keep 1, count extra as duplicate_rows_removed)
and conflicts (different records for same ID -> exclude all, count as conflicting_rows_excluded).
Emits clean records and tracks Hadoop counters.
"""
import sys

def process_group(curr_id, records, counters):
    n = len(records)
    if n == 0:
        return

    # Check for identical vs conflicting records
    unique_records = list(set(records))

    if len(unique_records) == 1:
        # Identical records: keep 1, count extra as duplicates removed
        chosen = unique_records[0]
        if n > 1:
            counters["duplicate_rows_removed"] += (n - 1)
            sys.stderr.write(f"reporter:counter:DataCleaning,duplicate_rows_removed,{n - 1}\n")
        counters["final_clean_rows"] += 1
        sys.stderr.write("reporter:counter:DataCleaning,final_clean_rows,1\n")
        # Emit: transaction_id \t normalized fields (supports both 7-field and 9-field schema)
        sys.stdout.write(f"{curr_id}\t" + "\t".join(chosen) + "\n")
    else:
        # Conflicting records sharing same transaction ID: exclude ALL of them
        counters["conflicting_rows_excluded"] += n
        sys.stderr.write(f"reporter:counter:DataCleaning,conflicting_rows_excluded,{n}\n")

def main():
    current_id = None
    current_records = []
    counters = {
        "final_clean_rows": 0,
        "duplicate_rows_removed": 0,
        "conflicting_rows_excluded": 0
    }

    for line in sys.stdin:
        line = line.rstrip("\r\n")
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) < 8:
            continue

        t_id = parts[0]
        val_tuple = tuple(parts[1:])  # price, sale_date, sale_year, sale_month, county, property_type, tenure, [postcode, town]

        if current_id == t_id:
            current_records.append(val_tuple)
        else:
            if current_id is not None:
                process_group(current_id, current_records, counters)
            current_id = t_id
            current_records = [val_tuple]

    if current_id is not None:
        process_group(current_id, current_records, counters)

    # Final summary counters for reducer
    sys.stderr.write(f"reporter:counter:DataCleaning,final_clean_rows_total,{counters['final_clean_rows']}\n")
    sys.stderr.write(f"reporter:counter:DataCleaning,duplicate_rows_removed_total,{counters['duplicate_rows_removed']}\n")
    sys.stderr.write(f"reporter:counter:DataCleaning,conflicting_rows_excluded_total,{counters['conflicting_rows_excluded']}\n")

if __name__ == "__main__":
    main()
