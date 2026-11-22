"""
Create Sample Raw Dataset for Classroom Demonstration
CSE3712 — Big Data Analytics
Extracts a 20,000-row sample from official HM Land Registry 2022 and 2023 CSVs,
and injects labelled test edge-cases so every Hadoop counter triggers and reconciles.
Output: data/sample/raw/sample_raw.csv
"""
import os
import csv
import sys

RAW_DIR = os.path.join("data", "raw")
SAMPLE_RAW_DIR = os.path.join("data", "sample", "raw")
os.makedirs(SAMPLE_RAW_DIR, exist_ok=True)
SAMPLE_OUT = os.path.join(SAMPLE_RAW_DIR, "sample_raw.csv")

def main():
    print("[SAMPLE GENERATOR] Extracting representative sample from raw 2022 & 2023 CSVs...")
    
    file_2022 = os.path.join(RAW_DIR, "pp-2022.csv")
    file_2023 = os.path.join(RAW_DIR, "pp-2023.csv")
    
    if not os.path.exists(file_2022) or not os.path.exists(file_2023):
        print(f"Error: Raw CSV files not found in {RAW_DIR}. Run scripts/01_ingest_data.py first.")
        sys.exit(1)
        
    records = []
    
    # 10,000 records from 2022
    with open(file_2022, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= 10000:
                break
            records.append(row)
            
    # 10,000 records from 2023
    with open(file_2023, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= 10000:
                break
            records.append(row)
            
    print(f"  -> Extracted {len(records):,} base records from official HM Land Registry CSVs.")
    
    # Injected Demonstration Edge Cases (Clearly labelled for demonstration)
    demo_edge_cases = [
        # 1. Duplicate rows (identical transaction_id and fields - reducer must keep 1 and drop 1)
        records[100],  # Duplicate of record 100
        records[200],  # Duplicate of record 200
        records[300],  # Duplicate of record 300
        records[400],  # Duplicate of record 400
        records[500],  # Duplicate of record 500
        
        # 2. Conflicting rows sharing same ID (reducer must exclude both)
        ["{DEMO-CONFLICT-ID-001}", "250000", "2022-05-10 00:00", "B1 1AA", "S", "N", "F", "1", "", "HIGH ST", "", "BIRMINGHAM", "BIRMINGHAM", "WEST MIDLANDS", "A", "A"],
        ["{DEMO-CONFLICT-ID-001}", "350000", "2022-05-10 00:00", "B1 1AA", "D", "N", "F", "1", "", "HIGH ST", "", "BIRMINGHAM", "BIRMINGHAM", "WEST MIDLANDS", "A", "A"],
        ["{DEMO-CONFLICT-ID-002}", "180000", "2023-08-15 00:00", "M1 1AA", "F", "N", "L", "2", "", "MARKET ST", "", "MANCHESTER", "MANCHESTER", "GREATER MANCHESTER", "A", "A"],
        ["{DEMO-CONFLICT-ID-002}", "180000", "2023-09-20 00:00", "M1 1AA", "F", "N", "L", "2", "", "MARKET ST", "", "MANCHESTER", "MANCHESTER", "GREATER MANCHESTER", "A", "A"],
        
        # 3. Out-of-range prices (< £15,000 or > £20,000,000)
        ["{DEMO-PRICE-LOW-001}", "5000", "2022-06-01 00:00", "E1 1AA", "F", "N", "L", "10", "", "ROAD", "", "LONDON", "TOWER HAMLETS", "GREATER LONDON", "A", "A"],
        ["{DEMO-PRICE-LOW-002}", "12000", "2022-06-01 00:00", "E1 1AA", "F", "N", "L", "11", "", "ROAD", "", "LONDON", "TOWER HAMLETS", "GREATER LONDON", "A", "A"],
        ["{DEMO-PRICE-HIGH-001}", "25000000", "2023-01-10 00:00", "SW1 1AA", "D", "N", "F", "1", "", "SQUARE", "", "LONDON", "WESTMINSTER", "GREATER LONDON", "A", "A"],
        
        # 4. Invalid non-numeric prices
        ["{DEMO-PRICE-INV-001}", "INVALID_PRICE", "2022-04-01 00:00", "E1 1AA", "F", "N", "L", "1", "", "ST", "", "LONDON", "TOWER HAMLETS", "GREATER LONDON", "A", "A"],
        ["{DEMO-PRICE-INV-002}", "-50000", "2023-04-01 00:00", "E1 1AA", "F", "N", "L", "1", "", "ST", "", "LONDON", "TOWER HAMLETS", "GREATER LONDON", "A", "A"],
        
        # 5. Invalid dates
        ["{DEMO-DATE-INV-001}", "300000", "2022-13-45 00:00", "E1 1AA", "F", "N", "L", "1", "", "ST", "", "LONDON", "TOWER HAMLETS", "GREATER LONDON", "A", "A"],
        ["{DEMO-DATE-INV-002}", "300000", "NOT_A_DATE", "E1 1AA", "F", "N", "L", "1", "", "ST", "", "LONDON", "TOWER HAMLETS", "GREATER LONDON", "A", "A"],
        
        # 6. Missing required fields (empty county)
        ["{DEMO-MISSING-001}", "300000", "2022-05-01 00:00", "E1 1AA", "F", "N", "L", "1", "", "ST", "", "LONDON", "TOWER HAMLETS", "", "A", "A"],
        ["{DEMO-MISSING-002}", "300000", "2022-05-01 00:00", "E1 1AA", "F", "N", "L", "1", "", "ST", "", "LONDON", "TOWER HAMLETS", "UNKNOWN", "A", "A"],
        
        # 7. Unsupported category codes (tenure 'U', property_type 'Z')
        ["{DEMO-CAT-INV-001}", "300000", "2022-07-01 00:00", "E1 1AA", "Z", "N", "F", "1", "", "ST", "", "LONDON", "TOWER HAMLETS", "GREATER LONDON", "A", "A"],
        ["{DEMO-CAT-INV-002}", "300000", "2022-07-01 00:00", "E1 1AA", "F", "N", "U", "1", "", "ST", "", "LONDON", "TOWER HAMLETS", "GREATER LONDON", "A", "A"]
    ]
    
    total_sample = records + demo_edge_cases
    
    with open(SAMPLE_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(total_sample)
        
    print(f"[SAMPLE GENERATOR] Created {SAMPLE_OUT} with {len(total_sample):,} total rows.")
    print(f"  - Clean real records: 20,000")
    print(f"  - Injected edge cases: {len(demo_edge_cases)} (5 duplicates, 4 conflicts, 3 out-of-range, 2 invalid prices, 2 invalid dates, 2 missing fields, 2 unsupported categories)")

if __name__ == "__main__":
    main()
