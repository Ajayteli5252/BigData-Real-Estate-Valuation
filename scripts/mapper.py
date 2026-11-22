#!/usr/bin/env python3
"""
Hadoop Streaming Mapper for UK Real Estate Data Cleaning
CSE3712 — Big Data Analytics
Reads raw official HM Land Registry CSV lines from sys.stdin.
Validates fields, normalizes codes, applies price/date filters,
and emits tab-separated key-value pairs for Hadoop reducer.
"""
import sys
import csv
import re

# Supported Category Codes
VALID_PROPERTY_TYPES = {'D', 'S', 'T', 'F', 'O'}  # Detached, Semi, Terraced, Flat, Other
VALID_TENURES = {'F', 'L'}                        # Freehold, Leasehold
MIN_PRICE = 15000
MAX_PRICE = 20000000

def main():
    raw_rows = 0
    rejected_rows = 0
    rejected_missing_field = 0
    rejected_invalid_price = 0
    rejected_out_of_range_price = 0
    rejected_invalid_date = 0
    rejected_unsupported_category = 0
    mapper_emitted_rows = 0

    # csv.reader handles quoted fields with internal commas properly
    reader = csv.reader(sys.stdin)

    for row in reader:
        raw_rows += 1
        if not row or len(row) < 14:
            rejected_rows += 1
            rejected_missing_field += 1
            sys.stderr.write("reporter:counter:DataCleaning,rejected_missing_field,1\n")
            continue

        raw_id = row[0].strip().strip("{}").strip()
        raw_price = row[1].strip()
        raw_date = row[2].strip()
        raw_postcode = row[3].strip().upper() if len(row) > 3 and row[3] else ""
        raw_prop_type = row[4].strip().upper()
        raw_tenure = row[6].strip().upper()
        raw_town = row[11].strip().upper() if len(row) > 11 and row[11] else ""
        raw_county = row[13].strip().upper()

        # Check required fields
        if not raw_id or not raw_price or not raw_date or not raw_prop_type or not raw_tenure or not raw_county:
            rejected_rows += 1
            rejected_missing_field += 1
            sys.stderr.write("reporter:counter:DataCleaning,rejected_missing_field,1\n")
            continue

        if raw_county in {'', 'UNKNOWN', 'NULL', 'NONE'}:
            rejected_rows += 1
            rejected_missing_field += 1
            sys.stderr.write("reporter:counter:DataCleaning,rejected_missing_field,1\n")
            continue

        # Validate Price
        try:
            price_val = float(raw_price)
            price = int(round(price_val))
            if price <= 0:
                raise ValueError("Price must be positive")
        except (ValueError, TypeError):
            rejected_rows += 1
            rejected_invalid_price += 1
            sys.stderr.write("reporter:counter:DataCleaning,rejected_invalid_price,1\n")
            continue

        # Filter Price Range (£15,000 - £20,000,000)
        if price < MIN_PRICE or price > MAX_PRICE:
            rejected_rows += 1
            rejected_out_of_range_price += 1
            sys.stderr.write("reporter:counter:DataCleaning,rejected_out_of_range_price,1\n")
            continue

        # Validate and parse date (YYYY-MM-DD)
        date_str = raw_date[:10]
        date_match = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", date_str)
        if not date_match:
            rejected_rows += 1
            rejected_invalid_date += 1
            sys.stderr.write("reporter:counter:DataCleaning,rejected_invalid_date,1\n")
            continue

        year = int(date_match.group(1))
        month = int(date_match.group(2))
        day = int(date_match.group(3))

        if year not in (2022, 2023) or not (1 <= month <= 12) or not (1 <= day <= 31):
            rejected_rows += 1
            rejected_invalid_date += 1
            sys.stderr.write("reporter:counter:DataCleaning,rejected_invalid_date,1\n")
            continue

        sale_date = f"{year:04d}-{month:02d}-{day:02d}"
        sale_year = str(year)
        sale_month = f"{month:02d}"

        # Validate Categories
        if raw_prop_type not in VALID_PROPERTY_TYPES or raw_tenure not in VALID_TENURES:
            rejected_rows += 1
            rejected_unsupported_category += 1
            sys.stderr.write("reporter:counter:DataCleaning,rejected_unsupported_category,1\n")
            continue

        # Emit normalized record: key = transaction_id
        # Value = price, sale_date, sale_year, sale_month, county, property_type, tenure, postcode, town
        mapper_emitted_rows += 1
        sys.stdout.write(f"{raw_id}\t{price}\t{sale_date}\t{sale_year}\t{sale_month}\t{raw_county}\t{raw_prop_type}\t{raw_tenure}\t{raw_postcode}\t{raw_town}\n")

    # Final summary counters for mapper
    sys.stderr.write(f"reporter:counter:DataCleaning,raw_rows,{raw_rows}\n")
    sys.stderr.write(f"reporter:counter:DataCleaning,rejected_rows,{rejected_rows}\n")
    sys.stderr.write(f"reporter:counter:DataCleaning,mapper_emitted_rows,{mapper_emitted_rows}\n")

if __name__ == "__main__":
    main()
