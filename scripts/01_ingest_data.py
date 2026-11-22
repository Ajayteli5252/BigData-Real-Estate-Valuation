"""
Phase 1: Ingest Data from Official HM Land Registry UK Open Government Data
Downloads official annual transaction CSVs:
- 2022 Price Paid Data (pp-2022.csv)
- 2023 Price Paid Data (pp-2023.csv)
"""
import os
import sys
import time
import urllib.request

RAW_DIR = os.path.join("data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

DATASETS = {
    "land_registry_2022": {
        "url": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2022.csv",
        "dest": os.path.join(RAW_DIR, "pp-2022.csv")
    },
    "land_registry_2023": {
        "url": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2023.csv",
        "dest": os.path.join(RAW_DIR, "pp-2023.csv")
    }
}

def download_file(url: str, dest_path: str):
    if os.path.exists(dest_path):
        size_mb = os.path.getsize(dest_path) / (1024 * 1024)
        print(f"[SKIP] {dest_path} already exists ({size_mb:.2f} MB).")
        return

    print(f"[DOWNLOADING] {url}")
    print(f"       -> Saving to: {dest_path}")
    
    start_time = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    
    with urllib.request.urlopen(req, timeout=60) as response, open(dest_path, "wb") as out_file:
        total_size = int(response.headers.get("Content-Length", 0))
        downloaded = 0
        chunk_size = 1024 * 1024  # 1 MB chunk
        last_logged = 0

        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            out_file.write(chunk)
            downloaded += len(chunk)
            
            if downloaded - last_logged >= 10 * 1024 * 1024 or (total_size > 0 and downloaded == total_size):
                elapsed = time.time() - start_time
                speed = (downloaded / (1024 * 1024)) / (elapsed if elapsed > 0 else 1)
                pct = (downloaded / total_size * 100) if total_size > 0 else 0
                print(f"  Progress: {downloaded / (1024*1024):.1f} MB / {total_size / (1024*1024):.1f} MB ({pct:.1f}%) at {speed:.2f} MB/s")
                last_logged = downloaded

    total_time = time.time() - start_time
    final_size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    print(f"[SUCCESS] Downloaded {final_size_mb:.2f} MB in {total_time:.1f}s.")

def main():
    print("=" * 65)
    print("Ingesting HM Land Registry Official Price Paid Datasets (2022 & 2023)")
    print("=" * 65)

    for lr_key in ["land_registry_2022", "land_registry_2023"]:
        lr_info = DATASETS[lr_key]
        download_file(lr_info["url"], lr_info["dest"])

    print("\n" + "=" * 65)
    print("[DOWNLOAD VERIFICATION COMPLETE]")
    for item in os.listdir(RAW_DIR):
        p = os.path.join(RAW_DIR, item)
        if os.path.isfile(p):
            print(f"  - {item}: {os.path.getsize(p) / (1024*1024):.2f} MB")
    print("=" * 65)

if __name__ == "__main__":
    main()
