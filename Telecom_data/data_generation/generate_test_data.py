"""
Telecom Load Test Data Generator
=================================
Generates 300 CDR + 300 network event files per day
simulating 300 regions/towers sending data daily.

Structure:
    output/raw/cdr/cdr_YYYYMMDD_001.csv     ← region 001
    output/raw/cdr/cdr_YYYYMMDD_002.csv     ← region 002
    ...
    output/raw/cdr/cdr_YYYYMMDD_300.csv     ← region 300

    output/raw/network_events/events_YYYYMMDD_001.json
    ...
    output/raw/network_events/events_YYYYMMDD_300.json

Config:
    NUM_DAYS       = 1    → 1 day of data
    FILES_PER_DAY  = 300   → 300 region files per day
    Total files    = 1 × 300 × 2 = 600 files

Run:
    pip install faker
    python generate_test_data.py
"""

import csv
import json
import os
import random
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

# ── CONFIG ────────────────────────────────────────────────────────────────────
SEED             = 99
NUM_DAYS         = 1    # number of days to generate
FILES_PER_DAY    = 1000     # number of region files per day
START_DATE       = datetime(2026, 3, 20)
CDR_ROWS_PER_FILE    = 200     # rows per region file
EVENTS_PER_FILE      = 100     # events per region file
OUTPUT_DIR           = "output/raw"
MAX_WORKERS          = 10      # parallel file generation
# ─────────────────────────────────────────────────────────────────────────────

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

CALL_TYPES    = ["local", "long_distance", "international", "toll_free"]
CALL_STATUSES = ["completed", "missed", "dropped", "busy"]
EVENT_TYPES   = ["dropped_call", "poor_signal", "data_throttle",
                 "tower_outage", "handoff_failure",
                 "network_congestion", "reconnect_success"]
SEVERITIES    = ["low", "medium", "high", "critical"]
RESOLUTIONS   = ["auto_reconnect", "manual_reset", "tower_switch",
                 "escalated", "resolved", "no_action"]

# Intentional bad data ratios — simulates real world data quality issues
BAD_DATA_RATIO = {
    "null_duration":    0.02,   # 2%  null durations
    "duplicate":        0.01,   # 1%  duplicate rows
    "negative_charge":  0.005,  # 0.5% negative charges
    "future_date":      0.003,  # 0.3% future dates
    "invalid_customer": 0.01    # 1%  invalid customer IDs
}


def generate_cdr(date_str: str, date_key: str,
                 region: int, filepath: Path):
    """Generate CDR CSV for one region on one date."""
    rng = random.Random(f"{date_key}_{region}")

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "cdr_id", "customer_id", "call_date", "call_time",
            "call_duration_mins", "call_type", "destination",
            "charge", "status", "roaming"
        ])
        writer.writeheader()

        rows_written = set()
        for i in range(CDR_ROWS_PER_FILE):
            call_type = rng.choice(CALL_TYPES)
            duration  = round(rng.expovariate(1/5), 2)
            rate      = {"local": 0.05, "long_distance": 0.15,
                         "international": 0.45, "toll_free": 0.0}
            cdr_id    = str(uuid.UUID(int=rng.getrandbits(128)))

            # Inject bad data
            if rng.random() < BAD_DATA_RATIO["null_duration"]:
                duration = None
            if rng.random() < BAD_DATA_RATIO["negative_charge"]:
                charge = -abs(round(duration * rate[call_type], 4)) if duration else -1.0
            else:
                charge = round(duration * rate[call_type], 4) if duration else 0.0
            if rng.random() < BAD_DATA_RATIO["future_date"]:
                call_date = (datetime.now() + timedelta(days=rng.randint(1, 30))).strftime("%Y-%m-%d")
            else:
                call_date = date_str
            if rng.random() < BAD_DATA_RATIO["invalid_customer"]:
                customer_id = f"INVALID_{rng.randint(1, 9999)}"
            else:
                customer_id = f"CUST{str(rng.randint(1, 5000)).zfill(6)}"

            row = {
                "cdr_id":             cdr_id,
                "customer_id":        customer_id,
                "call_date":          call_date,
                "call_time":          f"{date_str} {rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}",
                "call_duration_mins": duration,
                "call_type":          call_type,
                "destination":        f"+1{rng.randint(2000000000,9999999999)}",
                "charge":             charge,
                "status":             rng.choices(
                    CALL_STATUSES, weights=[0.85, 0.07, 0.05, 0.03])[0],
                "roaming":            rng.choice([0, 1])
            }
            writer.writerow(row)

            # Inject duplicate row
            if rng.random() < BAD_DATA_RATIO["duplicate"]:
                writer.writerow(row)


def generate_events(date_str: str, date_key: str,
                    region: int, filepath: Path):
    """Generate network events JSON for one region on one date."""
    rng    = random.Random(f"{date_key}_{region}_events")
    events = []

    for _ in range(EVENTS_PER_FILE):
        event_type = rng.choice(EVENT_TYPES)
        signal     = (rng.randint(-110, -90)
                      if event_type in ["tower_outage", "poor_signal"]
                      else rng.randint(-90, -60))
        events.append({
            "event_id":        str(uuid.UUID(int=rng.getrandbits(128))),
            "customer_id":     f"CUST{str(rng.randint(1, 5000)).zfill(6)}",
            "tower_id":        f"TWR{str(rng.randint(1, 200)).zfill(4)}",
            "timestamp":       f"{date_str} {rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}",
            "event_date":      date_str,
            "event_type":      event_type,
            "signal_strength": signal if rng.random() > 0.05 else None,
            "data_speed_mbps": round(rng.uniform(0.5, 100.0), 2),
            "resolution":      rng.choice(RESOLUTIONS),
            "duration_secs":   rng.randint(1, 300),
            "severity":        rng.choices(
                SEVERITIES, weights=[0.4, 0.35, 0.20, 0.05])[0]
        })

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(events, f)


def generate_day(day_index: int,
                 cdr_dir: Path,
                 events_dir: Path) -> dict:
    """Generate all 300 CDR + 300 event files for one day."""
    date     = START_DATE + timedelta(days=day_index)
    date_str = date.strftime("%Y-%m-%d")
    date_key = date.strftime("%Y%m%d")

    for region in range(1, FILES_PER_DAY + 1):
        region_str  = str(region).zfill(3)
        cdr_path    = cdr_dir    / f"cdr_{date_key}_{region_str}.csv"
        events_path = events_dir / f"events_{date_key}_{region_str}.json"

        generate_cdr(date_str, date_key, region, cdr_path)
        generate_events(date_str, date_key, region, events_path)

    return {
        "date":      date_str,
        "cdr_files": FILES_PER_DAY,
        "evt_files": FILES_PER_DAY
    }


def main():
    total_file_pairs = NUM_DAYS * FILES_PER_DAY
    total_cdr_rows   = total_file_pairs * CDR_ROWS_PER_FILE
    total_evt_rows   = total_file_pairs * EVENTS_PER_FILE

    print("=" * 60)
    print("  Telecom Load Test Data Generator")
    print("=" * 60)
    print(f"  Days:              {NUM_DAYS}")
    print(f"  Files per day:     {FILES_PER_DAY} CDR + {FILES_PER_DAY} events")
    print(f"  Total file pairs:  {total_file_pairs:,}")
    print(f"  CDR rows/file:     {CDR_ROWS_PER_FILE}")
    print(f"  Events/file:       {EVENTS_PER_FILE}")
    print(f"  Total CDR rows:    {total_cdr_rows:,}")
    print(f"  Total event rows:  {total_evt_rows:,}")
    print(f"  Total files:       {total_file_pairs * 2:,}")
    print("=" * 60)

    cdr_dir    = Path(OUTPUT_DIR) / "cdr"
    events_dir = Path(OUTPUT_DIR) / "network_events"
    cdr_dir.mkdir(parents=True, exist_ok=True)
    events_dir.mkdir(parents=True, exist_ok=True)

    days_done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(generate_day, i, cdr_dir, events_dir): i
            for i in range(NUM_DAYS)
        }
        for future in as_completed(futures):
            result   = future.result()
            days_done += 1
            if days_done % 5 == 0 or days_done == NUM_DAYS:
                print(f"  Generated {days_done}/{NUM_DAYS} days "
                      f"({days_done * FILES_PER_DAY * 2:,} files)...")

    print(f"\n  Done!")
    print(f"  CDR files:    {total_file_pairs:,} → {OUTPUT_DIR}/cdr/")
    print(f"  Event files:  {total_file_pairs:,} → {OUTPUT_DIR}/network_events/")
    print(f"  Total files:  {total_file_pairs * 2:,}")
    print(f"\n  File naming:")
    print(f"  cdr_20240201_001.csv ... cdr_20240201_300.csv  (day 1)")
    print(f"  cdr_20240202_001.csv ... cdr_20240202_300.csv  (day 2)")
    print(f"\n  Next: upload to S3 and trigger Databricks pipeline!")
    print("=" * 60)


if __name__ == "__main__":
    main()