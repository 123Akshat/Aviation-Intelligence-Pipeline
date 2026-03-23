"""
upload_csv_to_gcs.py
--------------------
Reads the real flight_date.xlsx (10,683 rows), converts each row
to a JSON record, and writes date-partitioned NDJSON files to GCS.

This is the ingestion entry point for the static dataset.
In a real-time scenario this would be replaced by the Pub/Sub
subscriber, but for a portfolio project this is how you prove
the pipeline with real data.

Usage:
    # Upload all data (partitioned by Date_of_Journey)
    python upload_csv_to_gcs.py --file flight_date.xlsx

    # Dry run — print first 5 records without uploading
    python upload_csv_to_gcs.py --file flight_date.xlsx --dry-run
"""

import pandas as pd
import json
import os
import re
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
from google.cloud import storage

GCS_BUCKET = os.environ.get("GCS_BUCKET", "skylens-datalake-skylens-india")


# ── Parsing helpers (handle the real messy data) ─────────────

def parse_journey_date(date_str: str) -> str:
    """
    Parse Date_of_Journey: handles both '1/05/2019' and '24/03/2019'
    Returns ISO format: '2019-05-01'
    """
    try:
        # Try DD/MM/YYYY (the format in the file)
        dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        return dt.date().isoformat()
    except ValueError:
        try:
            dt = datetime.strptime(date_str.strip(), "%-d/%m/%Y")
            return dt.date().isoformat()
        except Exception:
            return None


def parse_duration_to_minutes(duration_str: str) -> int:
    """
    Parse Duration column — real formats in the file:
      '2h 50m'  → 170
      '19h'     → 1140
      '21h 5m'  → 1265
      '1h 30m'  → 90
      '25h 30m' → 1530  (overnight multi-stop)
    """
    if not duration_str:
        return None
    duration_str = str(duration_str).strip()
    hours, minutes = 0, 0
    h_match = re.search(r'(\d+)h', duration_str)
    m_match = re.search(r'(\d+)m', duration_str)
    if h_match:
        hours = int(h_match.group(1))
    if m_match:
        minutes = int(m_match.group(1))
    return hours * 60 + minutes if (h_match or m_match) else None


def parse_stops(stops_str: str) -> int:
    """
    Parse Total_Stops — real values in file:
      'non-stop' → 0
      '1 stop'   → 1
      '2 stops'  → 2
      '3 stops'  → 3
      '4 stops'  → 4
    """
    if not stops_str or pd.isna(stops_str):
        return None
    s = str(stops_str).strip().lower()
    if s == 'non-stop':
        return 0
    match = re.search(r'(\d+)', s)
    return int(match.group(1)) if match else None


def standardise_city(city: str) -> str:
    """
    Standardise city names — real spelling issues in the file:
      'Banglore' → 'Bengaluru'
      'New Delhi' and 'Delhi' both exist — keep as-is (different airports)
    """
    if not city:
        return city
    mapping = {
        "Banglore":  "Bengaluru",
        "Bangalore": "Bengaluru",
        "Kolkata":   "Kolkata",
        "Cochin":    "Kochi",
    }
    return mapping.get(city.strip(), city.strip())


def city_to_iata(city: str) -> str:
    """Map city name to IATA code for consistency."""
    mapping = {
        "Delhi":     "DEL",
        "New Delhi": "DEL",
        "Bengaluru": "BLR",
        "Mumbai":    "BOM",
        "Kolkata":   "CCU",
        "Chennai":   "MAA",
        "Kochi":     "COK",
        "Hyderabad": "HYD",
    }
    return mapping.get(city, city)


def classify_airline(airline: str) -> str:
    """Map airline name to category for analysis."""
    lcc = ["IndiGo", "SpiceJet", "GoAir", "Air Asia", "TruJet"]
    fsc = ["Air India", "Vistara", "Jet Airways"]
    if any(a in airline for a in lcc):
        return "LCC"        # Low Cost Carrier
    elif any(a in airline for a in fsc):
        return "FSC"        # Full Service Carrier
    elif "Premium" in airline or "Business" in airline:
        return "PREMIUM"
    return "OTHER"


def row_to_record(row: pd.Series) -> dict:
    """Convert a raw DataFrame row to a clean JSON record."""
    journey_date = parse_journey_date(str(row["Date_of_Journey"]))
    duration_min = parse_duration_to_minutes(str(row["Duration"]))
    stops = parse_stops(row["Total_Stops"])
    source_clean = standardise_city(str(row["Source"]))
    dest_clean = standardise_city(str(row["Destination"]))

    return {
        # Raw fields (preserved for lineage)
        "airline":              str(row["Airline"]).strip(),
        "date_of_journey_raw":  str(row["Date_of_Journey"]).strip(),
        "source_raw":           str(row["Source"]).strip(),
        "destination_raw":      str(row["Destination"]).strip(),
        "route":                str(row["Route"]).strip() if pd.notna(row["Route"]) else None,
        "dep_time":             str(row["Dep_Time"]).strip(),
        "arrival_time":         str(row["Arrival_Time"]).strip(),
        "duration_raw":         str(row["Duration"]).strip(),
        "total_stops_raw":      str(row["Total_Stops"]).strip() if pd.notna(row["Total_Stops"]) else None,
        "additional_info":      str(row["Additional_Info"]).strip(),
        "price":                int(row["Price"]),

        # Parsed / cleaned fields (used by dbt staging as source-of-truth)
        "journey_date":         journey_date,
        "source_city":          source_clean,
        "destination_city":     dest_clean,
        "source_iata":          city_to_iata(source_clean),
        "destination_iata":     city_to_iata(dest_clean),
        "duration_minutes":     duration_min,
        "num_stops":            stops,
        "airline_category":     classify_airline(str(row["Airline"])),
        "has_next_day_arrival": any(
            m in str(row["Arrival_Time"])
            for m in ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        ),

        # Metadata
        "ingested_at": datetime.utcnow().isoformat() + "Z",
    }


def upload_to_gcs(records_by_date: dict, storage_client: storage.Client, dry_run: bool):
    """
    Write date-partitioned NDJSON files to GCS landing zone.
    Path: landing/date=YYYY-MM-DD/flights_<timestamp>.ndjson
    """
    bucket = storage_client.bucket(GCS_BUCKET)
    timestamp = int(datetime.utcnow().timestamp())
    uploaded = 0

    for date_str, records in sorted(records_by_date.items()):
        ndjson = "\n".join(json.dumps(r) for r in records)
        gcs_path = f"landing/date={date_str}/flights_{timestamp}.ndjson"

        if dry_run:
            print(f"[DRY RUN] Would write {len(records)} records → gs://{GCS_BUCKET}/{gcs_path}")
            print("Sample record:")
            print(json.dumps(records[0], indent=2))
            break
        else:
            blob = bucket.blob(gcs_path)
            blob.upload_from_string(ndjson, content_type="application/x-ndjson")
            print(f"Uploaded {len(records):4d} records → {gcs_path}")
            uploaded += len(records)

    return uploaded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="flight_date.xlsx",
                        help="Path to the xlsx file (default: flight_date.xlsx)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and print without uploading")
    args = parser.parse_args()

    print(f"Reading: {args.file}")
    df = pd.read_excel(args.file)
    print(f"Loaded {len(df)} rows × {len(df.columns)} columns")

    # Drop the 2 null rows (Route and Total_Stops each have 1 null)
    before = len(df)
    df = df.dropna(subset=["Route", "Total_Stops"])
    print(f"Dropped {before - len(df)} null rows → {len(df)} clean rows")

    # Group records by journey_date for GCS partitioning
    records_by_date = defaultdict(list)
    parse_errors = 0

    for _, row in df.iterrows():
        try:
            record = row_to_record(row)
            if record["journey_date"]:
                records_by_date[record["journey_date"]].append(record)
            else:
                parse_errors += 1
        except Exception as e:
            parse_errors += 1
            print(f"Parse error on row: {e}")

    print(f"\nParsed into {len(records_by_date)} date partitions")
    print(f"Parse errors: {parse_errors}")

    # Show partition summary
    for date, recs in sorted(records_by_date.items()):
        print(f"  {date}: {len(recs)} flights")

    if args.dry_run:
        storage_client = None
    else:
        storage_client = storage.Client()

    total = upload_to_gcs(records_by_date, storage_client, args.dry_run)
    if not args.dry_run:
        print(f"\nDone. Total uploaded: {total} records")


if __name__ == "__main__":
    main()
