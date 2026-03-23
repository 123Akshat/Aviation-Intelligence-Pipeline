"""
pubsub_publisher.py
-------------------
Replays the real flight_date.xlsx records as a streaming Pub/Sub feed.
This simulates what a live airline API feed would look like —
records published one by one (or in batches) with realistic delays.

Usage:
    python pubsub_publisher.py --file flight_date.xlsx
    python pubsub_publisher.py --file flight_date.xlsx --batch-size 50 --delay 0.5
"""

import pandas as pd
import json
import os
import time
import argparse
from datetime import datetime
from google.cloud import pubsub_v1
from upload_csv_to_gcs import row_to_record   # reuse the same parser

PROJECT_ID = os.environ.get("GCP_PROJECT", "skylens-india")
TOPIC_ID = os.environ.get("PUBSUB_TOPIC", "flights-topic")


def publish_batch(publisher, topic_path: str, records: list) -> int:
    """Publish a batch of records to Pub/Sub. Returns count published."""
    futures = []
    for record in records:
        data = json.dumps(record).encode("utf-8")
        # Pub/Sub message attributes allow server-side filtering
        attrs = {
            "airline":      record.get("airline", ""),
            "source":       record.get("source_iata", ""),
            "destination":  record.get("destination_iata", ""),
            "date":         record.get("journey_date", ""),
        }
        future = publisher.publish(topic_path, data, **attrs)
        futures.append(future)
    # Block until all messages are confirmed
    return sum(1 for f in futures if f.result())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="flight_date.xlsx")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Records per Pub/Sub batch (default: 100)")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Seconds between batches (default: 1.0)")
    parser.add_argument("--airline", default=None,
                        help="Filter to one airline (e.g. 'IndiGo')")
    args = parser.parse_args()

    print(f"Reading {args.file}...")
    df = pd.read_excel(args.file)
    df = df.dropna(subset=["Route", "Total_Stops"])

    if args.airline:
        df = df[df["Airline"] == args.airline]
        print(f"Filtered to {args.airline}: {len(df)} rows")

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    total = 0
    batch = []

    for _, row in df.iterrows():
        try:
            record = row_to_record(row)
            batch.append(record)
        except Exception as e:
            print(f"Row parse error: {e}")
            continue

        if len(batch) >= args.batch_size:
            count = publish_batch(publisher, topic_path, batch)
            total += count
            print(f"Published batch of {count} | Total: {total}")
            batch = []
            time.sleep(args.delay)

    # Publish remaining records
    if batch:
        count = publish_batch(publisher, topic_path, batch)
        total += count
        print(f"Published final batch of {count} | Total: {total}")

    print(f"\nDone. Published {total} records to {topic_path}")


if __name__ == "__main__":
    main()
