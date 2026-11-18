import _collector_path

import argparse
from datetime import date

from workflows import backfill_day


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="UTC date in YYYY-MM-DD format")
    args = parser.parse_args()
    backfill_day(date.fromisoformat(args.date))


if __name__ == "__main__":
    main()
