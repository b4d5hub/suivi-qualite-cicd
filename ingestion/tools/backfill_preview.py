import _collector_path

from datetime import datetime, timedelta, timezone

import requests

from workflows import collect_one_day


def main():

    # SELECT A COMPLETE DAY, THREE DAYS BEFORE TODAY (UTC)
    target_date = datetime.now(timezone.utc).date() - timedelta(days=3)

    try:

        collect_one_day(target_date)

    except (requests.RequestException, RuntimeError) as error:

        print(f"Collection failed: {error}")


if __name__ == "__main__":
    main()
