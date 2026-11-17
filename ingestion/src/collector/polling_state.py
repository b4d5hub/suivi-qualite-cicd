from datetime import datetime, timezone


def ensure_polling_table(connection):
    connection.execute("""
        CREATE TABLE IF NOT EXISTS polling_state (
            stream TEXT PRIMARY KEY,
            last_successful_scan_start TEXT NOT NULL
        )
        """)


def get_last_poll(connection, stream):
    row = connection.execute(
        """
        SELECT last_successful_scan_start
        FROM polling_state
        WHERE stream = ?
        """,
        (stream,),
    ).fetchone()

    if row is None:
        return None

    return datetime.fromisoformat(row[0]).astimezone(timezone.utc)


def record_successful_poll(connection, stream, scan_start):
    if scan_start.tzinfo is None or scan_start.utcoffset() is None:
        raise ValueError("scan_start must be timezone-aware")

    scan_start = scan_start.astimezone(timezone.utc)

    previous = get_last_poll(connection, stream)

    if previous is not None and scan_start < previous:
        raise ValueError("Polling watermark cannot move backwards")

    connection.execute(
        """
        INSERT INTO polling_state (
            stream,
            last_successful_scan_start
        )
        VALUES (?, ?)
        ON CONFLICT(stream)
        DO UPDATE SET
            last_successful_scan_start =
                excluded.last_successful_scan_start
        """,
        (stream, scan_start.isoformat()),
    )
