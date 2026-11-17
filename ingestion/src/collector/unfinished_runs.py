from datetime import datetime, timezone


def ensure_unfinished_table(connection):
    connection.execute("""
        CREATE TABLE IF NOT EXISTS unfinished_runs (
            run_id INTEGER PRIMARY KEY,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """)


def list_unfinished_run_ids(connection):
    rows = connection.execute("""
        SELECT run_id
        FROM unfinished_runs
        ORDER BY run_id
        """).fetchall()

    return [row[0] for row in rows]


def remember_unfinished(connection, run):
    if run["status"] == "completed":
        raise ValueError("A completed run must not be stored as unfinished")

    now = datetime.now(timezone.utc).isoformat()

    connection.execute(
        """
        INSERT INTO unfinished_runs (
            run_id, first_seen_at, last_seen_at
        )
        VALUES (?, ?, ?)
        ON CONFLICT(run_id)
        DO UPDATE SET last_seen_at = excluded.last_seen_at
        """,
        (run["id"], now, now),
    )


def forget_unfinished(connection, run_id):
    connection.execute(
        """
        DELETE FROM unfinished_runs
        WHERE run_id = ?
        """,
        (run_id,),
    )
