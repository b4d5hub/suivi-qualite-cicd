import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# FIND THE REPOSITORY ROOT DIRECTORY
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# LOCAL CHECKPOINT DATABASE LOCATION
DB_PATH = PROJECT_ROOT / "ingestion" / "state" / "collector.db"


def open_state():

    # MAKE SURE THE STATE DIRECTORY EXISTS
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # OPEN THE SQLITE CONNECTION
    connection = sqlite3.connect(DB_PATH)

    # CREATE THE CHECKPOINT TABLE IF IT DOES NOT EXIST YET
    connection.execute("""
        CREATE TABLE IF NOT EXISTS published_events (
            topic TEXT NOT NULL,
            message_key TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            published_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (topic, message_key)
        )
    """)

    connection.commit()

    return connection


def calculate_hash(payload):

    # SERIALIZE THE PAYLOAD CANONICALLY (SORTED KEYS, NO WHITESPACE)
    serialized = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )

    # RETURN A STABLE SHA-256 FINGERPRINT
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def needs_publishing(connection, topic, key, payload):

    # LOOK UP ANY EXISTING CHECKPOINT FOR THIS (TOPIC, KEY)
    row = connection.execute(
        """
        SELECT payload_hash
        FROM published_events
        WHERE topic = ? AND message_key = ?
        """,
        (topic, str(key)),
    ).fetchone()

    # HASH THE INCOMING PAYLOAD FOR COMPARISON
    current_hash = calculate_hash(payload)

    # NEW OR CHANGED PAYLOAD NEEDS PUBLISHING
    return row is None or row[0] != current_hash


def remember_published(connection, messages):

    # WRAP THE BATCH IN A TRANSACTION SO CHECKPOINTS SAVE ATOMICALLY
    with connection:

        # INSERT OR UPDATE ONE ROW PER (TOPIC, KEY)
        for topic, key, payload in messages:

            connection.execute(
                """
                INSERT INTO published_events (
                    topic, message_key, payload_hash
                )
                VALUES (?, ?, ?)
                ON CONFLICT(topic, message_key)
                DO UPDATE SET
                    payload_hash = excluded.payload_hash,
                    published_at = CURRENT_TIMESTAMP
                """,
                (topic, str(key), calculate_hash(payload)),
            )


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


def ensure_watch_table(connection):
    connection.execute("""
        CREATE TABLE IF NOT EXISTS deployment_watch (
            deployment_id INTEGER PRIMARY KEY,
            deployment_json TEXT NOT NULL,
            last_state TEXT,
            checked_at TEXT NOT NULL
        )
        """)


def table_exists(connection, table_name):
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()

    return row is not None


def load_watched_deployments(connection):
    if not table_exists(connection, "deployment_watch"):
        return {}

    rows = connection.execute("""
        SELECT deployment_id, deployment_json
        FROM deployment_watch
        """).fetchall()

    watched = {}

    for deployment_id, deployment_json in rows:
        deployment = json.loads(deployment_json)

        if deployment["id"] != deployment_id:
            raise RuntimeError("Invalid deployment watch record")

        watched[deployment_id] = deployment

    return watched


def remember_watched(connection, deployment, state, checked_at):
    connection.execute(
        """
        INSERT INTO deployment_watch (
            deployment_id,
            deployment_json,
            last_state,
            checked_at
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT(deployment_id)
        DO UPDATE SET
            deployment_json = excluded.deployment_json,
            last_state = excluded.last_state,
            checked_at = excluded.checked_at
        """,
        (
            deployment["id"],
            json.dumps(deployment, sort_keys=True),
            state,
            checked_at.isoformat(),
        ),
    )


def forget_watched(connection, deployment_id):
    connection.execute(
        """
        DELETE FROM deployment_watch
        WHERE deployment_id = ?
        """,
        (deployment_id,),
    )
