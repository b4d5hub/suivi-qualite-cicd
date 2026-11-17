import hashlib
import json
import sqlite3
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
