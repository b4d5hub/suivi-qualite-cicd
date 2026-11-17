import os
from contextlib import contextmanager
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]

LOCK_PATH = PROJECT_ROOT / "ingestion" / "state" / "poll_once.lock"


@contextmanager
def polling_lock():
    """
    Prevent two local polling cycles from running
    at the same time.

    The operating system releases the lock if
    the process exits unexpectedly.
    """
    LOCK_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(LOCK_PATH, "a+b") as handle:
        # Ensure there is one byte to lock on Windows.
        handle.seek(0, os.SEEK_END)

        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()

        handle.seek(0)

        if os.name == "nt":
            import msvcrt

            try:
                msvcrt.locking(
                    handle.fileno(),
                    msvcrt.LK_NBLCK,
                    1,
                )
            except OSError as error:
                raise BlockingIOError("Another polling cycle is running") from error

            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(
                    handle.fileno(),
                    msvcrt.LK_UNLCK,
                    1,
                )

        else:
            import fcntl

            try:
                fcntl.flock(
                    handle.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except BlockingIOError as error:
                raise BlockingIOError("Another polling cycle is running") from error

            try:
                yield
            finally:
                fcntl.flock(
                    handle.fileno(),
                    fcntl.LOCK_UN,
                )
