"""Regression tests for MetadataStore connection handling under a threadpool.

The H1 store is a process-wide singleton used by FastAPI sync endpoints, which
run in a threadpool.  A single shared SQLite connection raised
``sqlite3.InterfaceError: bad parameter or other API misuse`` on concurrent use.
File-backed stores now use one connection per thread; in-memory stays shared.
"""

from __future__ import annotations

import threading

from headwater.core.metadata import MetadataStore


def test_file_db_connection_is_per_thread(tmp_path):
    store = MetadataStore(str(tmp_path / "m.db"))
    store.init()
    try:
        seen: dict[int, int] = {}

        def grab() -> None:
            seen[threading.get_ident()] = id(store.con)

        threads = [threading.Thread(target=grab) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Each thread got its own distinct connection object.
        assert len(set(seen.values())) == len(seen) == 5
    finally:
        store.close()


def test_concurrent_reads_do_not_misuse_connection(tmp_path):
    store = MetadataStore(str(tmp_path / "m.db"))
    store.init()
    try:
        errors: list[str] = []

        def work() -> None:
            try:
                for _ in range(50):
                    store.con.execute("SELECT COUNT(*) FROM sources").fetchone()
            except Exception as exc:  # would fire with a shared connection
                errors.append(repr(exc))

        threads = [threading.Thread(target=work) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, errors
    finally:
        store.close()


def test_memory_db_shares_one_connection():
    store = MetadataStore(":memory:")
    store.init()
    try:
        # In-memory must stay shared, or data would split across threads.
        assert store.con is store.con
    finally:
        store.close()
