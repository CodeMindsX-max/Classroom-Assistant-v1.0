import os
import sqlite3
import threading
from contextlib import contextmanager

from app_logger_manager import get_app_logger, log_event, log_info

DATABASE_FILE = "classroom_assistant.db"
SCHEMA_VERSION = "1"
DATABASE_INIT_LOCK = threading.RLock()
CONNECTION_LOCK = threading.RLock()
THREAD_CONNECTIONS = {}
LOGGER = get_app_logger("database_manager")


def _where(function_name): return f"database_manager.{function_name}"

def _safe_entry_context(entry):
    return {key: entry.get(key) for key in ("id", "day", "start_time", "end_time", "class_name", "joined")}


def _entry_to_db_params(entry):
    return (
        entry["id"],
        entry["day"],
        entry["start_time"],
        entry["end_time"],
        entry["class_name"],
        entry["classroom_url"],
        int(entry["joined"]),
    )


def _recycle_record_to_db_params(record):
    return (
        record["recycle_id"],
        record["deleted_at"],
        record["entry"]["id"],
        record["entry"]["day"],
        record["entry"]["start_time"],
        record["entry"]["end_time"],
        record["entry"]["class_name"],
        record["entry"]["classroom_url"],
        int(record["entry"]["joined"]),
    )


def _close_connection(connection):
    try:
        if connection.in_transaction:
            connection.rollback()
    except sqlite3.Error:
        pass
    finally:
        connection.close()


def _close_all_connections():
    with CONNECTION_LOCK:
        connections = list(THREAD_CONNECTIONS.values())
        THREAD_CONNECTIONS.clear()
    for connection in connections:
        try:
            _close_connection(connection)
        except sqlite3.Error:
            continue


def _get_thread_connection():
    thread_id = threading.get_ident()
    with CONNECTION_LOCK:
        connection = THREAD_CONNECTIONS.get(thread_id)
        if connection is not None:
            try:
                connection.execute("SELECT 1")
                return connection
            except sqlite3.Error:
                try:
                    _close_connection(connection)
                except sqlite3.Error:
                    pass
                THREAD_CONNECTIONS.pop(thread_id, None)

        connection = sqlite3.connect(
            get_database_path(),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.execute("PRAGMA busy_timeout = 30000")
        THREAD_CONNECTIONS[thread_id] = connection
        return connection


def set_database_path(path):
    global DATABASE_FILE
    _close_all_connections()
    DATABASE_FILE = path


def get_database_path():
    return os.path.abspath(DATABASE_FILE)


def _connect():
    return _get_thread_connection()


@contextmanager
def read_connection():
    connection = _connect()
    yield connection


@contextmanager
def write_transaction():
    connection = _connect()
    try:
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise


def initialize_database():
    with DATABASE_INIT_LOCK:
        database_path = get_database_path()
        os.makedirs(os.path.dirname(database_path) or ".", exist_ok=True)

        with write_transaction() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS app_meta (
                    meta_key TEXT PRIMARY KEY,
                    meta_value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS classes (
                    entry_id TEXT PRIMARY KEY,
                    day TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    class_name TEXT NOT NULL,
                    classroom_url TEXT NOT NULL,
                    joined INTEGER NOT NULL DEFAULT 0 CHECK(joined IN (0, 1)),
                    UNIQUE(day, start_time, end_time)
                );

                CREATE TABLE IF NOT EXISTS recycle_bin (
                    recycle_id TEXT PRIMARY KEY,
                    deleted_at TEXT NOT NULL,
                    entry_id TEXT NOT NULL,
                    day TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    class_name TEXT NOT NULL,
                    classroom_url TEXT NOT NULL,
                    joined INTEGER NOT NULL DEFAULT 0 CHECK(joined IN (0, 1))
                );

                CREATE INDEX IF NOT EXISTS idx_classes_day_start_entry
                ON classes(day, start_time, entry_id);
                """
            )

        log_info(
            LOGGER,
            "SQLite database initialized.",
            what="Ensured the SQLite schema exists before storage operations run.",
            where=_where("initialize_database"),
            why="The application needs a ready local database before it can read or write timetable data.",
            context={"database_path": database_path},
        )


def is_migration_complete():
    with read_connection() as connection:
        row = connection.execute(
            "SELECT meta_value FROM app_meta WHERE meta_key = ?",
            ("migration_complete",),
        ).fetchone()
    return bool(row and row["meta_value"] == "1")


def mark_migration_complete():
    with write_transaction() as connection:
        connection.execute(
            "INSERT INTO app_meta(meta_key, meta_value) VALUES(?, ?) "
            "ON CONFLICT(meta_key) DO UPDATE SET meta_value = excluded.meta_value",
            ("migration_complete", "1"),
        )
        connection.execute(
            "INSERT INTO app_meta(meta_key, meta_value) VALUES(?, ?) "
            "ON CONFLICT(meta_key) DO UPDATE SET meta_value = excluded.meta_value",
            ("schema_version", SCHEMA_VERSION),
        )
    log_event(
        LOGGER,
        "Legacy JSON migration marked complete.",
        what="Recorded that the application has migrated into SQLite storage.",
        where=_where("mark_migration_complete"),
        why="Future startups should now use the database instead of remigrating legacy JSON files.",
    )


def reset_migration_state():
    database_path = get_database_path()
    _close_all_connections()
    for suffix in ("", "-wal", "-shm"):
        target = f"{database_path}{suffix}"
        if os.path.exists(target):
            os.remove(target)


def get_open_connection_count():
    with CONNECTION_LOCK:
        return len(THREAD_CONNECTIONS)


def _row_to_entry(row):
    if row is None:
        return None
    return {
        "id": row["entry_id"],
        "day": row["day"],
        "start_time": row["start_time"],
        "end_time": row["end_time"],
        "class_name": row["class_name"],
        "classroom_url": row["classroom_url"],
        "joined": bool(row["joined"]),
    }


def _row_to_recycle_record(row):
    if row is None:
        return None
    return {
        "recycle_id": row["recycle_id"],
        "deleted_at": row["deleted_at"],
        "entry": {
            "id": row["entry_id"],
            "day": row["day"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "class_name": row["class_name"],
            "classroom_url": row["classroom_url"],
            "joined": bool(row["joined"]),
        },
    }


def count_classes():
    with read_connection() as connection:
        row = connection.execute("SELECT COUNT(*) AS count FROM classes").fetchone()
    return row["count"]


def count_recycle_records():
    with read_connection() as connection:
        row = connection.execute("SELECT COUNT(*) AS count FROM recycle_bin").fetchone()
    return row["count"]


def replace_all_data(classes, recycle_bin):
    with write_transaction() as connection:
        connection.execute("DELETE FROM recycle_bin")
        connection.execute("DELETE FROM classes")
        connection.executemany(
            "INSERT INTO classes(entry_id, day, start_time, end_time, class_name, classroom_url, joined) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            [_entry_to_db_params(entry) for entry in classes],
        )
        connection.executemany(
            "INSERT INTO recycle_bin(recycle_id, deleted_at, entry_id, day, start_time, end_time, class_name, classroom_url, joined) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [_recycle_record_to_db_params(record) for record in recycle_bin],
        )

    log_event(
        LOGGER,
        "Replaced SQLite data from validated source records.",
        what="Loaded a full validated dataset into SQLite storage.",
        where=_where("replace_all_data"),
        why="Legacy data or a full-table save operation required replacing the stored rows safely.",
        context={"class_count": len(classes), "recycle_count": len(recycle_bin)},
    )


def replace_classes(classes):
    with write_transaction() as connection:
        connection.execute("DELETE FROM classes")
        connection.executemany(
            "INSERT INTO classes(entry_id, day, start_time, end_time, class_name, classroom_url, joined) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            [_entry_to_db_params(entry) for entry in classes],
        )


def replace_recycle_bin(recycle_bin):
    with write_transaction() as connection:
        connection.execute("DELETE FROM recycle_bin")
        connection.executemany(
            "INSERT INTO recycle_bin(recycle_id, deleted_at, entry_id, day, start_time, end_time, class_name, classroom_url, joined) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [_recycle_record_to_db_params(record) for record in recycle_bin],
        )


def fetch_all_classes():
    with read_connection() as connection:
        rows = connection.execute(
            "SELECT entry_id, day, start_time, end_time, class_name, classroom_url, joined "
            "FROM classes ORDER BY day, start_time, entry_id"
        ).fetchall()
    return [_row_to_entry(row) for row in rows]


def fetch_class_by_id(entry_id):
    with read_connection() as connection:
        row = connection.execute(
            "SELECT entry_id, day, start_time, end_time, class_name, classroom_url, joined "
            "FROM classes WHERE entry_id = ?",
            (entry_id,),
        ).fetchone()
    return _row_to_entry(row)


def fetch_all_recycle_records():
    with read_connection() as connection:
        rows = connection.execute(
            "SELECT recycle_id, deleted_at, entry_id, day, start_time, end_time, class_name, classroom_url, joined "
            "FROM recycle_bin ORDER BY recycle_id"
        ).fetchall()
    return [_row_to_recycle_record(row) for row in rows]


def fetch_recycle_record_by_id(recycle_id):
    with read_connection() as connection:
        row = connection.execute(
            "SELECT recycle_id, deleted_at, entry_id, day, start_time, end_time, class_name, classroom_url, joined "
            "FROM recycle_bin WHERE recycle_id = ?",
            (recycle_id,),
        ).fetchone()
    return _row_to_recycle_record(row)


def fetch_entry_ids_by_prefix(prefix):
    with read_connection() as connection:
        rows = connection.execute(
            "SELECT entry_id FROM classes WHERE entry_id LIKE ?",
            (f"{prefix}_%",),
        ).fetchall()
    return [row["entry_id"] for row in rows]


def slot_exists(day, start_time, end_time, ignore_entry_id=None):
    query = (
        "SELECT 1 FROM classes WHERE day = ? AND start_time < ? AND end_time > ? "
        "AND (? IS NULL OR entry_id <> ?) LIMIT 1"
    )
    with read_connection() as connection:
        row = connection.execute(
            query,
            (day, end_time, start_time, ignore_entry_id, ignore_entry_id),
        ).fetchone()
    return row is not None


def next_recycle_id():
    with read_connection() as connection:
        row = connection.execute(
            "SELECT COALESCE(MAX(CAST(SUBSTR(recycle_id, 5) AS INTEGER)), 0) + 1 AS next_value "
            "FROM recycle_bin"
        ).fetchone()
    return f"bin_{row['next_value']}"


def insert_class(entry):
    with write_transaction() as connection:
        connection.execute(
            "INSERT INTO classes(entry_id, day, start_time, end_time, class_name, classroom_url, joined) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            _entry_to_db_params(entry),
        )
    log_event(
        LOGGER,
        "Inserted a class row into SQLite.",
        what="Stored a new timetable entry in the database.",
        where=_where("insert_class"),
        why="A validated add-entry operation committed a new class row.",
        context={"entry": _safe_entry_context(entry)},
    )


def update_class(entry_id, entry):
    with write_transaction() as connection:
        cursor = connection.execute(
            "UPDATE classes SET day = ?, start_time = ?, end_time = ?, class_name = ?, classroom_url = ?, joined = ? "
            "WHERE entry_id = ?",
            (
                entry["day"],
                entry["start_time"],
                entry["end_time"],
                entry["class_name"],
                entry["classroom_url"],
                int(entry["joined"]),
                entry_id,
            ),
        )
        if cursor.rowcount == 0:
            raise LookupError(f"No entry found with id '{entry_id}'.")


def update_joined_status(entry_id, joined):
    with write_transaction() as connection:
        cursor = connection.execute(
            "UPDATE classes SET joined = ? WHERE entry_id = ?",
            (int(joined), entry_id),
        )
        if cursor.rowcount == 0:
            raise LookupError(f"No entry found with id '{entry_id}'.")


def delete_class(entry_id):
    with write_transaction() as connection:
        cursor = connection.execute("DELETE FROM classes WHERE entry_id = ?", (entry_id,))
        return cursor.rowcount


def insert_recycle_record(record):
    with write_transaction() as connection:
        connection.execute(
            "INSERT INTO recycle_bin(recycle_id, deleted_at, entry_id, day, start_time, end_time, class_name, classroom_url, joined) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            _recycle_record_to_db_params(record),
        )


def delete_recycle_record(recycle_id):
    with write_transaction() as connection:
        cursor = connection.execute("DELETE FROM recycle_bin WHERE recycle_id = ?", (recycle_id,))
        return cursor.rowcount


def delete_all_recycle_records():
    with write_transaction() as connection:
        cursor = connection.execute("DELETE FROM recycle_bin")
        return cursor.rowcount


def move_entry_to_recycle(entry, recycle_id, deleted_at):
    with write_transaction() as connection:
        delete_cursor = connection.execute("DELETE FROM classes WHERE entry_id = ?", (entry["id"],))
        if delete_cursor.rowcount == 0:
            raise LookupError(f"No entry found with id '{entry['id']}'.")
        record = {"recycle_id": recycle_id, "deleted_at": deleted_at, "entry": entry}
        connection.execute(
            "INSERT INTO recycle_bin(recycle_id, deleted_at, entry_id, day, start_time, end_time, class_name, classroom_url, joined) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            _recycle_record_to_db_params(record),
        )


def restore_recycle_record(record):
    with write_transaction() as connection:
        delete_cursor = connection.execute("DELETE FROM recycle_bin WHERE recycle_id = ?", (record["recycle_id"],))
        if delete_cursor.rowcount == 0:
            raise LookupError(f"No recycle bin entry found with recycle_id '{record['recycle_id']}'.")
        connection.execute(
            "INSERT INTO classes(entry_id, day, start_time, end_time, class_name, classroom_url, joined) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            _entry_to_db_params(record["entry"]),
        )


def shutdown_database():
    _close_all_connections()
    log_info(
        LOGGER,
        "SQLite database manager shutdown completed.",
        what="Finished database manager shutdown work.",
        where=_where("shutdown_database"),
        why="Connections are short-lived per operation, so shutdown only needs to confirm normal completion.",
    )
