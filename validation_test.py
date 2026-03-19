import io
import json
import os
import sqlite3
import tempfile
import threading
import traceback
from contextlib import redirect_stdout
from unittest.mock import patch

import app_logger_manager as alm
import database_manager as dbm
import main
import timetable_manager as tm


BASE_TIMETABLE = [
    {
        "id": "mon_1",
        "day": "Monday",
        "start_time": "08:00",
        "end_time": "09:00",
        "class_name": "Math",
        "classroom_url": "https://example.com/math",
        "joined": False,
    },
    {
        "id": "tue_1",
        "day": "Tuesday",
        "start_time": "09:00",
        "end_time": "10:00",
        "class_name": "Science",
        "classroom_url": "https://example.com/science",
        "joined": False,
    },
]


def reset_database(classes=None, recycle_bin=None):
    dbm.reset_migration_state()
    tm.clear_storage_cache()
    dbm.initialize_database()
    dbm.replace_all_data(classes or [], recycle_bin or [])
    tm.clear_storage_cache()


def expect_exception(label, func, expected_exception, expected_text=None):
    try:
        func()
    except expected_exception as exc:
        if expected_text and expected_text not in str(exc):
            raise AssertionError(
                f"{label}: expected error containing {expected_text!r}, got {str(exc)!r}"
            )
        print(f"PASS: {label}")
        return
    except Exception as exc:
        raise AssertionError(
            f"{label}: expected {expected_exception.__name__}, got {type(exc).__name__}: {exc}"
        ) from exc
    raise AssertionError(f"{label}: expected {expected_exception.__name__} but no exception was raised")


def expect_equal(label, actual, expected):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")
    print(f"PASS: {label}")


def capture_output(func, *args, **kwargs):
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        result = func(*args, **kwargs)
    return result, buffer.getvalue()


def run_with_inputs(func, inputs):
    buffer = io.StringIO()
    with patch("builtins.input", side_effect=inputs), redirect_stdout(buffer):
        result = func()
    return result, buffer.getvalue()


def print_validation_story():
    print("\nValidation story:")
    print("- SQLite storage accepted clean data and returned consistent rows for timetable and recycle-bin operations.")
    print("- Validation proved that class slots must use HH:MM format, stay within one to three hours, and never overlap.")
    print("- CRUD and recycle-bin tests proved that add, edit, delete, restore, and permanent delete flows still work safely.")
    print("- Repair tests proved that malformed SQLite rows can be inspected, edited, or removed without crashing startup.")
    print("- Schema checks proved that the database itself blocks exact duplicate timetable slots.")
    print("- Structured logging checks proved that JSON logs are written safely and sensitive URL fields are redacted.")
    print("- Main-file prompt checks proved that menu helpers still guide the user, retry bad input, and respect cancel words.")
    print("- Concurrency checks proved that repeated reads and updates still behave safely under threaded access.")


with tempfile.TemporaryDirectory() as temp_dir:
    database_path = os.path.join(temp_dir, "classroom_assistant.db")
    log_dir = os.path.join(temp_dir, "logs")
    original_database_file = dbm.DATABASE_FILE
    dbm.set_database_path(database_path)
    alm.configure_logging(log_dir=log_dir, force=True)

    try:
        reset_database(BASE_TIMETABLE, [])

        loaded = tm.load_timetable()
        expect_equal("load_timetable valid data count", len(loaded), 2)
        expect_equal("sqlite database file created", os.path.exists(database_path), True)
        expect_equal("load_recycle_bin empty data", tm.load_recycle_bin(), [])

        expect_equal("validate_required_text valid", tm.validate_required_text(" Name ", "name"), "Name")
        expect_equal("validate_optional_text blank", tm.validate_optional_text("   "), "")
        expect_equal("validate_day_input valid", tm.validate_day_input("monday"), "Monday")
        expect_equal("validate_time_input valid", tm.validate_time_input("09:30", "start_time"), "09:30")
        expect_equal("validate_time_input max boundary", tm.validate_time_input("23:59", "end_time"), "23:59")
        expect_equal("validate_yes_no_input yes", tm.validate_yes_no_input("Yes"), True)
        expect_equal("validate_yes_no_input no", tm.validate_yes_no_input("n"), False)
        expect_equal("shutdown event initially clear", tm.get_shutdown_event().is_set(), False)
        expect_equal("database starts with no cached connections", dbm.get_open_connection_count(), 1)

        test_logger = alm.get_app_logger("validation_test")
        alm.log_event(
            test_logger,
            "Structured logging smoke test.",
            what="Recorded a JSON log event for validation testing.",
            where="validation_test",
            why="The suite is proving that structured logging is configured correctly.",
            context={"entry_id": "mon_1", "classroom_url": "https://secret.example.com/class"},
        )
        alm.flush_logging()
        log_path = alm.get_log_file_path()
        if not os.path.exists(log_path):
            raise AssertionError("structured logger did not create a log file")
        print("PASS: logger created log file")

        with open(log_path, "r", encoding="utf-8") as log_file:
            log_lines = [line.strip() for line in log_file if line.strip()]

        latest_log = json.loads(log_lines[-1])
        for required_key in ["timestamp", "when", "where", "what", "why", "severity", "level"]:
            if required_key not in latest_log:
                raise AssertionError(f"structured logger missing key: {required_key}")
        print("PASS: logger writes required JSON fields")

        expect_equal("logger uses custom EVENT level", latest_log["level"], "EVENT")
        expect_equal("logger redacts sensitive context", latest_log["context"]["classroom_url"], "[REDACTED]")
        if "https://secret.example.com/class" in "\n".join(log_lines):
            raise AssertionError("structured logger leaked a sensitive URL into the log file")
        print("PASS: logger keeps URLs redacted")

        old_rotated_log = os.path.join(log_dir, "classroom_assistant.log.2000-01-01")
        recent_rotated_log = os.path.join(log_dir, "classroom_assistant.log.2099-01-01")
        active_log = os.path.join(log_dir, "classroom_assistant.log")
        with open(old_rotated_log, "w", encoding="utf-8") as file:
            file.write("old log")
        with open(recent_rotated_log, "w", encoding="utf-8") as file:
            file.write("recent log")
        with open(active_log, "a", encoding="utf-8") as file:
            file.write("")
        alm.configure_logging(log_dir=log_dir, force=True)
        expect_equal("logger removes old rotated logs", os.path.exists(old_rotated_log), False)
        expect_equal("logger keeps recent rotated logs", os.path.exists(recent_rotated_log), True)
        expect_equal("logger keeps active log file", os.path.exists(active_log), True)

        start, end = tm.validate_time_range("10:00", "12:00")
        expect_equal("validate_time_range valid start", start, "10:00")
        expect_equal("validate_time_range valid end", end, "12:00")

        expect_exception("validate_required_text empty", lambda: tm.validate_required_text("   ", "name"), ValueError, "cannot be empty")
        expect_exception("validate_optional_text invalid type", lambda: tm.validate_optional_text(None), ValueError, "must be a string")
        expect_exception("validate_yes_no_input invalid", lambda: tm.validate_yes_no_input("maybe"), ValueError, "Please enter y or n")
        expect_exception("validate_time_range same time", lambda: tm.validate_time_range("09:00", "09:00"), ValueError, "at least 1 hour")
        expect_exception("validate_time_range reverse time", lambda: tm.validate_time_range("10:00", "09:00"), ValueError, "at least 1 hour")
        expect_exception("validate_time_input requires zero padded hour", lambda: tm.validate_time_input("9:00", "start_time"), ValueError, "HH:MM")

        saved_timetable = tm.save_timetable(BASE_TIMETABLE)
        expect_equal("save_timetable roundtrip count", len(saved_timetable), 2)
        expect_equal("save_timetable persisted count", len(tm.load_timetable()), 2)
        expect_equal("same-thread database connection reuse", dbm.get_open_connection_count(), 1)

        entry = tm.add_entry("Wednesday", "10:00", "11:00", "History", "https://example.com/history")
        expect_equal("add_entry generated id", entry["id"], "wed_1")
        expect_equal("add_entry default joined false", entry["joined"], False)
        expect_equal("add_entry data count", len(tm.load_timetable()), 3)

        expect_exception(
            "add_entry duplicate slot",
            lambda: tm.add_entry("Wednesday", "10:00", "11:00", "Duplicate History", "https://example.com/dup"),
            ValueError,
            "already exists",
        )
        expect_exception(
            "add_entry overlapping slot",
            lambda: tm.add_entry("Monday", "08:30", "09:30", "Overlap Math", "https://example.com/overlap"),
            ValueError,
            "overlaps",
        )

        edited = tm.edit_entry("wed_1", class_name="World History", end_time="12:00")
        expect_equal("edit_entry updated class", edited["class_name"], "World History")
        expect_equal("edit_entry updated end time", edited["end_time"], "12:00")
        expect_exception(
            "edit_entry overlapping slot",
            lambda: tm.edit_entry("wed_1", day="Monday", start_time="08:30", end_time="09:30"),
            ValueError,
            "overlaps",
        )

        joined_entry = tm.set_entry_joined_status("wed_1", True)
        expect_equal("set_entry_joined_status true", joined_entry["joined"], True)
        expect_exception(
            "set_entry_joined_status invalid type",
            lambda: tm.set_entry_joined_status("wed_1", "yes"),
            ValueError,
            "joined must be a boolean",
        )

        recycle_record = tm.delete_entry("wed_1")
        expect_equal("delete_entry moved id", recycle_record["entry"]["id"], "wed_1")
        expect_equal("delete_entry recycle count", len(tm.load_recycle_bin()), 1)
        restored_entry = tm.restore_entry(recycle_record["recycle_id"])
        expect_equal("restore_entry valid data", restored_entry["id"], "wed_1")

        recycle_record = tm.delete_entry("wed_1")
        deleted_record = tm.permanently_delete_recycle_entry(recycle_record["recycle_id"])
        expect_equal("permanently_delete_recycle_entry valid data", deleted_record["recycle_id"], recycle_record["recycle_id"])

        recycle_record_a = tm.delete_entry("mon_1")
        recycle_record_b = tm.delete_entry("tue_1")
        cleared_records = tm.clear_recycle_bin()
        expect_equal("clear_recycle_bin deleted count", len(cleared_records), 2)
        expect_equal("clear_recycle_bin empties recycle bin", len(tm.load_recycle_bin()), 0)
        expect_equal(
            "clear_recycle_bin returns deleted ids",
            sorted(record["entry"]["id"] for record in cleared_records),
            sorted([recycle_record_a["entry"]["id"], recycle_record_b["entry"]["id"]]),
        )
        expect_exception(
            "clear_recycle_bin empty recycle bin",
            tm.clear_recycle_bin,
            LookupError,
            "Recycle bin is empty",
        )

        saved_recycle_bin = tm.save_recycle_bin(
            [
                {
                    "recycle_id": "bin_1",
                    "deleted_at": "2026-03-18T10:00:00",
                    "entry": BASE_TIMETABLE[0],
                }
            ]
        )
        expect_equal("save_recycle_bin roundtrip count", len(saved_recycle_bin), 1)
        expect_equal("save_recycle_bin persisted count", len(tm.load_recycle_bin()), 1)

        reset_database(BASE_TIMETABLE, [])

        reset_database(
            [
                {
                    "id": "mon_2",
                    "day": "Monday",
                    "start_time": "09:00",
                    "end_time": "10:00",
                    "class_name": "Physics",
                    "classroom_url": "https://example.com/physics",
                    "joined": False,
                }
            ],
            [],
        )
        reused_entry = tm.add_entry("Monday", "10:00", "11:00", "Chemistry", "https://example.com/chemistry")
        expect_equal("add_entry reuses first missing id", reused_entry["id"], "mon_1")

        with dbm.write_transaction() as connection:
            connection.execute(
                "INSERT INTO classes(entry_id, day, start_time, end_time, class_name, classroom_url, joined) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("mon_9", "Monday", "11:00", "12:00", "Extra", "https://example.com/extra", 0),
            )
            try:
                connection.execute(
                    "INSERT INTO classes(entry_id, day, start_time, end_time, class_name, classroom_url, joined) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    ("dup_1", "Monday", "11:00", "12:00", "Duplicate", "https://example.com/duplicate", 0),
                )
            except sqlite3.IntegrityError:
                pass
            else:
                raise AssertionError("database did not block an exact duplicate slot")
        print("PASS: sqlite schema blocks duplicate slot")

        reset_database(BASE_TIMETABLE, [])
        with dbm.write_transaction() as connection:
            connection.execute("UPDATE classes SET start_time = ? WHERE entry_id = ?", ("9:00", "mon_1"))
        expect_exception("initialize_storage detects invalid stored data", tm.initialize_storage, tm.InvalidTimetableEntriesError, "Invalid timetable entries")
        issues = tm.inspect_timetable_entry_issues()
        expect_equal("inspect_timetable_entry_issues finds invalid row", len(issues), 1)
        expect_equal("inspect_timetable_entry_issues invalid field list", issues[0]["fields"], ["start_time", "end_time"])
        repaired = tm.repair_timetable_entry(0, "mon_1", "Monday", "09:00", "10:00", "Math", "https://example.com/math")
        expect_equal("repair_timetable_entry fixed id", repaired["id"], "mon_1")
        expect_equal("repair_timetable_entry makes database loadable", len(tm.load_timetable()), 2)

        reset_database(BASE_TIMETABLE, [])
        with dbm.write_transaction() as connection:
            connection.execute("UPDATE classes SET day = ? WHERE entry_id = ?", ("Funday", "mon_1"))
        deleted_raw_entry = tm.delete_raw_timetable_entry(0)
        expect_equal("delete_raw_timetable_entry returns removed raw id", deleted_raw_entry["id"], "mon_1")
        expect_equal("delete_raw_timetable_entry removes broken row", len(tm.load_timetable()), 1)

        reset_database(BASE_TIMETABLE, [])
        with dbm.write_transaction() as connection:
            connection.execute(
                "INSERT INTO classes(entry_id, day, start_time, end_time, class_name, classroom_url, joined) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("mon_2", "Monday", "08:30", "09:30", "Overlap", "https://example.com/overlap", 0),
            )
        expect_exception("initialize_storage detects overlapping stored data", tm.initialize_storage, tm.InvalidTimetableEntriesError, "overlaps")
        overlap_issues = tm.inspect_timetable_entry_issues()
        expect_equal("inspect_timetable_entry_issues overlap field list", overlap_issues[0]["fields"], ["start_time", "end_time"])

        reset_database(BASE_TIMETABLE, [])
        recycle_record = tm.delete_entry("mon_1")
        with dbm.write_transaction() as connection:
            connection.execute("UPDATE recycle_bin SET recycle_id = ? WHERE recycle_id = ?", ("wrong", recycle_record["recycle_id"]))
        expect_exception("load_recycle_bin validates recycle id format", tm.load_recycle_bin, ValueError, "bin_<number>")

        reset_database(BASE_TIMETABLE, [])
        with dbm.write_transaction() as connection:
            connection.execute(
                "INSERT INTO recycle_bin(recycle_id, deleted_at, entry_id, day, start_time, end_time, class_name, classroom_url, joined) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("bin_9", "2026-03-18T10:00:00", "thu_1", "Tuesday", "09:30", "10:30", "Overlap Slot", "https://example.com/overlap-slot", 0),
            )
        expect_exception("restore_entry blocks overlapping slot", lambda: tm.restore_entry("bin_9"), ValueError, "overlaps")

        reset_database(BASE_TIMETABLE, [])
        result, output = capture_output(main.show_recycle_bin)
        expect_equal("main show_recycle_bin empty return", result, False)
        if "Recycle bin is empty." not in output:
            raise AssertionError("main show_recycle_bin empty output missing expected message")
        print("PASS: main show_recycle_bin empty output")

        recycle_record = tm.delete_entry("mon_1")
        result, output = capture_output(main.show_recycle_bin)
        expect_equal("main show_recycle_bin non-empty return", result, True)
        if recycle_record["recycle_id"] not in output:
            raise AssertionError("main show_recycle_bin table output missing expected values")
        print("PASS: main show_recycle_bin table output")

        with patch("main.show_recycle_bin", return_value=True), patch(
            "main.prompt_yes_no", return_value=True
        ), patch("main.clear_recycle_bin", return_value=[{"recycle_id": "bin_1"}, {"recycle_id": "bin_2"}]):
            _, output = run_with_inputs(main.handle_recycle_bin, ["4", "5"])
        if "Permanently deleted all recycle bin entries: 2 record(s)" not in output:
            raise AssertionError("main handle_recycle_bin clear output missing expected summary")
        print("PASS: main handle_recycle_bin clear output")

        reset_database(BASE_TIMETABLE, [])
        with patch("main.shutdown_storage"), patch("main.shutdown_logging"):
            _, output = run_with_inputs(
                main.main,
                [
                    "2",
                    "Wednesday",
                    "10:00",
                    "11:00",
                    "History",
                    "https://example.com/history",
                    "4",
                    "wed_1",
                    "y",
                    "5",
                    "4",
                    "y",
                    "5",
                    "6",
                ],
            )
        if "Entry added successfully: wed_1" not in output:
            raise AssertionError("main integration flow did not add the expected entry")
        if "Entry moved to recycle bin successfully: wed_1 -> bin_1" not in output:
            raise AssertionError("main integration flow did not delete the expected entry")
        if "Permanently deleted all recycle bin entries: 1 record(s)" not in output:
            raise AssertionError("main integration flow did not clear the recycle bin")
        if "Goodbye." not in output:
            raise AssertionError("main integration flow did not exit cleanly")
        print("PASS: main full integration flow")

        result, output = capture_output(main.show_timetable)
        if "ID" not in output or "tue_1" not in output:
            raise AssertionError("main show_timetable table output missing expected values")
        print("PASS: main show_timetable table output")

        value, _ = run_with_inputs(lambda: main.prompt_day("Day"), ["monday"])
        expect_equal("main prompt_day normalizes case", value, "Monday")
        value, output = run_with_inputs(lambda: main.prompt_day("Day"), ["funday", "tuesday"])
        expect_equal("main prompt_day retries invalid input", value, "Tuesday")
        if "day must be a valid day name" not in output:
            raise AssertionError("main prompt_day invalid message missing")
        print("PASS: main prompt_day invalid message")

        value, output = run_with_inputs(lambda: main.prompt_time("Time"), ["wrong", "09:30"])
        expect_equal("main prompt_time retries invalid input", value, "09:30")
        if "must be in HH:MM format" not in output:
            raise AssertionError("main prompt_time invalid message missing")
        print("PASS: main prompt_time invalid message")

        expect_exception(
            "main prompt_required cancel keyword",
            lambda: run_with_inputs(lambda: main.prompt_required("Field"), ["back"]),
            main.UserCancelledOperation,
            "Action cancelled",
        )
        expect_exception(
            "main prompt_yes_no cancel keyword",
            lambda: run_with_inputs(lambda: main.prompt_yes_no("Confirm"), ["menu"]),
            main.UserCancelledOperation,
            "Action cancelled",
        )

        reset_database(BASE_TIMETABLE, [])
        tm.initialize_storage()
        thread_errors = []

        def reader_worker():
            try:
                for _ in range(100):
                    tm.load_timetable()
                    tm.get_entry_by_id("mon_1")
            except Exception as exc:
                thread_errors.append(exc)

        def joined_worker():
            try:
                for index in range(100):
                    tm.set_entry_joined_status("mon_1", index % 2 == 0)
            except Exception as exc:
                thread_errors.append(exc)

        def editor_worker():
            try:
                for index in range(30):
                    tm.edit_entry("tue_1", class_name=f"Science {index}")
            except Exception as exc:
                thread_errors.append(exc)

        threads = [
            threading.Thread(target=reader_worker),
            threading.Thread(target=joined_worker),
            threading.Thread(target=editor_worker),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        if thread_errors:
            raise AssertionError(f"Concurrency stress test failed: {thread_errors[0]}")
        print("PASS: concurrency stress test")
        expect_equal("thread-local database connections created", dbm.get_open_connection_count(), 4)

        print_validation_story()
        print("\nALL TESTS PASSED")
    except Exception:
        traceback.print_exc()
        raise
    finally:
        alm.shutdown_logging()
        tm.clear_storage_cache()
        dbm.set_database_path(original_database_file)
