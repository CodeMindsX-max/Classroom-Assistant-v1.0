import os
import random
import tempfile
import threading
import traceback

import app_logger_manager as alm
import database_manager as dbm
import timetable_manager as tm


SEED = 20260320
SEQUENTIAL_ITERATIONS = 600
CONCURRENT_THREADS = 4
CONCURRENT_ITERATIONS = 250

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

VALID_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
EXPECTED_EXCEPTIONS = (tm.TimetableError, ValueError, LookupError)


def reset_database():
    dbm.reset_migration_state()
    tm.clear_storage_cache()
    dbm.initialize_database()
    dbm.replace_all_data(BASE_TIMETABLE, [])
    tm.clear_storage_cache()


def assert_invariants():
    timetable = tm.load_timetable()
    recycle_bin = tm.load_recycle_bin()
    ids = [entry["id"] for entry in timetable]
    recycle_ids = [record["recycle_id"] for record in recycle_bin]

    if len(ids) != len(set(ids)):
        raise AssertionError("Duplicate active entry ids detected after fuzz run.")
    if len(recycle_ids) != len(set(recycle_ids)):
        raise AssertionError("Duplicate recycle ids detected after fuzz run.")

    active_ids = set(ids)
    for record in recycle_bin:
        if record["entry"]["id"] in active_ids:
            continue


def random_time_block(rng):
    start_hour = rng.randint(6, 20)
    start_minute = rng.choice([0, 30])
    duration_hours = rng.randint(1, 3)
    end_hour = start_hour + duration_hours
    if end_hour > 23:
        end_hour = 23
        start_hour = end_hour - duration_hours
    return f"{start_hour:02d}:{start_minute:02d}", f"{end_hour:02d}:{start_minute:02d}"


def random_invalid_time(rng):
    return rng.choice(["", "9:00", "25:00", "ab:cd", "99:99", None, 123])


def random_invalid_day(rng):
    return rng.choice(["Funday", "", None, 123, "Mon"])


def random_name(rng):
    return f"Class {rng.randint(1, 999)}"


def random_url(rng):
    return f"https://example.com/class/{rng.randint(1, 9999)}"


def choose_entry_id(rng):
    timetable = tm.load_timetable()
    if not timetable:
        return rng.choice(["mon_1", "bad", "", None])
    return rng.choice([entry["id"] for entry in timetable] + ["bad", "", None])


def choose_recycle_id(rng):
    recycle_bin = tm.load_recycle_bin()
    if not recycle_bin:
        return rng.choice(["bin_1", "wrong", "", None])
    return rng.choice([record["recycle_id"] for record in recycle_bin] + ["wrong", "", None])


def run_random_operation(rng):
    operation = rng.choice(
        [
            "load",
            "add_validish",
            "add_invalidish",
            "edit",
            "delete",
            "restore",
            "joined",
            "clear_recycle",
        ]
    )

    if operation == "load":
        tm.load_timetable()
        tm.load_recycle_bin()
        return

    if operation == "add_validish":
        day = rng.choice(VALID_DAYS)
        start_time, end_time = random_time_block(rng)
        tm.add_entry(day, start_time, end_time, random_name(rng), random_url(rng))
        return

    if operation == "add_invalidish":
        day = rng.choice([rng.choice(VALID_DAYS), random_invalid_day(rng)])
        start_time = rng.choice([random_time_block(rng)[0], random_invalid_time(rng)])
        end_time = rng.choice([random_time_block(rng)[1], random_invalid_time(rng)])
        class_name = rng.choice([random_name(rng), "", None, 123])
        url = rng.choice([random_url(rng), "", None, 123])
        tm.add_entry(day, start_time, end_time, class_name, url)
        return

    if operation == "edit":
        entry_id = choose_entry_id(rng)
        updates = {}
        if rng.random() < 0.5:
            updates["day"] = rng.choice(VALID_DAYS + [random_invalid_day(rng)])
        if rng.random() < 0.5:
            updates["start_time"] = rng.choice([random_time_block(rng)[0], random_invalid_time(rng)])
        if rng.random() < 0.5:
            updates["end_time"] = rng.choice([random_time_block(rng)[1], random_invalid_time(rng)])
        if rng.random() < 0.5:
            updates["class_name"] = rng.choice([random_name(rng), "", None, 123])
        if rng.random() < 0.5:
            updates["classroom_url"] = rng.choice([random_url(rng), "", None, 123])
        if not updates:
            updates["class_name"] = random_name(rng)
        tm.edit_entry(entry_id, **updates)
        return

    if operation == "delete":
        tm.delete_entry(choose_entry_id(rng))
        return

    if operation == "restore":
        tm.restore_entry(choose_recycle_id(rng))
        return

    if operation == "joined":
        tm.set_entry_joined_status(choose_entry_id(rng), rng.choice([True, False, "yes", None]))
        return

    if operation == "clear_recycle":
        tm.clear_recycle_bin()


def run_sequential_fuzz(rng):
    handled_exceptions = 0
    for _ in range(SEQUENTIAL_ITERATIONS):
        try:
            run_random_operation(rng)
        except EXPECTED_EXCEPTIONS:
            handled_exceptions += 1
        assert_invariants()
    return handled_exceptions


def run_concurrent_fuzz():
    thread_errors = []
    handled_exceptions = []

    def worker(seed_offset):
        worker_rng = random.Random(SEED + seed_offset)
        local_handled = 0
        try:
            for _ in range(CONCURRENT_ITERATIONS):
                try:
                    run_random_operation(worker_rng)
                except EXPECTED_EXCEPTIONS:
                    local_handled += 1
        except Exception as exc:
            thread_errors.append(exc)
        finally:
            handled_exceptions.append(local_handled)

    threads = [threading.Thread(target=worker, args=(index + 1,)) for index in range(CONCURRENT_THREADS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if thread_errors:
        raise AssertionError(f"Concurrent fuzz test failed: {thread_errors[0]}")

    assert_invariants()
    return sum(handled_exceptions)


with tempfile.TemporaryDirectory() as temp_dir:
    database_path = os.path.join(temp_dir, "classroom_assistant.db")
    log_dir = os.path.join(temp_dir, "logs")
    original_database_file = dbm.DATABASE_FILE
    dbm.set_database_path(database_path)
    alm.configure_logging(log_dir=log_dir, force=True)

    try:
        reset_database()
        tm.initialize_storage()

        rng = random.Random(SEED)
        sequential_handled = run_sequential_fuzz(rng)
        print(f"PASS: sequential fuzz test handled {sequential_handled} expected exceptions safely")

        reset_database()
        tm.initialize_storage()
        concurrent_handled = run_concurrent_fuzz()
        print(f"PASS: concurrent fuzz test handled {concurrent_handled} expected exceptions safely")

        print("PASS: fuzz and stress invariants remained valid")
        print("\nFUZZ STRESS TESTS PASSED")
    except Exception:
        traceback.print_exc()
        raise
    finally:
        alm.shutdown_logging()
        tm.shutdown_storage()
        tm.clear_storage_cache()
        dbm.set_database_path(original_database_file)
