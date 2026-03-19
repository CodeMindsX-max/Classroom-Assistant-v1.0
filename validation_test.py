import io
import json
import os
import tempfile
import threading
import traceback
from contextlib import redirect_stdout
from unittest.mock import patch

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
    },
    {
        "id": "tue_1",
        "day": "Tuesday",
        "start_time": "09:00",
        "end_time": "10:00",
        "class_name": "Science",
        "classroom_url": "https://example.com/science",
    },
]


# This writes temporary JSON data to a test file
# so each validation scenario can run without touching real project data.
def write_json(path, data):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)
    tm.clear_storage_cache()


# This expects one specific failure path and prints a pass when it happens
# so negative tests prove validation is blocking bad input correctly.
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


# This checks two values are equal and prints a pass when they match
# so success-path tests can prove functions return the expected result.
def expect_equal(label, actual, expected):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")
    print(f"PASS: {label}")


# This captures printed output and returns both the function result and text
# so CLI display helpers can be tested without manual terminal checks.
def capture_output(func, *args, **kwargs):
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        result = func(*args, **kwargs)
    return result, buffer.getvalue()


# This feeds mock user input into one function and returns its result plus output
# so prompt behavior can be tested safely without typing during the run.
def run_with_inputs(func, inputs):
    buffer = io.StringIO()
    with patch("builtins.input", side_effect=inputs), redirect_stdout(buffer):
        result = func()
    return result, buffer.getvalue()


# This prints a short end-of-run story about the passing checks
# so the test output explains how the whole project is behaving correctly.
def print_validation_story():
    print("\nValidation story:")
    print("- The timetable manager accepted clean data and rejected broken data before saving it.")
    print("- Time validation proved that class slots must use HH:MM format and stay within the allowed duration.")
    print("- Add and edit validation proved that duplicate slots, bad days, and bad fields are blocked safely.")
    print("- Delete, recycle bin, restore, and permanent delete checks proved that recovery flows are working.")
    print("- Cache checks proved that repeated operations reuse loaded data instead of rereading the JSON files.")
    print("- Stored JSON corruption checks proved that invalid timetable or recycle-bin files are detected early.")
    print("- Main-file prompt checks proved that menu helpers guide the user, retry bad input, and respect cancel words.")
    print("- Because every test ran on temporary files, the real timetable and recycle bin stayed untouched.")


with tempfile.TemporaryDirectory() as temp_dir:
    timetable_path = os.path.join(temp_dir, "timetable.json")
    recycle_bin_path = os.path.join(temp_dir, "recycle_bin.json")
    write_json(timetable_path, BASE_TIMETABLE)
    write_json(recycle_bin_path, [])

    original_timetable_file = tm.TIMETABLE_FILE
    original_recycle_file = tm.RECYCLE_BIN_FILE
    tm.TIMETABLE_FILE = timetable_path
    tm.RECYCLE_BIN_FILE = recycle_bin_path

    try:
        loaded = tm.load_timetable()
        expect_equal("load_timetable valid data count", len(loaded), 2)

        recycle_loaded = tm.load_recycle_bin()
        expect_equal("load_recycle_bin empty data", recycle_loaded, [])

        expect_equal("validate_required_text valid", tm.validate_required_text(" Name ", "name"), "Name")
        expect_equal("validate_optional_text blank", tm.validate_optional_text("   "), "")
        expect_equal("validate_day_input valid", tm.validate_day_input("monday"), "Monday")
        expect_equal("validate_time_input valid", tm.validate_time_input("09:30", "start_time"), "09:30")
        expect_equal("validate_yes_no_input yes", tm.validate_yes_no_input("Yes"), True)
        expect_equal("validate_yes_no_input no", tm.validate_yes_no_input("n"), False)
        expect_equal("shutdown event initially clear", tm.get_shutdown_event().is_set(), False)

        start, end = tm.validate_time_range("10:00", "12:00")
        expect_equal("validate_time_range valid start", start, "10:00")
        expect_equal("validate_time_range valid end", end, "12:00")

        expect_exception(
            "validate_required_text empty",
            lambda: tm.validate_required_text("   ", "name"),
            ValueError,
            "cannot be empty",
        )

        expect_exception(
            "validate_optional_text invalid type",
            lambda: tm.validate_optional_text(None),
            ValueError,
            "must be a string",
        )

        expect_exception(
            "validate_yes_no_input invalid",
            lambda: tm.validate_yes_no_input("maybe"),
            ValueError,
            "Please enter y or n",
        )

        tm.clear_storage_cache()
        read_counts = {"timetable": 0, "recycle": 0}
        original_read_timetable_file = tm._read_timetable_file
        original_read_recycle_bin_file = tm._read_recycle_bin_file

        def counted_read_timetable_file():
            read_counts["timetable"] += 1
            return original_read_timetable_file()

        def counted_read_recycle_bin_file():
            read_counts["recycle"] += 1
            return original_read_recycle_bin_file()

        tm._read_timetable_file = counted_read_timetable_file
        tm._read_recycle_bin_file = counted_read_recycle_bin_file
        try:
            tm.initialize_storage()
            tm.get_entry_by_id("mon_1")
            tm.edit_entry("mon_1", class_name="Advanced Math")
            recycle_record = tm.delete_entry("tue_1")
            tm.restore_entry(recycle_record["recycle_id"])
            expect_equal("cache reads timetable only once", read_counts["timetable"], 1)
            expect_equal("cache reads recycle bin only once", read_counts["recycle"], 1)
        finally:
            tm._read_timetable_file = original_read_timetable_file
            tm._read_recycle_bin_file = original_read_recycle_bin_file

        write_json(timetable_path, BASE_TIMETABLE)
        write_json(recycle_bin_path, [])

        expect_exception(
            "validate_time_range same time",
            lambda: tm.validate_time_range("09:00", "09:00"),
            ValueError,
            "at least 1 hour",
        )

        expect_exception(
            "validate_time_range reverse time",
            lambda: tm.validate_time_range("10:00", "09:00"),
            ValueError,
            "at least 1 hour",
        )

        expect_exception(
            "validate_time_range invalid format",
            lambda: tm.validate_time_range("9am", "10:00"),
            ValueError,
            "HH:MM",
        )

        expect_exception(
            "validate_time_input requires zero padded hour",
            lambda: tm.validate_time_input("9:00", "start_time"),
            ValueError,
            "HH:MM",
        )

        entry = tm.add_entry(
            "Wednesday",
            "10:00",
            "11:00",
            "History",
            "https://example.com/history",
        )
        expect_equal("add_entry generated id", entry["id"], "wed_1")
        expect_equal("add_entry default joined false", entry["joined"], False)
        expect_equal("add_entry data count", len(tm.load_timetable()), 3)

        expect_exception(
            "add_entry duplicate slot",
            lambda: tm.add_entry(
                "Wednesday",
                "10:00",
                "11:00",
                "Duplicate History",
                "https://example.com/dup",
            ),
            ValueError,
            "already exists",
        )

        expect_exception(
            "add_entry invalid day",
            lambda: tm.add_entry(
                "Funday",
                "10:00",
                "11:00",
                "Bad Day",
                "https://example.com/bad",
            ),
            ValueError,
            "valid day name",
        )

        expect_exception(
            "add_entry empty class name",
            lambda: tm.add_entry(
                "Thursday",
                "10:00",
                "11:00",
                "   ",
                "https://example.com/blank",
            ),
            ValueError,
            "class_name cannot be empty",
        )

        expect_exception(
            "add_entry invalid url type",
            lambda: tm.add_entry(
                "Thursday",
                "10:00",
                "11:00",
                "Typing",
                123,
            ),
            ValueError,
            "classroom_url must be a string",
        )

        expect_exception(
            "add_entry invalid time gap short",
            lambda: tm.add_entry(
                "Thursday",
                "10:00",
                "10:30",
                "Short Gap",
                "https://example.com/short",
            ),
            ValueError,
            "at least 1 hour",
        )

        expect_exception(
            "add_entry invalid time gap long",
            lambda: tm.add_entry(
                "Thursday",
                "10:00",
                "14:30",
                "Long Gap",
                "https://example.com/long",
            ),
            ValueError,
            "not be more than 3 hours",
        )

        found = tm.get_entry_by_id("wed_1")
        expect_equal("get_entry_by_id valid", found["class_name"], "History")

        expect_exception(
            "get_entry_by_id invalid type",
            lambda: tm.get_entry_by_id(10),
            ValueError,
            "entry_id must be a string",
        )

        edited = tm.edit_entry("wed_1", class_name="World History", end_time="12:00")
        expect_equal("edit_entry updated class", edited["class_name"], "World History")
        expect_equal("edit_entry updated end time", edited["end_time"], "12:00")

        edited = tm.edit_entry("wed_1", url="https://example.com/new-history")
        expect_equal("edit_entry url alias", edited["classroom_url"], "https://example.com/new-history")

        joined_entry = tm.set_entry_joined_status("wed_1", True)
        expect_equal("set_entry_joined_status true", joined_entry["joined"], True)
        joined_entry = tm.set_entry_joined_status("wed_1", False)
        expect_equal("set_entry_joined_status false", joined_entry["joined"], False)

        expect_exception(
            "set_entry_joined_status invalid type",
            lambda: tm.set_entry_joined_status("wed_1", "yes"),
            ValueError,
            "joined must be a boolean",
        )

        expect_exception(
            "edit_entry invalid id",
            lambda: tm.edit_entry("abc_1", class_name="Nope"),
            LookupError,
            "No entry found",
        )

        expect_exception(
            "edit_entry no updates",
            lambda: tm.edit_entry("wed_1"),
            ValueError,
            "Provide at least one field",
        )

        expect_exception(
            "edit_entry duplicate slot",
            lambda: tm.edit_entry("wed_1", day="Tuesday", start_time="09:00", end_time="10:00"),
            ValueError,
            "already exists",
        )

        expect_exception(
            "edit_entry invalid field",
            lambda: tm.edit_entry("wed_1", teacher="Someone"),
            ValueError,
            "Invalid field",
        )

        expect_exception(
            "edit_entry url and classroom_url conflict",
            lambda: tm.edit_entry(
                "wed_1",
                url="https://example.com/a",
                classroom_url="https://example.com/b",
            ),
            ValueError,
            "Use either 'url' or 'classroom_url'",
        )

        recycle_record = tm.delete_entry("wed_1")
        expect_equal("delete_entry moved id", recycle_record["entry"]["id"], "wed_1")
        expect_equal("delete_entry recycle id", recycle_record["recycle_id"], "bin_1")
        expect_equal("delete_entry timetable count", len(tm.load_timetable()), 2)
        expect_equal("delete_entry recycle count", len(tm.load_recycle_bin()), 1)

        recycle_record_2 = tm.delete_entry("mon_1")
        expect_equal("delete_entry second recycle id", recycle_record_2["recycle_id"], "bin_2")

        restored_entry = tm.restore_entry(recycle_record["recycle_id"])
        expect_equal("restore_entry valid data", restored_entry["id"], "wed_1")
        expect_equal("restore_entry recycle count", len(tm.load_recycle_bin()), 1)

        deleted_record = tm.permanently_delete_recycle_entry(recycle_record_2["recycle_id"])
        expect_equal(
            "permanently_delete_recycle_entry valid data",
            deleted_record["recycle_id"],
            recycle_record_2["recycle_id"],
        )
        expect_equal("permanently_delete_recycle_entry recycle count", len(tm.load_recycle_bin()), 0)

        write_json(
            timetable_path,
            [
                {
                    "id": "mon_2",
                    "day": "Monday",
                    "start_time": "09:00",
                    "end_time": "10:00",
                    "class_name": "Physics",
                    "classroom_url": "https://example.com/physics",
                }
            ],
        )
        write_json(recycle_bin_path, [])
        reused_entry = tm.add_entry(
            "Monday",
            "10:00",
            "11:00",
            "Chemistry",
            "https://example.com/chemistry",
        )
        expect_equal("add_entry reuses first missing id", reused_entry["id"], "mon_1")

        expect_exception(
            "delete_entry invalid id type",
            lambda: tm.delete_entry(None),
            ValueError,
            "entry_id must be a string",
        )

        expect_exception(
            "delete_entry missing id",
            lambda: tm.delete_entry("abc_1"),
            LookupError,
            "No entry found",
        )

        expect_exception(
            "get_recycle_record_by_id validates recycle_id format",
            lambda: tm.get_recycle_record_by_id("wrong_id"),
            ValueError,
            "bin_<number>",
        )

        expect_exception(
            "restore_entry validates recycle_id format",
            lambda: tm.restore_entry("bin_x"),
            ValueError,
            "bin_<number>",
        )

        expect_exception(
            "permanently_delete_recycle_entry validates recycle_id format",
            lambda: tm.permanently_delete_recycle_entry("123"),
            ValueError,
            "bin_<number>",
        )

        expect_exception(
            "get_recycle_record_by_id missing recycle id",
            lambda: tm.get_recycle_record_by_id("bin_9"),
            LookupError,
            "No recycle bin entry found",
        )

        write_json(
            timetable_path,
            BASE_TIMETABLE
            + [
                {
                    "id": "dup_1",
                    "day": "Monday",
                    "start_time": "08:00",
                    "end_time": "09:00",
                    "class_name": "Duplicate",
                    "classroom_url": "https://example.com/dup",
                }
            ],
        )
        expect_exception(
            "load_timetable duplicate slot detection",
            tm.load_timetable,
            tm.DuplicateTimeSlotError,
        )

        duplicate_loaded = tm.load_timetable(allow_duplicate_slots=True)
        expect_equal("load_timetable allows duplicate slot cleanup mode", len(duplicate_loaded), 3)

        write_json(
            timetable_path,
            [
                BASE_TIMETABLE[0],
                dict(BASE_TIMETABLE[0]),
            ],
        )
        expect_exception(
            "load_timetable duplicate id detection",
            tm.load_timetable,
            ValueError,
            "Duplicate id",
        )

        write_json(
            timetable_path,
            [
                {
                    "id": "mon_1",
                    "day": "Monday",
                    "start_time": "9:00",
                    "end_time": "10:00",
                    "class_name": "Bad Time",
                    "classroom_url": "https://example.com/bad-time",
                }
            ],
        )
        issues = tm.inspect_timetable_entry_issues()
        expect_equal("inspect_timetable_entry_issues finds invalid row", len(issues), 1)
        if "HH:MM" not in issues[0]["error"]:
            raise AssertionError("inspect_timetable_entry_issues missing invalid time detail")
        print("PASS: inspect_timetable_entry_issues error detail")
        expect_equal(
            "inspect_timetable_entry_issues invalid field list",
            issues[0]["fields"],
            ["start_time", "end_time"],
        )

        repaired = tm.repair_timetable_entry(
            0,
            "mon_1",
            "Monday",
            "09:00",
            "10:00",
            "Fixed Time",
            "https://example.com/fixed-time",
        )
        expect_equal("repair_timetable_entry fixed id", repaired["id"], "mon_1")
        expect_equal("repair_timetable_entry fixed time", repaired["start_time"], "09:00")
        expect_equal("repair_timetable_entry makes file loadable", len(tm.load_timetable()), 1)

        write_json(
            timetable_path,
            [
                {
                    "id": "bad",
                    "day": "Monday",
                    "start_time": "9:00",
                    "end_time": "10:00",
                    "class_name": "Broken Id And Time",
                    "classroom_url": "https://example.com/broken-id",
                },
                {
                    "id": "mon_1",
                    "day": "Monday",
                    "start_time": "10:00",
                    "end_time": "11:00",
                    "class_name": "Existing Monday",
                    "classroom_url": "https://example.com/existing-monday",
                }
            ],
        )
        repaired = tm.repair_timetable_entry(
            0,
            "bad",
            "Monday",
            "09:00",
            "10:00",
            "Fixed Broken Id",
            "https://example.com/fixed-broken-id",
        )
        expect_equal("repair_timetable_entry auto-fixes invalid id", repaired["id"], "mon_2")

        write_json(
            timetable_path,
            [
                {
                    "id": "bad",
                    "day": "Funday",
                    "start_time": "09:00",
                    "end_time": "10:00",
                    "class_name": "Broken",
                    "classroom_url": "https://example.com/broken",
                }
            ],
        )
        deleted_raw_entry = tm.delete_raw_timetable_entry(0)
        expect_equal("delete_raw_timetable_entry returns removed raw id", deleted_raw_entry["id"], "bad")
        expect_equal("delete_raw_timetable_entry empties file", len(tm.load_timetable()), 0)

        write_json(
            timetable_path,
            [
                {
                    "id": "bad_1",
                    "day": "Monday",
                    "start_time": "08:00",
                    "end_time": "08:30",
                    "class_name": "Bad Gap",
                    "classroom_url": "https://example.com/bad",
                }
            ],
        )
        expect_exception(
            "load_timetable validates stored time gap",
            tm.load_timetable,
            ValueError,
            "at least 1 hour",
        )

        write_json(timetable_path, {"not": "a list"})
        expect_exception(
            "load_timetable requires list",
            tm.load_timetable,
            ValueError,
            "must contain a list",
        )

        with open(timetable_path, "w", encoding="utf-8") as file:
            file.write("{bad json")
        tm.clear_storage_cache()
        expect_exception(
            "load_timetable invalid json",
            tm.load_timetable,
            ValueError,
            "contains invalid JSON",
        )

        write_json(timetable_path, BASE_TIMETABLE)
        write_json(
            recycle_bin_path,
            [
                {
                    "recycle_id": "bin_1",
                    "deleted_at": "2026-03-18T10:00:00",
                    "entry": BASE_TIMETABLE[0],
                },
                {
                    "recycle_id": "bin_1",
                    "deleted_at": "2026-03-18T10:05:00",
                    "entry": BASE_TIMETABLE[1],
                },
            ],
        )
        expect_exception(
            "load_recycle_bin duplicate recycle ids",
            tm.load_recycle_bin,
            ValueError,
            "Duplicate recycle_id",
        )

        write_json(
            recycle_bin_path,
            [
                {
                    "recycle_id": "wrong",
                    "deleted_at": "2026-03-18T10:00:00",
                    "entry": BASE_TIMETABLE[0],
                }
            ],
        )
        expect_exception(
            "load_recycle_bin validates recycle id format",
            tm.load_recycle_bin,
            ValueError,
            "bin_<number>",
        )

        write_json(
            recycle_bin_path,
            [
                {
                    "recycle_id": "bin_2",
                    "deleted_at": "2026-03-18T10:00:00",
                    "entry": {
                        "id": "bad_2",
                        "day": "Funday",
                        "start_time": "08:00",
                        "end_time": "09:00",
                        "class_name": "Bad Entry",
                        "classroom_url": "https://example.com/bad",
                    },
                }
            ],
        )
        expect_exception(
            "load_recycle_bin validates embedded entry",
            tm.load_recycle_bin,
            ValueError,
            "valid day name",
        )

        write_json(recycle_bin_path, {"not": "a list"})
        expect_exception(
            "load_recycle_bin requires list",
            tm.load_recycle_bin,
            ValueError,
            "must contain a list",
        )

        write_json(timetable_path, BASE_TIMETABLE)
        write_json(
            recycle_bin_path,
            [
                {
                    "recycle_id": "bin_3",
                    "deleted_at": "2026-03-18T10:00:00",
                    "entry": {
                        "id": "tue_1",
                        "day": "Thursday",
                        "start_time": "11:00",
                        "end_time": "12:00",
                        "class_name": "Clash ID",
                        "classroom_url": "https://example.com/clash-id",
                    },
                }
            ],
        )
        expect_exception(
            "restore_entry blocks duplicate id",
            lambda: tm.restore_entry("bin_3"),
            ValueError,
            "already exists",
        )

        write_json(
            recycle_bin_path,
            [
                {
                    "recycle_id": "bin_4",
                    "deleted_at": "2026-03-18T10:00:00",
                    "entry": {
                        "id": "thu_1",
                        "day": "Tuesday",
                        "start_time": "09:00",
                        "end_time": "10:00",
                        "class_name": "Clash Slot",
                        "classroom_url": "https://example.com/clash-slot",
                    },
                }
            ],
        )
        expect_exception(
            "restore_entry blocks duplicate slot",
            lambda: tm.restore_entry("bin_4"),
            ValueError,
            "already exists",
        )

        write_json(timetable_path, BASE_TIMETABLE)
        write_json(recycle_bin_path, [])

        result, output = capture_output(main.show_recycle_bin)
        expect_equal("main show_recycle_bin empty return", result, False)
        if "Recycle bin is empty." not in output:
            raise AssertionError("main show_recycle_bin empty output missing expected message")
        print("PASS: main show_recycle_bin empty output")

        write_json(
            recycle_bin_path,
            [
                {
                    "recycle_id": "bin_5",
                    "deleted_at": "2026-03-18T10:00:00",
                    "entry": {
                        "id": "wed_1",
                        "day": "Wednesday",
                        "start_time": "10:00",
                        "end_time": "11:00",
                        "class_name": "History",
                        "classroom_url": "https://example.com/history",
                    },
                }
            ],
        )

        result, output = capture_output(main.show_recycle_bin)
        expect_equal("main show_recycle_bin non-empty return", result, True)
        if "Recycle ID" not in output or "bin_5" not in output:
            raise AssertionError("main show_recycle_bin table output missing expected values")
        print("PASS: main show_recycle_bin table output")

        result, output = capture_output(main.show_timetable)
        if "ID" not in output or "mon_1" not in output or "tue_1" not in output:
            raise AssertionError("main show_timetable table output missing expected values")
        print("PASS: main show_timetable table output")

        duplicate_error = tm.DuplicateTimeSlotError(
            [[
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
                    "id": "mon_2",
                    "day": "Monday",
                    "start_time": "08:00",
                    "end_time": "09:00",
                    "class_name": "Science",
                    "classroom_url": "https://example.com/science",
                    "joined": False,
                },
            ]]
        )
        with patch("main.prompt_yes_no", side_effect=[False, True]), patch(
            "main.prompt_required", return_value="mon_1"
        ), patch("main.delete_entry") as mocked_delete_entry:
            result, output = capture_output(main.resolve_duplicate_slots, duplicate_error)
        expect_equal("main resolve_duplicate_slots loops on no", result, True)
        if "This data must be repaired before the program can continue." not in output:
            raise AssertionError("main resolve_duplicate_slots missing blocking message")
        if mocked_delete_entry.call_count != 1:
            raise AssertionError("main resolve_duplicate_slots did not delete duplicate after retry")
        print("PASS: main resolve_duplicate_slots retry flow")

        invalid_error = tm.InvalidTimetableEntriesError(
            [
                {
                    "index": 0,
                    "entry": {
                        "id": "mon_1",
                        "day": "Monday",
                        "start_time": "9:00",
                        "end_time": "10:00",
                        "class_name": "Math",
                        "classroom_url": "https://example.com/math",
                    },
                    "error": "start_time must be in HH:MM format.",
                    "fields": ["start_time", "end_time"],
                }
            ]
        )
        with patch("main.prompt_yes_no", side_effect=[False, True]), patch(
            "main.prompt_required", side_effect=["edit"]
        ), patch("main.prompt_time", side_effect=["09:00", "10:00"]), patch(
            "main.repair_timetable_entry",
            return_value={
                "id": "mon_1",
                "day": "Monday",
                "start_time": "09:00",
                "end_time": "10:00",
                "class_name": "Math",
                "classroom_url": "https://example.com/math",
                "joined": False,
            },
        ), patch("main.initialize_storage", return_value=None):
            result, output = capture_output(main.resolve_invalid_timetable_entries, invalid_error)
        expect_equal("main resolve_invalid_timetable_entries loops on no", result, True)
        if "This data must be repaired before the program can continue." not in output:
            raise AssertionError("main resolve_invalid_timetable_entries missing blocking message")
        print("PASS: main resolve_invalid_timetable_entries retry flow")

        write_json(timetable_path, BASE_TIMETABLE)
        write_json(recycle_bin_path, [])
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

        value, output = run_with_inputs(lambda: main.prompt_yes_no("Confirm"), ["maybe", "y"])
        expect_equal("main prompt_yes_no retries invalid input", value, True)
        if "Please enter y or n." not in output:
            raise AssertionError("main prompt_yes_no invalid message missing")
        print("PASS: main prompt_yes_no invalid message")

        print_validation_story()
        print("\nALL TESTS PASSED")
    except Exception:
        traceback.print_exc()
        raise
    finally:
        tm.clear_storage_cache()
        tm.TIMETABLE_FILE = original_timetable_file
        tm.RECYCLE_BIN_FILE = original_recycle_file
