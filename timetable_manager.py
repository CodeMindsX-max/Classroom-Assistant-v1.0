import copy
import re
import sqlite3
import threading
from datetime import datetime

import database_manager as db
from app_logger_manager import get_app_logger, log_error, log_event, log_info, log_warning

VALID_FIELDS = {"day", "start_time", "end_time", "class_name", "classroom_url"}
RECYCLE_ID_PATTERN = re.compile(r"^bin_\d+$")
ENTRY_ID_PATTERN = re.compile(r"^[a-z]{3}_\d+$")
TIME_PATTERN = re.compile(r"^\d{2}:\d{2}$")
VALID_DAYS = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
VALID_YES_VALUES = {"y", "yes"}
VALID_NO_VALUES = {"n", "no"}


class TimetableError(Exception): ...
class TimetableValidationError(TimetableError, ValueError): ...
class TimetableStorageError(TimetableError, OSError): ...
class TimetableNotFoundError(TimetableError, LookupError): ...
class TimetableConflictError(TimetableError, ValueError): ...


class DuplicateTimeSlotError(TimetableConflictError):
    def __init__(self, duplicates):
        self.duplicates = duplicates
        super().__init__("Duplicate timetable slots found in stored data.")


class InvalidTimetableEntriesError(TimetableValidationError):
    def __init__(self, issues):
        self.issues = issues
        first = issues[0]["error"] if issues else "Unknown validation error."
        super().__init__(
            f"Invalid timetable entries found in stored data ({len(issues)} issue(s)). First issue: {first}"
        )


STORAGE_LOCK = threading.RLock()
SHUTDOWN_EVENT = threading.Event()
DATABASE_READY = False
LOGGER = get_app_logger("timetable_manager")


def _where(function_name):
    return f"timetable_manager.{function_name}"


def _safe_entry_context(entry):
    if not isinstance(entry, dict):
        return {"entry_type": type(entry).__name__}
    return {
        key: entry.get(key)
        for key in ("id", "day", "start_time", "end_time", "class_name", "joined")
        if key in entry
    }


def _safe_record_context(record):
    if not isinstance(record, dict):
        return {"record_type": type(record).__name__}
    return {
        "recycle_id": record.get("recycle_id"),
        "deleted_at": record.get("deleted_at"),
        "entry": _safe_entry_context(record.get("entry")),
    }


def _safe_issue_context(issues):
    if not issues:
        return {}
    first_issue = issues[0]
    return {
        "issue_count": len(issues),
        "first_issue": {
            "index": first_issue.get("index"),
            "fields": first_issue.get("fields", []),
            "error": first_issue.get("error"),
        },
    }


def _log_known_issue(function_name, message, exc, context=None):
    log_warning(
        LOGGER,
        message,
        what=message,
        where=_where(function_name),
        why=str(exc),
        context=context,
    )


def _log_unexpected_issue(function_name, message, context=None):
    log_error(
        LOGGER,
        message,
        what=message,
        where=_where(function_name),
        why="Unexpected exception escaped a timetable manager operation.",
        context=context,
        exc_info=True,
    )


def _clone_entry(entry):
    return dict(entry)


def _clone_timetable(timetable):
    return [_clone_entry(entry) for entry in timetable]


def _clone_recycle_record(record):
    return {
        "recycle_id": record["recycle_id"],
        "deleted_at": record["deleted_at"],
        "entry": _clone_entry(record["entry"]),
    }


def _clone_recycle_bin(recycle_bin):
    return [_clone_recycle_record(record) for record in recycle_bin]


def _validate_text(value, field_name):
    if not isinstance(value, str):
        raise TimetableValidationError(f"{field_name} must be a string.")
    value = value.strip()
    if not value:
        raise TimetableValidationError(f"{field_name} cannot be empty.")
    return value


def validate_required_text(value, field_name):
    return _validate_text(value, field_name)


def validate_optional_text(value):
    if not isinstance(value, str):
        raise TimetableValidationError("Optional input must be a string.")
    return value.strip()


def _validate_recycle_id(value):
    value = _validate_text(value, "recycle_id")
    if not RECYCLE_ID_PATTERN.fullmatch(value):
        raise TimetableValidationError(
            "recycle_id must be in the format 'bin_<number>', for example 'bin_1'."
        )
    return value


def validate_recycle_id(value):
    return _validate_recycle_id(value)


def _validate_entry_id(value):
    value = _validate_text(value, "entry_id")
    if not ENTRY_ID_PATTERN.fullmatch(value):
        raise TimetableValidationError(
            "entry_id must be in the format '<day_prefix>_<number>', for example 'mon_1'."
        )
    return value


def validate_entry_id(value):
    return _validate_entry_id(value)


def _parse_time_to_minutes(value, field_name):
    value = _validate_text(value, field_name)
    if not TIME_PATTERN.fullmatch(value):
        raise TimetableValidationError(f"{field_name} must be in HH:MM format.")
    hours = int(value[:2])
    minutes = int(value[3:5])
    if hours > 23 or minutes > 59:
        raise TimetableValidationError(f"{field_name} must be in HH:MM format.")
    return value, hours * 60 + minutes


def _validate_time(value, field_name):
    value, _ = _parse_time_to_minutes(value, field_name)
    return value


def validate_time_input(value, field_name="time"):
    return _validate_time(value, field_name)


def _validate_day(value):
    value = _validate_text(value, "day").title()
    if value not in VALID_DAYS:
        raise TimetableValidationError(
            f"day must be a valid day name: {', '.join(sorted(VALID_DAYS))}."
        )
    return value


def validate_day_input(value):
    return _validate_day(value)


def validate_yes_no_input(value):
    value = _validate_text(value, "confirmation").lower()
    if value in VALID_YES_VALUES:
        return True
    if value in VALID_NO_VALUES:
        return False
    raise TimetableValidationError("Please enter y or n.")


def _validate_joined_flag(value):
    if not isinstance(value, bool):
        raise TimetableValidationError("joined must be a boolean value.")
    return value


def validate_time_range(start_time, end_time):
    start_time, start_minutes = _parse_time_to_minutes(start_time, "start_time")
    end_time, end_minutes = _parse_time_to_minutes(end_time, "end_time")
    gap_minutes = end_minutes - start_minutes
    if gap_minutes < 60:
        raise TimetableValidationError(
            "The gap between start_time and end_time must be at least 1 hour."
        )
    if gap_minutes > 180:
        raise TimetableValidationError(
            "The gap between start_time and end_time must not be more than 3 hours."
        )
    return start_time, end_time


def _normalize_update_fields(kwargs):
    normalized = dict(kwargs)
    if "url" in normalized:
        if "classroom_url" in normalized:
            raise TimetableValidationError("Use either 'url' or 'classroom_url', not both.")
        normalized["classroom_url"] = normalized.pop("url")
    invalid_fields = set(normalized) - VALID_FIELDS
    if invalid_fields:
        raise TimetableValidationError(f"Invalid field(s): {', '.join(sorted(invalid_fields))}")
    return normalized


def _validate_entry(entry):
    if not isinstance(entry, dict):
        raise TimetableValidationError("Each timetable entry must be an object.")
    start_time, end_time = validate_time_range(entry.get("start_time"), entry.get("end_time"))
    return {
        "id": _validate_entry_id(entry.get("id")),
        "day": _validate_day(entry.get("day")),
        "start_time": start_time,
        "end_time": end_time,
        "class_name": _validate_text(entry.get("class_name"), "class_name"),
        "classroom_url": _validate_text(entry.get("classroom_url"), "classroom_url"),
        "joined": _validate_joined_flag(entry.get("joined", False)),
    }


def build_validated_entry(entry_id, day, start_time, end_time, class_name, classroom_url):
    return _validate_entry(
        {
            "id": entry_id,
            "day": day,
            "start_time": start_time,
            "end_time": end_time,
            "class_name": class_name,
            "classroom_url": classroom_url,
            "joined": False,
        }
    )


def _validate_recycle_record(record):
    if not isinstance(record, dict):
        raise TimetableValidationError("Each recycle bin record must be an object.")
    deleted_at = _validate_text(record.get("deleted_at"), "deleted_at")
    try:
        datetime.fromisoformat(deleted_at)
    except ValueError as exc:
        raise TimetableValidationError("deleted_at must be a valid ISO datetime string.") from exc
    return {
        "recycle_id": _validate_recycle_id(record.get("recycle_id")),
        "deleted_at": deleted_at,
        "entry": _validate_entry(record.get("entry")),
    }


def _slot_key(entry):
    return entry["day"], entry["start_time"], entry["end_time"]


def _build_timetable_indexes(timetable):
    id_to_index = {}
    slot_to_indexes = {}
    prefix_to_numbers = {}

    for index, entry in enumerate(timetable):
        entry_id = entry["id"]
        if entry_id in id_to_index:
            raise TimetableValidationError(f"Duplicate id found in timetable: {entry_id}")
        id_to_index[entry_id] = index
        slot_to_indexes.setdefault(_slot_key(entry), []).append(index)
        prefix, suffix = entry_id.split("_", 1)
        prefix_to_numbers.setdefault(prefix, set()).add(int(suffix))

    return id_to_index, slot_to_indexes, prefix_to_numbers


def _build_recycle_index_map(recycle_bin):
    result = {}
    for index, record in enumerate(recycle_bin):
        recycle_id = record["recycle_id"]
        if recycle_id in result:
            raise TimetableValidationError(
                f"Duplicate recycle_id found in recycle bin: {recycle_id}"
            )
        result[recycle_id] = index
    return result


def _get_first_available_number(used_numbers):
    number = 1
    while number in used_numbers:
        number += 1
    return number


def _find_repair_entry_id(raw_entries, index_to_replace, proposed_id, day):
    try:
        return _validate_entry_id(proposed_id)
    except TimetableValidationError:
        pass

    prefix = _validate_day(day)[:3].lower()
    used_numbers = set()

    for index, entry in enumerate(raw_entries):
        if index == index_to_replace or not isinstance(entry, dict):
            continue
        entry_id = entry.get("id")
        if not isinstance(entry_id, str):
            continue
        match = ENTRY_ID_PATTERN.fullmatch(entry_id.strip())
        if not match:
            continue
        entry_prefix, suffix = entry_id.strip().split("_", 1)
        if entry_prefix == prefix:
            used_numbers.add(int(suffix))

    return f"{prefix}_{_get_first_available_number(used_numbers)}"


def _ensure_no_duplicate_slots(timetable, slot_to_indexes):
    duplicates = [
        [copy.deepcopy(timetable[index]) for index in indexes]
        for indexes in slot_to_indexes.values()
        if len(indexes) > 1
    ]
    if duplicates:
        raise DuplicateTimeSlotError(duplicates)


def _format_overlap_message(existing_entry, candidate_entry):
    return (
        f"Entry '{candidate_entry['id']}' overlaps with entry '{existing_entry['id']}' "
        f"on {existing_entry['day']} from {existing_entry['start_time']} "
        f"to {existing_entry['end_time']}."
    )


def _find_overlapping_entries(timetable, ignore_exact_duplicates=False):
    day_groups = {}
    overlaps = []

    for index, entry in enumerate(timetable):
        day_groups.setdefault(entry["day"], []).append(
            (entry["start_time"], entry["end_time"], index, entry)
        )

    for day_entries in day_groups.values():
        day_entries.sort(key=lambda item: (item[0], item[1], item[2]))
        max_end = None
        max_index = None
        max_entry = None

        for start_time, end_time, index, entry in day_entries:
            if max_end is not None and start_time < max_end:
                is_exact_duplicate = (
                    max_entry["start_time"] == entry["start_time"]
                    and max_entry["end_time"] == entry["end_time"]
                )
                if not (ignore_exact_duplicates and is_exact_duplicate):
                    overlaps.append((max_index, index))
            if max_end is None or end_time > max_end:
                max_end = end_time
                max_index = index
                max_entry = entry

    return overlaps


def _ensure_no_overlapping_slots(timetable, ignore_exact_duplicates=False):
    overlaps = _find_overlapping_entries(
        timetable,
        ignore_exact_duplicates=ignore_exact_duplicates,
    )
    if overlaps:
        existing_index, candidate_index = overlaps[0]
        raise TimetableConflictError(
            _format_overlap_message(timetable[existing_index], timetable[candidate_index])
        )


def _validate_timetable_entries(data, allow_duplicate_slots=False):
    if not isinstance(data, list):
        raise TimetableValidationError("Timetable data must be a list.")
    validated = [_validate_entry(entry) for entry in data]
    _, slot_to_indexes, _ = _build_timetable_indexes(validated)
    if not allow_duplicate_slots:
        _ensure_no_duplicate_slots(validated, slot_to_indexes)
    _ensure_no_overlapping_slots(validated, ignore_exact_duplicates=allow_duplicate_slots)
    return validated


def _validate_recycle_bin(data):
    if not isinstance(data, list):
        raise TimetableValidationError("Recycle bin data must be a list.")
    validated = [_validate_recycle_record(record) for record in data]
    _build_recycle_index_map(validated)
    return validated


def _collect_invalid_entry_fields(entry):
    if not isinstance(entry, dict):
        return ["day", "start_time", "end_time", "class_name", "classroom_url"]

    invalid_fields = []

    try:
        _validate_entry_id(entry.get("id"))
    except TimetableError:
        invalid_fields.append("id")

    try:
        _validate_day(entry.get("day"))
    except TimetableError:
        invalid_fields.append("day")

    time_invalid = False
    try:
        _validate_time(entry.get("start_time"), "start_time")
    except TimetableError:
        time_invalid = True
    try:
        _validate_time(entry.get("end_time"), "end_time")
    except TimetableError:
        time_invalid = True
    if not time_invalid:
        try:
            validate_time_range(entry.get("start_time"), entry.get("end_time"))
        except TimetableError:
            time_invalid = True
    if time_invalid:
        invalid_fields.extend(["start_time", "end_time"])

    try:
        _validate_text(entry.get("class_name"), "class_name")
    except TimetableError:
        invalid_fields.append("class_name")

    try:
        _validate_text(entry.get("classroom_url"), "classroom_url")
    except TimetableError:
        invalid_fields.append("classroom_url")

    try:
        _validate_joined_flag(entry.get("joined", False))
    except TimetableError:
        invalid_fields.append("joined")

    ordered_fields = []
    for field in ["id", "day", "start_time", "end_time", "class_name", "classroom_url", "joined"]:
        if field in invalid_fields:
            ordered_fields.append(field)
    return ordered_fields


def _inspect_timetable_entry_issues_from_data(raw_timetable):
    issues = []
    valid_entries = []

    for index, entry in enumerate(raw_timetable):
        try:
            valid_entries.append((index, _validate_entry(entry)))
        except TimetableError as exc:
            issues.append(
                {
                    "index": index,
                    "entry": copy.deepcopy(entry),
                    "error": str(exc),
                    "fields": _collect_invalid_entry_fields(entry),
                }
            )

    overlaps = _find_overlapping_entries(
        [validated_entry for _, validated_entry in valid_entries],
        ignore_exact_duplicates=True,
    )
    for existing_valid_index, candidate_valid_index in overlaps:
        raw_index, candidate_entry = valid_entries[candidate_valid_index]
        existing_entry = valid_entries[existing_valid_index][1]
        issues.append(
            {
                "index": raw_index,
                "entry": copy.deepcopy(raw_timetable[raw_index]),
                "error": _format_overlap_message(existing_entry, candidate_entry),
                "fields": ["start_time", "end_time"],
            }
        )

    return issues


def _fetch_raw_timetable():
    return db.fetch_all_classes()


def _fetch_raw_recycle_bin():
    return db.fetch_all_recycle_records()


def _ensure_database_ready():
    global DATABASE_READY
    if DATABASE_READY:
        return

    with STORAGE_LOCK:
        if DATABASE_READY:
            return
        try:
            db.initialize_database()
            DATABASE_READY = True
        except sqlite3.DatabaseError as exc:
            raise TimetableStorageError(f"Could not initialize the database: {exc}") from exc


def load_timetable(allow_duplicate_slots=False):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            timetable = _fetch_raw_timetable()
            validated = _validate_timetable_entries(
                timetable,
                allow_duplicate_slots=allow_duplicate_slots,
            )
            return _clone_timetable(validated)
        except TimetableError as exc:
            _log_known_issue(
                "load_timetable",
                "Loading timetable data failed.",
                exc,
                {"allow_duplicate_slots": allow_duplicate_slots},
            )
            raise
        except sqlite3.DatabaseError as exc:
            _log_known_issue("load_timetable", "Loading timetable data failed.", exc)
            raise TimetableStorageError(f"Could not read timetable data: {exc}") from exc
        except Exception:
            _log_unexpected_issue("load_timetable", "Loading timetable data failed unexpectedly.")
            raise


def load_recycle_bin():
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            recycle_bin = _fetch_raw_recycle_bin()
            validated = _validate_recycle_bin(recycle_bin)
            return _clone_recycle_bin(validated)
        except TimetableError as exc:
            _log_known_issue("load_recycle_bin", "Loading recycle-bin data failed.", exc)
            raise
        except sqlite3.DatabaseError as exc:
            _log_known_issue("load_recycle_bin", "Loading recycle-bin data failed.", exc)
            raise TimetableStorageError(f"Could not read recycle-bin data: {exc}") from exc
        except Exception:
            _log_unexpected_issue(
                "load_recycle_bin",
                "Loading recycle-bin data failed unexpectedly.",
            )
            raise


def load_raw_timetable():
    with STORAGE_LOCK:
        _ensure_database_ready()
        return _clone_timetable(_fetch_raw_timetable())


def inspect_timetable_entry_issues():
    with STORAGE_LOCK:
        _ensure_database_ready()
        return _inspect_timetable_entry_issues_from_data(_fetch_raw_timetable())


def initialize_storage():
    global DATABASE_READY
    with STORAGE_LOCK:
        try:
            SHUTDOWN_EVENT.clear()
            _ensure_database_ready()
            raw_timetable = _fetch_raw_timetable()
            issues = _inspect_timetable_entry_issues_from_data(raw_timetable)
            if issues:
                raise InvalidTimetableEntriesError(issues)
            _validate_timetable_entries(raw_timetable)
            _validate_recycle_bin(_fetch_raw_recycle_bin())
            DATABASE_READY = True
            log_info(
                LOGGER,
                "Storage initialization completed.",
                what="Initialized SQLite-backed timetable storage.",
                where=_where("initialize_storage"),
                why="The application startup sequence requested a ready database state.",
                context={
                    "timetable_count": db.count_classes(),
                    "recycle_bin_count": db.count_recycle_records(),
                    "database_path": db.get_database_path(),
                },
            )
        except TimetableError as exc:
            _log_known_issue("initialize_storage", "Storage initialization failed.", exc)
            raise
        except sqlite3.DatabaseError as exc:
            _log_known_issue("initialize_storage", "Storage initialization failed.", exc)
            raise TimetableStorageError(f"Could not initialize storage: {exc}") from exc
        except Exception:
            _log_unexpected_issue("initialize_storage", "Storage initialization failed unexpectedly.")
            raise


def clear_storage_cache():
    global DATABASE_READY
    DATABASE_READY = False


def get_shutdown_event():
    return SHUTDOWN_EVENT


def shutdown_storage():
    global DATABASE_READY
    SHUTDOWN_EVENT.set()
    DATABASE_READY = False
    db.shutdown_database()
    log_info(
        LOGGER,
        "Storage shutdown signal set.",
        what="Marked the timetable storage layer for shutdown.",
        where=_where("shutdown_storage"),
        why="The application is exiting or stopping background work safely.",
    )


def save_timetable(data):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            validated = _validate_timetable_entries(data)
            db.replace_classes(validated)
            log_event(
                LOGGER,
                "Saved timetable data to SQLite.",
                what="Committed timetable changes into the database.",
                where=_where("save_timetable"),
                why="Validated timetable data needed to be persisted safely in SQLite.",
                context={"entry_count": len(validated)},
            )
            return _clone_timetable(validated)
        except TimetableError as exc:
            _log_known_issue(
                "save_timetable",
                "Saving timetable data failed.",
                exc,
                {"entry_count": len(data) if isinstance(data, list) else None},
            )
            raise
        except sqlite3.DatabaseError as exc:
            _log_known_issue("save_timetable", "Saving timetable data failed.", exc)
            raise TimetableStorageError(f"Could not save timetable data: {exc}") from exc
        except Exception:
            _log_unexpected_issue("save_timetable", "Saving timetable data failed unexpectedly.")
            raise


def save_recycle_bin(data):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            validated = _validate_recycle_bin(data)
            db.replace_recycle_bin(validated)
            log_event(
                LOGGER,
                "Saved recycle-bin data to SQLite.",
                what="Committed recycle-bin changes into the database.",
                where=_where("save_recycle_bin"),
                why="Validated recycle-bin data needed to be persisted safely in SQLite.",
                context={"entry_count": len(validated)},
            )
            return _clone_recycle_bin(validated)
        except TimetableError as exc:
            _log_known_issue(
                "save_recycle_bin",
                "Saving recycle-bin data failed.",
                exc,
                {"entry_count": len(data) if isinstance(data, list) else None},
            )
            raise
        except sqlite3.DatabaseError as exc:
            _log_known_issue("save_recycle_bin", "Saving recycle-bin data failed.", exc)
            raise TimetableStorageError(f"Could not save recycle-bin data: {exc}") from exc
        except Exception:
            _log_unexpected_issue(
                "save_recycle_bin",
                "Saving recycle-bin data failed unexpectedly.",
            )
            raise


def delete_raw_timetable_entry(index):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            raw = _fetch_raw_timetable()
            if not isinstance(index, int):
                raise TimetableValidationError("index must be an integer.")
            if index < 0 or index >= len(raw):
                raise TimetableNotFoundError(f"No raw timetable entry found at index {index}.")
            deleted = copy.deepcopy(raw[index])
            deleted_count = db.delete_class(deleted["id"])
            if deleted_count == 0:
                raise TimetableNotFoundError(
                    f"No raw timetable entry found at index {index}."
                )
            log_event(
                LOGGER,
                "Deleted an invalid stored timetable entry.",
                what="Removed a malformed timetable row directly from SQLite storage.",
                where=_where("delete_raw_timetable_entry"),
                why="Startup repair removed a stored row that could not be kept safely.",
                context={"index": index, "entry": _safe_entry_context(deleted)},
            )
            return deleted
        except TimetableError as exc:
            _log_known_issue(
                "delete_raw_timetable_entry",
                "Deleting a raw timetable entry failed.",
                exc,
                {"index": index},
            )
            raise
        except sqlite3.DatabaseError as exc:
            _log_known_issue(
                "delete_raw_timetable_entry",
                "Deleting a raw timetable entry failed.",
                exc,
                {"index": index},
            )
            raise TimetableStorageError(f"Could not delete raw timetable entry: {exc}") from exc
        except Exception:
            _log_unexpected_issue(
                "delete_raw_timetable_entry",
                "Deleting a raw timetable entry failed unexpectedly.",
                {"index": index},
            )
            raise


def repair_timetable_entry(index, entry_id, day, start_time, end_time, class_name, classroom_url):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            raw = _fetch_raw_timetable()
            if not isinstance(index, int):
                raise TimetableValidationError("index must be an integer.")
            if index < 0 or index >= len(raw):
                raise TimetableNotFoundError(f"No raw timetable entry found at index {index}.")
            repair_entry_id = _find_repair_entry_id(raw, index, entry_id, day)
            repaired_entry = build_validated_entry(
                repair_entry_id,
                day,
                start_time,
                end_time,
                class_name,
                classroom_url,
            )
            raw[index] = repaired_entry
            save_timetable(raw)
            log_event(
                LOGGER,
                "Repaired an invalid stored timetable entry.",
                what="Updated a malformed timetable row in SQLite storage.",
                where=_where("repair_timetable_entry"),
                why="Startup repair replaced invalid stored values with validated values.",
                context={"index": index, "entry": _safe_entry_context(repaired_entry)},
            )
            return _clone_entry(repaired_entry)
        except TimetableError as exc:
            _log_known_issue(
                "repair_timetable_entry",
                "Repairing a raw timetable entry failed.",
                exc,
                {"index": index, "entry_id": entry_id},
            )
            raise
        except sqlite3.DatabaseError as exc:
            _log_known_issue(
                "repair_timetable_entry",
                "Repairing a raw timetable entry failed.",
                exc,
                {"index": index, "entry_id": entry_id},
            )
            raise TimetableStorageError(f"Could not repair raw timetable entry: {exc}") from exc
        except Exception:
            _log_unexpected_issue(
                "repair_timetable_entry",
                "Repairing a raw timetable entry failed unexpectedly.",
                {"index": index, "entry_id": entry_id},
            )
            raise


def add_entry(day, start_time, end_time, class_name, url):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            day = _validate_day(day)
            start_time, end_time = validate_time_range(start_time, end_time)
            class_name = _validate_text(class_name, "class_name")
            url = _validate_text(url, "classroom_url")
            if db.slot_exists(day, start_time, end_time):
                raise TimetableConflictError(
                    f"A timetable entry already exists or overlaps on {day} "
                    f"between {start_time} and {end_time}."
                )
            prefix = day[:3].lower()
            used_numbers = {
                int(entry_id.split("_", 1)[1])
                for entry_id in db.fetch_entry_ids_by_prefix(prefix)
            }
            new_entry = {
                "id": f"{prefix}_{_get_first_available_number(used_numbers)}",
                "day": day,
                "start_time": start_time,
                "end_time": end_time,
                "class_name": class_name,
                "classroom_url": url,
                "joined": False,
            }
            db.insert_class(new_entry)
            log_event(
                LOGGER,
                "Added a timetable entry.",
                what="Created a new timetable slot in SQLite.",
                where=_where("add_entry"),
                why="A new class entry was validated and saved into the database.",
                context={"entry": _safe_entry_context(new_entry)},
            )
            return _clone_entry(new_entry)
        except TimetableError as exc:
            _log_known_issue(
                "add_entry",
                "Adding a timetable entry failed.",
                exc,
                {
                    "day": day,
                    "start_time": start_time,
                    "end_time": end_time,
                    "class_name": class_name,
                },
            )
            raise
        except sqlite3.IntegrityError as exc:
            _log_known_issue(
                "add_entry",
                "Adding a timetable entry failed.",
                exc,
                {
                    "day": day,
                    "start_time": start_time,
                    "end_time": end_time,
                    "class_name": class_name,
                },
            )
            raise TimetableConflictError(
                "The timetable entry could not be added because it conflicts with existing stored data."
            ) from exc
        except Exception:
            _log_unexpected_issue(
                "add_entry",
                "Adding a timetable entry failed unexpectedly.",
                {
                    "day": day,
                    "start_time": start_time,
                    "end_time": end_time,
                    "class_name": class_name,
                },
            )
            raise


def get_entry_by_id(entry_id):
    try:
        _ensure_database_ready()
        entry_id = _validate_entry_id(entry_id)
        entry = db.fetch_class_by_id(entry_id)
        if entry is None:
            raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
        return _clone_entry(entry)
    except TimetableError as exc:
        _log_known_issue("get_entry_by_id", "Entry lookup failed.", exc, {"entry_id": entry_id})
        raise
    except sqlite3.DatabaseError as exc:
        _log_known_issue("get_entry_by_id", "Entry lookup failed.", exc, {"entry_id": entry_id})
        raise TimetableStorageError(f"Could not look up entry data: {exc}") from exc
    except Exception:
        _log_unexpected_issue("get_entry_by_id", "Entry lookup failed unexpectedly.", {"entry_id": entry_id})
        raise


def get_recycle_record_by_id(recycle_id):
    try:
        _ensure_database_ready()
        recycle_id = _validate_recycle_id(recycle_id)
        record = db.fetch_recycle_record_by_id(recycle_id)
        if record is None:
            raise TimetableNotFoundError(
                f"No recycle bin entry found with recycle_id '{recycle_id}'."
            )
        return _clone_recycle_record(record)
    except TimetableError as exc:
        _log_known_issue(
            "get_recycle_record_by_id",
            "Recycle-bin lookup failed.",
            exc,
            {"recycle_id": recycle_id},
        )
        raise
    except sqlite3.DatabaseError as exc:
        _log_known_issue(
            "get_recycle_record_by_id",
            "Recycle-bin lookup failed.",
            exc,
            {"recycle_id": recycle_id},
        )
        raise TimetableStorageError(f"Could not look up recycle-bin data: {exc}") from exc
    except Exception:
        _log_unexpected_issue(
            "get_recycle_record_by_id",
            "Recycle-bin lookup failed unexpectedly.",
            {"recycle_id": recycle_id},
        )
        raise


def edit_entry(entry_id, **kwargs):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            entry_id = _validate_entry_id(entry_id)
            updates = _normalize_update_fields(kwargs)
            if not updates:
                raise TimetableValidationError("Provide at least one field to update.")
            current = db.fetch_class_by_id(entry_id)
            if current is None:
                raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
            updated = _validate_entry(
                {
                    "id": current["id"],
                    "day": updates.get("day", current["day"]),
                    "start_time": updates.get("start_time", current["start_time"]),
                    "end_time": updates.get("end_time", current["end_time"]),
                    "class_name": updates.get("class_name", current["class_name"]),
                    "classroom_url": updates.get("classroom_url", current["classroom_url"]),
                    "joined": current["joined"],
                }
            )
            if db.slot_exists(
                updated["day"],
                updated["start_time"],
                updated["end_time"],
                ignore_entry_id=entry_id,
            ):
                raise TimetableConflictError(
                    f"A timetable entry already exists or overlaps on {updated['day']} "
                    f"between {updated['start_time']} and {updated['end_time']}."
                )
            db.update_class(entry_id, updated)
            log_event(
                LOGGER,
                "Edited a timetable entry.",
                what="Updated an existing timetable slot in SQLite.",
                where=_where("edit_entry"),
                why="User or UI changes were validated and saved into the database.",
                context={
                    "entry_id": entry_id,
                    "updated_fields": sorted(updates),
                    "entry": _safe_entry_context(updated),
                },
            )
            return _clone_entry(updated)
        except TimetableError as exc:
            _log_known_issue(
                "edit_entry",
                "Editing a timetable entry failed.",
                exc,
                {"entry_id": entry_id, "updated_fields": sorted(kwargs)},
            )
            raise
        except sqlite3.IntegrityError as exc:
            _log_known_issue(
                "edit_entry",
                "Editing a timetable entry failed.",
                exc,
                {"entry_id": entry_id, "updated_fields": sorted(kwargs)},
            )
            raise TimetableConflictError(
                "The timetable entry could not be updated because it conflicts with existing stored data."
            ) from exc
        except LookupError as exc:
            _log_known_issue("edit_entry", "Editing a timetable entry failed.", exc, {"entry_id": entry_id})
            raise TimetableNotFoundError(str(exc)) from exc
        except Exception:
            _log_unexpected_issue(
                "edit_entry",
                "Editing a timetable entry failed unexpectedly.",
                {"entry_id": entry_id, "updated_fields": sorted(kwargs)},
            )
            raise


def set_entry_joined_status(entry_id, joined):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            entry_id = _validate_entry_id(entry_id)
            joined = _validate_joined_flag(joined)
            current = db.fetch_class_by_id(entry_id)
            if current is None:
                raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
            current["joined"] = joined
            db.update_joined_status(entry_id, joined)
            log_event(
                LOGGER,
                "Updated joined status for a timetable entry.",
                what="Marked a class as joined or not joined in SQLite.",
                where=_where("set_entry_joined_status"),
                why="Background automation or future scheduler state changed.",
                context={"entry_id": entry_id, "joined": joined},
            )
            return _clone_entry(current)
        except TimetableError as exc:
            _log_known_issue(
                "set_entry_joined_status",
                "Updating joined status failed.",
                exc,
                {"entry_id": entry_id, "joined": joined},
            )
            raise
        except LookupError as exc:
            _log_known_issue(
                "set_entry_joined_status",
                "Updating joined status failed.",
                exc,
                {"entry_id": entry_id, "joined": joined},
            )
            raise TimetableNotFoundError(str(exc)) from exc
        except sqlite3.DatabaseError as exc:
            _log_known_issue(
                "set_entry_joined_status",
                "Updating joined status failed.",
                exc,
                {"entry_id": entry_id, "joined": joined},
            )
            raise TimetableStorageError(f"Could not update joined status: {exc}") from exc
        except Exception:
            _log_unexpected_issue(
                "set_entry_joined_status",
                "Updating joined status failed unexpectedly.",
                {"entry_id": entry_id, "joined": joined},
            )
            raise


def delete_entry(entry_id, allow_duplicate_slots=False):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            entry_id = _validate_entry_id(entry_id)
            entry = db.fetch_class_by_id(entry_id)
            if entry is None:
                raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
            record = {
                "recycle_id": db.next_recycle_id(),
                "deleted_at": datetime.now().isoformat(timespec="seconds"),
                "entry": _clone_entry(entry),
            }
            db.move_entry_to_recycle(entry, record["recycle_id"], record["deleted_at"])
            log_event(
                LOGGER,
                "Moved a timetable entry to the recycle bin.",
                what="Soft-deleted a timetable entry from SQLite.",
                where=_where("delete_entry"),
                why="The entry was removed from the active timetable but kept for recovery.",
                context={"entry_id": entry_id, "recycle_record": _safe_record_context(record)},
            )
            return _clone_recycle_record(record)
        except TimetableError as exc:
            _log_known_issue(
                "delete_entry",
                "Deleting a timetable entry failed.",
                exc,
                {"entry_id": entry_id, "allow_duplicate_slots": allow_duplicate_slots},
            )
            raise
        except LookupError as exc:
            _log_known_issue(
                "delete_entry",
                "Deleting a timetable entry failed.",
                exc,
                {"entry_id": entry_id, "allow_duplicate_slots": allow_duplicate_slots},
            )
            raise TimetableNotFoundError(str(exc)) from exc
        except sqlite3.IntegrityError as exc:
            _log_known_issue(
                "delete_entry",
                "Deleting a timetable entry failed.",
                exc,
                {"entry_id": entry_id, "allow_duplicate_slots": allow_duplicate_slots},
            )
            raise TimetableConflictError(
                "The timetable entry could not be deleted safely because the recycle-bin move conflicted with existing stored data."
            ) from exc
        except Exception:
            _log_unexpected_issue(
                "delete_entry",
                "Deleting a timetable entry failed unexpectedly.",
                {"entry_id": entry_id, "allow_duplicate_slots": allow_duplicate_slots},
            )
            raise


def restore_entry(recycle_id, allow_duplicate_slots=False):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            recycle_id = _validate_recycle_id(recycle_id)
            record = db.fetch_recycle_record_by_id(recycle_id)
            if record is None:
                raise TimetableNotFoundError(
                    f"No recycle bin entry found with recycle_id '{recycle_id}'."
                )
            entry = record["entry"]
            if db.fetch_class_by_id(entry["id"]) is not None:
                raise TimetableConflictError(
                    f"Cannot restore entry because id '{entry['id']}' already exists in the timetable."
                )
            if db.slot_exists(entry["day"], entry["start_time"], entry["end_time"]):
                raise TimetableConflictError(
                    f"A timetable entry already exists or overlaps on {entry['day']} "
                    f"between {entry['start_time']} and {entry['end_time']}."
                )
            db.restore_recycle_record(record)
            log_event(
                LOGGER,
                "Restored a recycle-bin entry into the timetable.",
                what="Recovered a previously deleted timetable entry from SQLite.",
                where=_where("restore_entry"),
                why="The recycle-bin record passed validation and no conflicts blocked restoration.",
                context={"recycle_id": recycle_id, "entry": _safe_entry_context(entry)},
            )
            return _clone_entry(entry)
        except TimetableError as exc:
            _log_known_issue(
                "restore_entry",
                "Restoring a recycle-bin entry failed.",
                exc,
                {"recycle_id": recycle_id, "allow_duplicate_slots": allow_duplicate_slots},
            )
            raise
        except LookupError as exc:
            _log_known_issue(
                "restore_entry",
                "Restoring a recycle-bin entry failed.",
                exc,
                {"recycle_id": recycle_id, "allow_duplicate_slots": allow_duplicate_slots},
            )
            raise TimetableNotFoundError(str(exc)) from exc
        except sqlite3.IntegrityError as exc:
            _log_known_issue(
                "restore_entry",
                "Restoring a recycle-bin entry failed.",
                exc,
                {"recycle_id": recycle_id, "allow_duplicate_slots": allow_duplicate_slots},
            )
            raise TimetableConflictError(
                "The recycle-bin entry could not be restored because it conflicts with existing stored data."
            ) from exc
        except Exception:
            _log_unexpected_issue(
                "restore_entry",
                "Restoring a recycle-bin entry failed unexpectedly.",
                {"recycle_id": recycle_id, "allow_duplicate_slots": allow_duplicate_slots},
            )
            raise


def permanently_delete_recycle_entry(recycle_id):
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            recycle_id = _validate_recycle_id(recycle_id)
            record = db.fetch_recycle_record_by_id(recycle_id)
            if record is None:
                raise TimetableNotFoundError(
                    f"No recycle bin entry found with recycle_id '{recycle_id}'."
                )
            deleted_count = db.delete_recycle_record(recycle_id)
            if deleted_count == 0:
                raise TimetableNotFoundError(
                    f"No recycle bin entry found with recycle_id '{recycle_id}'."
                )
            log_event(
                LOGGER,
                "Permanently deleted a recycle-bin record.",
                what="Removed a recycle-bin entry permanently from SQLite.",
                where=_where("permanently_delete_recycle_entry"),
                why="The recycle-bin record was intentionally removed and cannot be restored now.",
                context={"recycle_record": _safe_record_context(record)},
            )
            return _clone_recycle_record(record)
        except TimetableError as exc:
            _log_known_issue(
                "permanently_delete_recycle_entry",
                "Permanent recycle-bin deletion failed.",
                exc,
                {"recycle_id": recycle_id},
            )
            raise
        except sqlite3.DatabaseError as exc:
            _log_known_issue(
                "permanently_delete_recycle_entry",
                "Permanent recycle-bin deletion failed.",
                exc,
                {"recycle_id": recycle_id},
            )
            raise TimetableStorageError(f"Could not permanently delete recycle-bin data: {exc}") from exc
        except Exception:
            _log_unexpected_issue(
                "permanently_delete_recycle_entry",
                "Permanent recycle-bin deletion failed unexpectedly.",
                {"recycle_id": recycle_id},
            )
            raise


def clear_recycle_bin():
    with STORAGE_LOCK:
        try:
            _ensure_database_ready()
            recycle_bin = _validate_recycle_bin(_fetch_raw_recycle_bin())
            if not recycle_bin:
                raise TimetableNotFoundError("Recycle bin is empty.")
            deleted_count = db.delete_all_recycle_records()
            if deleted_count != len(recycle_bin):
                raise TimetableStorageError(
                    "Recycle bin could not be cleared safely because the deleted row count did not match the loaded records."
                )
            log_event(
                LOGGER,
                "Cleared the recycle bin.",
                what="Removed all recycle-bin records permanently from SQLite.",
                where=_where("clear_recycle_bin"),
                why="The user confirmed a full recycle-bin cleanup.",
                context={"deleted_count": deleted_count},
            )
            return _clone_recycle_bin(recycle_bin)
        except TimetableError as exc:
            _log_known_issue(
                "clear_recycle_bin",
                "Clearing the recycle bin failed.",
                exc,
            )
            raise
        except sqlite3.DatabaseError as exc:
            _log_known_issue(
                "clear_recycle_bin",
                "Clearing the recycle bin failed.",
                exc,
            )
            raise TimetableStorageError(f"Could not clear recycle-bin data: {exc}") from exc
        except Exception:
            _log_unexpected_issue(
                "clear_recycle_bin",
                "Clearing the recycle bin failed unexpectedly.",
            )
            raise
