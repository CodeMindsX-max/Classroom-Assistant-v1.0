import copy, json, os, re, tempfile, threading
from datetime import datetime

from app_logger_manager import (
    get_app_logger,
    log_debug,
    log_error,
    log_event,
    log_info,
    log_warning,
)

TIMETABLE_FILE = "timetable.json"
RECYCLE_BIN_FILE = "recycle_bin.json"
VALID_FIELDS = {"day", "start_time", "end_time", "class_name", "classroom_url"}
RECYCLE_ID_PATTERN = re.compile(r"^bin_\d+$")
ENTRY_ID_PATTERN = re.compile(r"^[a-z]{3}_\d+$")
TIME_PATTERN = re.compile(r"^\d{2}:\d{2}$")
VALID_DAYS = {"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"}
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
        super().__init__(f"Invalid timetable entries found in stored data ({len(issues)} issue(s)). First issue: {first}")


TIMETABLE_CACHE = None
RECYCLE_BIN_CACHE = None
TIMETABLE_ID_INDEX = None
TIMETABLE_SLOT_INDEX = None
TIMETABLE_PREFIX_INDEX = None
RECYCLE_INDEX_MAP = None
STORAGE_LOCK = threading.RLock()
SHUTDOWN_EVENT = threading.Event()
LOGGER = get_app_logger("timetable_manager")


def _where(function_name): return f"timetable_manager.{function_name}"

def _safe_entry_context(entry):
    if not isinstance(entry, dict):
        return {"entry_type": type(entry).__name__}
    return {key: entry.get(key) for key in ("id", "day", "start_time", "end_time", "class_name", "joined") if key in entry}

def _safe_record_context(record):
    if not isinstance(record, dict):
        return {"record_type": type(record).__name__}
    return {"recycle_id": record.get("recycle_id"), "deleted_at": record.get("deleted_at"), "entry": _safe_entry_context(record.get("entry"))}

def _safe_issue_context(issues):
    if not issues:
        return {}
    first_issue = issues[0]
    return {
        "issue_count": len(issues),
        "first_issue": {"index": first_issue.get("index"), "fields": first_issue.get("fields", []), "error": first_issue.get("error")},
    }

def _log_known_issue(function_name, message, exc, context=None):
    log_warning(LOGGER, message, what=message, where=_where(function_name), why=str(exc), context=context)

def _log_unexpected_issue(function_name, message, context=None):
    log_error(LOGGER, message, what=message, where=_where(function_name), why="Unexpected exception escaped a timetable manager operation.", context=context, exc_info=True)

def _clone_entry(entry): return dict(entry)
def _clone_timetable(timetable): return [_clone_entry(entry) for entry in timetable]
def _clone_recycle_record(record): return {"recycle_id": record["recycle_id"], "deleted_at": record["deleted_at"], "entry": _clone_entry(record["entry"])}
def _clone_recycle_bin(recycle_bin): return [_clone_recycle_record(record) for record in recycle_bin]


def _validate_text(value, field_name):
    if not isinstance(value, str):
        raise TimetableValidationError(f"{field_name} must be a string.")
    value = value.strip()
    if not value:
        raise TimetableValidationError(f"{field_name} cannot be empty.")
    return value

def validate_required_text(value, field_name): return _validate_text(value, field_name)
def validate_optional_text(value):
    if not isinstance(value, str):
        raise TimetableValidationError("Optional input must be a string.")
    return value.strip()

def _validate_recycle_id(value):
    value = _validate_text(value, "recycle_id")
    if not RECYCLE_ID_PATTERN.fullmatch(value):
        raise TimetableValidationError("recycle_id must be in the format 'bin_<number>', for example 'bin_1'.")
    return value

def validate_recycle_id(value): return _validate_recycle_id(value)

def _validate_entry_id(value):
    value = _validate_text(value, "entry_id")
    if not ENTRY_ID_PATTERN.fullmatch(value):
        raise TimetableValidationError("entry_id must be in the format '<day_prefix>_<number>', for example 'mon_1'.")
    return value

def validate_entry_id(value): return _validate_entry_id(value)

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

def validate_time_input(value, field_name="time"): return _validate_time(value, field_name)

def _validate_day(value):
    value = _validate_text(value, "day").title()
    if value not in VALID_DAYS:
        raise TimetableValidationError(f"day must be a valid day name: {', '.join(sorted(VALID_DAYS))}.")
    return value

def validate_day_input(value): return _validate_day(value)

def validate_yes_no_input(value):
    value = _validate_text(value, "confirmation").lower()
    if value in VALID_YES_VALUES: return True
    if value in VALID_NO_VALUES: return False
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
        raise TimetableValidationError("The gap between start_time and end_time must be at least 1 hour.")
    if gap_minutes > 180:
        raise TimetableValidationError("The gap between start_time and end_time must not be more than 3 hours.")
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
    return _validate_entry({"id": entry_id, "day": day, "start_time": start_time, "end_time": end_time, "class_name": class_name, "classroom_url": classroom_url, "joined": False})

def _validate_recycle_record(record):
    if not isinstance(record, dict):
        raise TimetableValidationError("Each recycle bin record must be an object.")
    deleted_at = _validate_text(record.get("deleted_at"), "deleted_at")
    try:
        datetime.fromisoformat(deleted_at)
    except ValueError as exc:
        raise TimetableValidationError("deleted_at must be a valid ISO datetime string.") from exc
    return {"recycle_id": _validate_recycle_id(record.get("recycle_id")), "deleted_at": deleted_at, "entry": _validate_entry(record.get("entry"))}

def _slot_key(entry): return entry["day"], entry["start_time"], entry["end_time"]

def _build_timetable_indexes(timetable):
    id_to_index, slot_to_indexes, prefix_to_numbers = {}, {}, {}
    for i, entry in enumerate(timetable):
        entry_id = entry["id"]
        if entry_id in id_to_index: raise TimetableValidationError(f"Duplicate id found in timetable: {entry_id}")
        id_to_index[entry_id] = i
        slot_to_indexes.setdefault(_slot_key(entry), []).append(i)
        prefix, suffix = entry_id.split("_", 1)
        prefix_to_numbers.setdefault(prefix, set()).add(int(suffix))
    return id_to_index, slot_to_indexes, prefix_to_numbers

def _build_recycle_index_map(recycle_bin):
    result = {}
    for i, record in enumerate(recycle_bin):
        rid = record["recycle_id"]
        if rid in result: raise TimetableValidationError(f"Duplicate recycle_id found in recycle bin: {rid}")
        result[rid] = i
    return result

def _refresh_timetable_cache(validated):
    global TIMETABLE_CACHE, TIMETABLE_ID_INDEX, TIMETABLE_SLOT_INDEX, TIMETABLE_PREFIX_INDEX
    TIMETABLE_CACHE = validated
    TIMETABLE_ID_INDEX, TIMETABLE_SLOT_INDEX, TIMETABLE_PREFIX_INDEX = _build_timetable_indexes(validated)

def _refresh_recycle_bin_cache(validated):
    global RECYCLE_BIN_CACHE, RECYCLE_INDEX_MAP
    RECYCLE_BIN_CACHE = validated
    RECYCLE_INDEX_MAP = _build_recycle_index_map(validated)

def _get_timetable_indexes(allow_duplicate_slots=False):
    _get_timetable_ref(allow_duplicate_slots=allow_duplicate_slots)
    return TIMETABLE_ID_INDEX, TIMETABLE_SLOT_INDEX, TIMETABLE_PREFIX_INDEX

def _get_recycle_index_map():
    _get_recycle_bin_ref()
    return RECYCLE_INDEX_MAP

def _get_first_available_number(used_numbers):
    n = 1
    while n in used_numbers: n += 1
    return n

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
    duplicates = [[copy.deepcopy(timetable[i]) for i in idxs] for idxs in slot_to_indexes.values() if len(idxs) > 1]
    if duplicates: raise DuplicateTimeSlotError(duplicates)

def _ensure_slot_available(slot_to_indexes, day, start_time, end_time, ignore_entry_id=None, id_to_index=None):
    for index in slot_to_indexes.get((day, start_time, end_time), []):
        if ignore_entry_id and id_to_index and id_to_index.get(ignore_entry_id) == index: continue
        raise TimetableConflictError(f"A timetable entry already exists for {day} from {start_time} to {end_time}.")

def _read_json_list_file(path):
    try:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError as exc:
        log_warning(LOGGER, f"Storage file was not found: {path}", what="Storage read failed because the file was missing.", where=_where("_read_json_list_file"), why="The requested JSON file does not exist yet.", context={"path": path})
        raise FileNotFoundError(f"{path} was not found.") from exc
    except json.JSONDecodeError as exc:
        log_error(LOGGER, f"Storage file contains invalid JSON: {path}", what="Storage read failed because JSON parsing failed.", where=_where("_read_json_list_file"), why="The JSON file is corrupted or malformed.", context={"path": path}, exc_info=True)
        raise TimetableValidationError(f"{path} contains invalid JSON.") from exc
    except OSError as exc:
        log_error(LOGGER, f"Storage file could not be read: {path}", what="Storage read failed because the file system rejected the read.", where=_where("_read_json_list_file"), why=str(exc), context={"path": path}, exc_info=True)
        raise TimetableStorageError(f"Could not read {path}: {exc}") from exc
    if not isinstance(data, list):
        log_warning(LOGGER, f"Storage file does not contain a list: {path}", what="Storage validation failed after reading JSON data.", where=_where("_read_json_list_file"), why="The top-level JSON structure must be a list.", context={"path": path, "data_type": type(data).__name__})
        raise TimetableValidationError(f"{path} must contain a list of entries.")
    log_debug(LOGGER, f"Read JSON storage file: {path}", what="Loaded JSON data from disk.", where=_where("_read_json_list_file"), why="A cache miss required reading structured data from storage.", context={"path": path, "entry_count": len(data)})
    return data

def _read_timetable_file():
    return _read_json_list_file(TIMETABLE_FILE)

def _read_recycle_bin_file():
    return _read_json_list_file(RECYCLE_BIN_FILE)

def _atomic_write_json_list_file(path, data):
    directory = os.path.dirname(os.path.abspath(path)) or "."
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=directory, delete=False, suffix=".tmp") as temp_file:
            json.dump(data, temp_file, indent=2, ensure_ascii=False)
            temp_path = temp_file.name
        os.replace(temp_path, path)
    except TypeError as exc:
        log_error(LOGGER, f"Storage write rejected non-serializable data for {path}", what="Storage write failed because the data could not be serialized to JSON.", where=_where("_atomic_write_json_list_file"), why=str(exc), context={"path": path}, exc_info=True)
        raise TimetableValidationError(f"{path} contains non-JSON-serializable values: {exc}") from exc
    except OSError as exc:
        log_error(LOGGER, f"Storage write failed for {path}", what="Storage write failed while replacing the JSON file.", where=_where("_atomic_write_json_list_file"), why=str(exc), context={"path": path, "temporary_path": temp_path}, exc_info=True)
        raise TimetableStorageError(f"Could not save {path}: {exc}") from exc
    finally:
        if temp_path and os.path.exists(temp_path):
            try: os.remove(temp_path)
            except OSError: pass
    log_debug(LOGGER, f"Wrote JSON storage file: {path}", what="Persisted JSON data to disk.", where=_where("_atomic_write_json_list_file"), why="A validated storage update needed to be committed atomically.", context={"path": path, "entry_count": len(data)})

def _validate_timetable_entries(data, allow_duplicate_slots=False):
    if not isinstance(data, list): raise TimetableValidationError("Timetable data must be a list.")
    validated = [_validate_entry(entry) for entry in data]
    _, slot_to_indexes, _ = _build_timetable_indexes(validated)
    if not allow_duplicate_slots: _ensure_no_duplicate_slots(validated, slot_to_indexes)
    return validated

def _validate_recycle_bin(data):
    if not isinstance(data, list): raise TimetableValidationError("Recycle bin data must be a list.")
    validated = [_validate_recycle_record(record) for record in data]
    _build_recycle_index_map(validated)
    return validated

def _inspect_timetable_entry_issues_from_data(raw_timetable):
    issues = []
    for index, entry in enumerate(raw_timetable):
        try: _validate_entry(entry)
        except TimetableError as exc:
            issues.append({
                "index": index,
                "entry": copy.deepcopy(entry),
                "error": str(exc),
                "fields": _collect_invalid_entry_fields(entry),
            })
    return issues

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

def _get_timetable_ref(allow_duplicate_slots=False):
    global TIMETABLE_CACHE
    if TIMETABLE_CACHE is None:
        raw = _read_timetable_file()
        issues = _inspect_timetable_entry_issues_from_data(raw)
        if issues:
            log_error(LOGGER, "Timetable cache load found invalid stored entries.", what="Rejected invalid timetable data during cache initialization.", where=_where("_get_timetable_ref"), why="Stored timetable rows failed validation before being cached.", context=_safe_issue_context(issues))
            raise InvalidTimetableEntriesError(issues)
        validated = _validate_timetable_entries(raw, allow_duplicate_slots=allow_duplicate_slots)
        _refresh_timetable_cache(validated)
        log_info(LOGGER, "Timetable cache initialized from disk.", what="Loaded timetable data into memory.", where=_where("_get_timetable_ref"), why="The timetable cache was empty and needed initialization.", context={"entry_count": len(TIMETABLE_CACHE), "allow_duplicate_slots": allow_duplicate_slots})
    if not allow_duplicate_slots:
        try:
            _ensure_no_duplicate_slots(TIMETABLE_CACHE, TIMETABLE_SLOT_INDEX)
        except DuplicateTimeSlotError as exc:
            log_error(LOGGER, "Timetable cache validation found duplicate time slots.", what="Rejected duplicate timetable slots during cache use.", where=_where("_get_timetable_ref"), why=str(exc), context={"duplicate_group_count": len(exc.duplicates)})
            raise
    return TIMETABLE_CACHE

def _get_recycle_bin_ref():
    global RECYCLE_BIN_CACHE
    if RECYCLE_BIN_CACHE is None:
        try:
            validated = _validate_recycle_bin(_read_recycle_bin_file())
        except FileNotFoundError:
            validated = []
            _atomic_write_json_list_file(RECYCLE_BIN_FILE, validated)
            _refresh_recycle_bin_cache(validated)
            log_info(LOGGER, "Recycle bin file was created automatically.", what="Initialized an empty recycle bin file.", where=_where("_get_recycle_bin_ref"), why="The recycle bin did not exist yet and had to be created safely.", context={"path": RECYCLE_BIN_FILE})
        else:
            _refresh_recycle_bin_cache(validated)
            log_info(LOGGER, "Recycle bin cache initialized from disk.", what="Loaded recycle bin data into memory.", where=_where("_get_recycle_bin_ref"), why="The recycle bin cache was empty and needed initialization.", context={"entry_count": len(RECYCLE_BIN_CACHE)})
    return RECYCLE_BIN_CACHE

def load_timetable(allow_duplicate_slots=False):
    with STORAGE_LOCK: return _clone_timetable(_get_timetable_ref(allow_duplicate_slots=allow_duplicate_slots))

def load_recycle_bin():
    with STORAGE_LOCK: return _clone_recycle_bin(_get_recycle_bin_ref())

def load_raw_timetable():
    with STORAGE_LOCK: return _read_timetable_file()

def inspect_timetable_entry_issues():
    with STORAGE_LOCK: return _inspect_timetable_entry_issues_from_data(_read_timetable_file())

def initialize_storage():
    with STORAGE_LOCK:
        try:
            clear_storage_cache()
            SHUTDOWN_EVENT.clear()
            timetable = _get_timetable_ref()
            recycle_bin = _get_recycle_bin_ref()
            log_info(LOGGER, "Storage initialization completed.", what="Initialized timetable and recycle-bin caches.", where=_where("initialize_storage"), why="The application startup sequence requested a clean synchronized storage state.", context={"timetable_count": len(timetable), "recycle_bin_count": len(recycle_bin)})
        except TimetableError as exc:
            _log_known_issue("initialize_storage", "Storage initialization failed.", exc)
            raise
        except Exception:
            _log_unexpected_issue("initialize_storage", "Storage initialization failed unexpectedly.")
            raise

def clear_storage_cache():
    global TIMETABLE_CACHE, RECYCLE_BIN_CACHE, TIMETABLE_ID_INDEX, TIMETABLE_SLOT_INDEX, TIMETABLE_PREFIX_INDEX, RECYCLE_INDEX_MAP
    TIMETABLE_CACHE = None
    RECYCLE_BIN_CACHE = None
    TIMETABLE_ID_INDEX = None
    TIMETABLE_SLOT_INDEX = None
    TIMETABLE_PREFIX_INDEX = None
    RECYCLE_INDEX_MAP = None

def get_shutdown_event(): return SHUTDOWN_EVENT
def shutdown_storage():
    SHUTDOWN_EVENT.set()
    log_info(LOGGER, "Storage shutdown signal set.", what="Marked the timetable storage layer for shutdown.", where=_where("shutdown_storage"), why="The application is exiting or stopping background work safely.")

def save_timetable(data):
    with STORAGE_LOCK:
        try:
            validated = _validate_timetable_entries(data)
            _atomic_write_json_list_file(TIMETABLE_FILE, validated)
            _refresh_timetable_cache(validated)
            log_event(LOGGER, "Saved timetable data to disk.", what="Committed timetable changes.", where=_where("save_timetable"), why="Validated timetable data needed to be persisted.", context={"entry_count": len(validated)})
            return _clone_timetable(validated)
        except TimetableError as exc:
            _log_known_issue("save_timetable", "Saving timetable data failed.", exc, {"entry_count": len(data) if isinstance(data, list) else None})
            raise
        except Exception:
            _log_unexpected_issue("save_timetable", "Saving timetable data failed unexpectedly.")
            raise

def save_recycle_bin(data):
    with STORAGE_LOCK:
        try:
            validated = _validate_recycle_bin(data)
            _atomic_write_json_list_file(RECYCLE_BIN_FILE, validated)
            _refresh_recycle_bin_cache(validated)
            log_event(LOGGER, "Saved recycle-bin data to disk.", what="Committed recycle-bin changes.", where=_where("save_recycle_bin"), why="Validated recycle-bin data needed to be persisted.", context={"entry_count": len(validated)})
            return _clone_recycle_bin(validated)
        except TimetableError as exc:
            _log_known_issue("save_recycle_bin", "Saving recycle-bin data failed.", exc, {"entry_count": len(data) if isinstance(data, list) else None})
            raise
        except Exception:
            _log_unexpected_issue("save_recycle_bin", "Saving recycle-bin data failed unexpectedly.")
            raise

def delete_raw_timetable_entry(index):
    with STORAGE_LOCK:
        try:
            raw = _read_timetable_file()
            if not isinstance(index, int): raise TimetableValidationError("index must be an integer.")
            if index < 0 or index >= len(raw): raise TimetableNotFoundError(f"No raw timetable entry found at index {index}.")
            deleted = copy.deepcopy(raw.pop(index))
            _atomic_write_json_list_file(TIMETABLE_FILE, raw)
            clear_storage_cache()
            log_event(LOGGER, "Deleted an invalid raw timetable entry.", what="Removed a malformed timetable row directly from storage.", where=_where("delete_raw_timetable_entry"), why="Startup repair removed a stored row that could not be kept safely.", context={"index": index, "entry": _safe_entry_context(deleted)})
            return deleted
        except TimetableError as exc:
            _log_known_issue("delete_raw_timetable_entry", "Deleting a raw timetable entry failed.", exc, {"index": index})
            raise
        except Exception:
            _log_unexpected_issue("delete_raw_timetable_entry", "Deleting a raw timetable entry failed unexpectedly.", {"index": index})
            raise

def repair_timetable_entry(index, entry_id, day, start_time, end_time, class_name, classroom_url):
    with STORAGE_LOCK:
        try:
            raw = _read_timetable_file()
            if not isinstance(index, int): raise TimetableValidationError("index must be an integer.")
            if index < 0 or index >= len(raw): raise TimetableNotFoundError(f"No raw timetable entry found at index {index}.")
            repair_entry_id = _find_repair_entry_id(raw, index, entry_id, day)
            raw[index] = build_validated_entry(repair_entry_id, day, start_time, end_time, class_name, classroom_url)
            _atomic_write_json_list_file(TIMETABLE_FILE, raw)
            clear_storage_cache()
            log_event(LOGGER, "Repaired an invalid timetable entry in storage.", what="Updated a malformed stored timetable row.", where=_where("repair_timetable_entry"), why="Startup repair replaced invalid stored values with validated values.", context={"index": index, "entry": _safe_entry_context(raw[index])})
            return copy.deepcopy(raw[index])
        except TimetableError as exc:
            _log_known_issue("repair_timetable_entry", "Repairing a raw timetable entry failed.", exc, {"index": index, "entry_id": entry_id})
            raise
        except Exception:
            _log_unexpected_issue("repair_timetable_entry", "Repairing a raw timetable entry failed unexpectedly.", {"index": index, "entry_id": entry_id})
            raise

def add_entry(day, start_time, end_time, class_name, url):
    with STORAGE_LOCK:
        try:
            day = _validate_day(day)
            start_time, end_time = validate_time_range(start_time, end_time)
            class_name = _validate_text(class_name, "class_name")
            url = _validate_text(url, "classroom_url")
            timetable = _get_timetable_ref()
            _, slot_to_indexes, prefix_to_numbers = _get_timetable_indexes()
            _ensure_slot_available(slot_to_indexes, day, start_time, end_time)
            prefix = day[:3].lower()
            new_entry = {"id": f"{prefix}_{_get_first_available_number(prefix_to_numbers.get(prefix, set()))}", "day": day, "start_time": start_time, "end_time": end_time, "class_name": class_name, "classroom_url": url, "joined": False}
            timetable.append(new_entry)
            validated = _validate_timetable_entries(timetable)
            _atomic_write_json_list_file(TIMETABLE_FILE, validated)
            _refresh_timetable_cache(validated)
            log_event(LOGGER, "Added a timetable entry.", what="Created a new timetable slot.", where=_where("add_entry"), why="A new class entry was validated and saved.", context={"entry": _safe_entry_context(new_entry)})
            return _clone_entry(new_entry)
        except TimetableError as exc:
            _log_known_issue("add_entry", "Adding a timetable entry failed.", exc, {"day": day, "start_time": start_time, "end_time": end_time, "class_name": class_name})
            raise
        except Exception:
            _log_unexpected_issue("add_entry", "Adding a timetable entry failed unexpectedly.", {"day": day, "start_time": start_time, "end_time": end_time, "class_name": class_name})
            raise

def get_entry_by_id(entry_id):
    with STORAGE_LOCK:
        try:
            entry_id = _validate_entry_id(entry_id)
            timetable = _get_timetable_ref()
            id_to_index, _, _ = _get_timetable_indexes()
            if entry_id not in id_to_index: raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
            return _clone_entry(timetable[id_to_index[entry_id]])
        except TimetableError as exc:
            _log_known_issue("get_entry_by_id", "Entry lookup failed.", exc, {"entry_id": entry_id})
            raise
        except Exception:
            _log_unexpected_issue("get_entry_by_id", "Entry lookup failed unexpectedly.", {"entry_id": entry_id})
            raise

def get_recycle_record_by_id(recycle_id):
    with STORAGE_LOCK:
        try:
            recycle_id = _validate_recycle_id(recycle_id)
            recycle_bin = _get_recycle_bin_ref()
            recycle_index_map = _get_recycle_index_map()
            if recycle_id not in recycle_index_map: raise TimetableNotFoundError(f"No recycle bin entry found with recycle_id '{recycle_id}'.")
            return _clone_recycle_record(recycle_bin[recycle_index_map[recycle_id]])
        except TimetableError as exc:
            _log_known_issue("get_recycle_record_by_id", "Recycle-bin lookup failed.", exc, {"recycle_id": recycle_id})
            raise
        except Exception:
            _log_unexpected_issue("get_recycle_record_by_id", "Recycle-bin lookup failed unexpectedly.", {"recycle_id": recycle_id})
            raise

def edit_entry(entry_id, **kwargs):
    with STORAGE_LOCK:
        try:
            entry_id = _validate_entry_id(entry_id)
            updates = _normalize_update_fields(kwargs)
            if not updates: raise TimetableValidationError("Provide at least one field to update.")
            timetable = _get_timetable_ref()
            id_to_index, slot_to_indexes, _ = _get_timetable_indexes()
            if entry_id not in id_to_index: raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
            current = timetable[id_to_index[entry_id]]
            updated = _validate_entry({"id": current["id"], "day": updates.get("day", current["day"]), "start_time": updates.get("start_time", current["start_time"]), "end_time": updates.get("end_time", current["end_time"]), "class_name": updates.get("class_name", current["class_name"]), "classroom_url": updates.get("classroom_url", current["classroom_url"]), "joined": current["joined"]})
            _ensure_slot_available(slot_to_indexes, updated["day"], updated["start_time"], updated["end_time"], ignore_entry_id=entry_id, id_to_index=id_to_index)
            timetable[id_to_index[entry_id]] = updated
            validated = _validate_timetable_entries(timetable)
            _atomic_write_json_list_file(TIMETABLE_FILE, validated)
            _refresh_timetable_cache(validated)
            log_event(LOGGER, "Edited a timetable entry.", what="Updated an existing timetable slot.", where=_where("edit_entry"), why="User or UI changes were validated and saved.", context={"entry_id": entry_id, "updated_fields": sorted(updates), "entry": _safe_entry_context(updated)})
            return _clone_entry(updated)
        except TimetableError as exc:
            _log_known_issue("edit_entry", "Editing a timetable entry failed.", exc, {"entry_id": entry_id, "updated_fields": sorted(kwargs)})
            raise
        except Exception:
            _log_unexpected_issue("edit_entry", "Editing a timetable entry failed unexpectedly.", {"entry_id": entry_id, "updated_fields": sorted(kwargs)})
            raise

def set_entry_joined_status(entry_id, joined):
    with STORAGE_LOCK:
        try:
            entry_id = _validate_entry_id(entry_id)
            joined = _validate_joined_flag(joined)
            timetable = _get_timetable_ref()
            id_to_index, _, _ = _get_timetable_indexes()
            if entry_id not in id_to_index: raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
            updated = _clone_entry(timetable[id_to_index[entry_id]])
            updated["joined"] = joined
            timetable[id_to_index[entry_id]] = updated
            validated = _validate_timetable_entries(timetable)
            _atomic_write_json_list_file(TIMETABLE_FILE, validated)
            _refresh_timetable_cache(validated)
            log_event(LOGGER, "Updated joined status for a timetable entry.", what="Marked a class as joined or not joined.", where=_where("set_entry_joined_status"), why="Background automation or future scheduler state changed.", context={"entry_id": entry_id, "joined": joined})
            return _clone_entry(updated)
        except TimetableError as exc:
            _log_known_issue("set_entry_joined_status", "Updating joined status failed.", exc, {"entry_id": entry_id, "joined": joined})
            raise
        except Exception:
            _log_unexpected_issue("set_entry_joined_status", "Updating joined status failed unexpectedly.", {"entry_id": entry_id, "joined": joined})
            raise

def delete_entry(entry_id, allow_duplicate_slots=False):
    with STORAGE_LOCK:
        try:
            entry_id = _validate_entry_id(entry_id)
            timetable = _get_timetable_ref(allow_duplicate_slots=allow_duplicate_slots)
            recycle_bin = _get_recycle_bin_ref()
            id_to_index, _, _ = _get_timetable_indexes(allow_duplicate_slots=allow_duplicate_slots)
            if entry_id not in id_to_index: raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
            deleted = _clone_entry(timetable.pop(id_to_index[entry_id]))
            next_bin = max([int(r["recycle_id"].split("_")[1]) for r in recycle_bin], default=0) + 1
            record = {"recycle_id": f"bin_{next_bin}", "deleted_at": datetime.now().isoformat(timespec="seconds"), "entry": deleted}
            recycle_bin.append(record)
            validated_timetable = _validate_timetable_entries(timetable, allow_duplicate_slots=allow_duplicate_slots)
            validated_recycle = _validate_recycle_bin(recycle_bin)
            _atomic_write_json_list_file(TIMETABLE_FILE, validated_timetable)
            _atomic_write_json_list_file(RECYCLE_BIN_FILE, validated_recycle)
            _refresh_timetable_cache(validated_timetable)
            _refresh_recycle_bin_cache(validated_recycle)
            log_event(LOGGER, "Moved a timetable entry to the recycle bin.", what="Soft-deleted a timetable entry.", where=_where("delete_entry"), why="The entry was removed from the active timetable but kept for recovery.", context={"entry_id": entry_id, "recycle_record": _safe_record_context(record)})
            return _clone_recycle_record(record)
        except TimetableError as exc:
            _log_known_issue("delete_entry", "Deleting a timetable entry failed.", exc, {"entry_id": entry_id, "allow_duplicate_slots": allow_duplicate_slots})
            raise
        except Exception:
            _log_unexpected_issue("delete_entry", "Deleting a timetable entry failed unexpectedly.", {"entry_id": entry_id, "allow_duplicate_slots": allow_duplicate_slots})
            raise

def restore_entry(recycle_id, allow_duplicate_slots=False):
    with STORAGE_LOCK:
        try:
            recycle_id = _validate_recycle_id(recycle_id)
            timetable = _get_timetable_ref(allow_duplicate_slots=allow_duplicate_slots)
            recycle_bin = _get_recycle_bin_ref()
            recycle_index_map = _get_recycle_index_map()
            if recycle_id not in recycle_index_map: raise TimetableNotFoundError(f"No recycle bin entry found with recycle_id '{recycle_id}'.")
            entry = _clone_entry(recycle_bin[recycle_index_map[recycle_id]]["entry"])
            id_to_index, slot_to_indexes, _ = _get_timetable_indexes(allow_duplicate_slots=allow_duplicate_slots)
            if entry["id"] in id_to_index: raise TimetableConflictError(f"Cannot restore entry because id '{entry['id']}' already exists in the timetable.")
            _ensure_slot_available(slot_to_indexes, entry["day"], entry["start_time"], entry["end_time"])
            timetable.append(entry)
            recycle_bin.pop(recycle_index_map[recycle_id])
            validated_timetable = _validate_timetable_entries(timetable, allow_duplicate_slots=allow_duplicate_slots)
            validated_recycle = _validate_recycle_bin(recycle_bin)
            _atomic_write_json_list_file(TIMETABLE_FILE, validated_timetable)
            _atomic_write_json_list_file(RECYCLE_BIN_FILE, validated_recycle)
            _refresh_timetable_cache(validated_timetable)
            _refresh_recycle_bin_cache(validated_recycle)
            log_event(LOGGER, "Restored a recycle-bin entry into the timetable.", what="Recovered a previously deleted timetable entry.", where=_where("restore_entry"), why="The recycle-bin record passed validation and no conflicts blocked restoration.", context={"recycle_id": recycle_id, "entry": _safe_entry_context(entry)})
            return _clone_entry(entry)
        except TimetableError as exc:
            _log_known_issue("restore_entry", "Restoring a recycle-bin entry failed.", exc, {"recycle_id": recycle_id, "allow_duplicate_slots": allow_duplicate_slots})
            raise
        except Exception:
            _log_unexpected_issue("restore_entry", "Restoring a recycle-bin entry failed unexpectedly.", {"recycle_id": recycle_id, "allow_duplicate_slots": allow_duplicate_slots})
            raise

def permanently_delete_recycle_entry(recycle_id):
    with STORAGE_LOCK:
        try:
            recycle_id = _validate_recycle_id(recycle_id)
            recycle_bin = _get_recycle_bin_ref()
            recycle_index_map = _get_recycle_index_map()
            if recycle_id not in recycle_index_map: raise TimetableNotFoundError(f"No recycle bin entry found with recycle_id '{recycle_id}'.")
            deleted = _clone_recycle_record(recycle_bin.pop(recycle_index_map[recycle_id]))
            validated_recycle = _validate_recycle_bin(recycle_bin)
            _atomic_write_json_list_file(RECYCLE_BIN_FILE, validated_recycle)
            _refresh_recycle_bin_cache(validated_recycle)
            log_event(LOGGER, "Permanently deleted a recycle-bin record.", what="Removed a recycle-bin entry permanently.", where=_where("permanently_delete_recycle_entry"), why="The recycle-bin record was intentionally removed and cannot be restored now.", context={"recycle_record": _safe_record_context(deleted)})
            return deleted
        except TimetableError as exc:
            _log_known_issue("permanently_delete_recycle_entry", "Permanent recycle-bin deletion failed.", exc, {"recycle_id": recycle_id})
            raise
        except Exception:
            _log_unexpected_issue("permanently_delete_recycle_entry", "Permanent recycle-bin deletion failed unexpectedly.", {"recycle_id": recycle_id})
            raise
