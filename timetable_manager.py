import copy, json, os, re, tempfile, threading
from datetime import datetime

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
STORAGE_LOCK = threading.RLock()
SHUTDOWN_EVENT = threading.Event()


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

def _validate_time(value, field_name):
    value = _validate_text(value, field_name)
    if not TIME_PATTERN.fullmatch(value):
        raise TimetableValidationError(f"{field_name} must be in HH:MM format.")
    try:
        datetime.strptime(value, "%H:%M")
    except ValueError as exc:
        raise TimetableValidationError(f"{field_name} must be in HH:MM format.") from exc
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
    start_time = _validate_time(start_time, "start_time")
    end_time = _validate_time(end_time, "end_time")
    gap = (datetime.strptime(end_time, "%H:%M") - datetime.strptime(start_time, "%H:%M")).total_seconds() / 3600
    if gap < 1:
        raise TimetableValidationError("The gap between start_time and end_time must be at least 1 hour.")
    if gap > 3:
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
        raise FileNotFoundError(f"{path} was not found.") from exc
    except json.JSONDecodeError as exc:
        raise TimetableValidationError(f"{path} contains invalid JSON.") from exc
    except OSError as exc:
        raise TimetableStorageError(f"Could not read {path}: {exc}") from exc
    if not isinstance(data, list):
        raise TimetableValidationError(f"{path} must contain a list of entries.")
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
        raise TimetableValidationError(f"{path} contains non-JSON-serializable values: {exc}") from exc
    except OSError as exc:
        raise TimetableStorageError(f"Could not save {path}: {exc}") from exc
    finally:
        if temp_path and os.path.exists(temp_path):
            try: os.remove(temp_path)
            except OSError: pass

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
        if issues: raise InvalidTimetableEntriesError(issues)
        TIMETABLE_CACHE = _validate_timetable_entries(raw, allow_duplicate_slots=allow_duplicate_slots)
    if not allow_duplicate_slots:
        _, slot_to_indexes, _ = _build_timetable_indexes(TIMETABLE_CACHE)
        _ensure_no_duplicate_slots(TIMETABLE_CACHE, slot_to_indexes)
    return TIMETABLE_CACHE

def _get_recycle_bin_ref():
    global RECYCLE_BIN_CACHE
    if RECYCLE_BIN_CACHE is None:
        try: RECYCLE_BIN_CACHE = _validate_recycle_bin(_read_recycle_bin_file())
        except FileNotFoundError:
            RECYCLE_BIN_CACHE = []
            _atomic_write_json_list_file(RECYCLE_BIN_FILE, RECYCLE_BIN_CACHE)
    return RECYCLE_BIN_CACHE

def load_timetable(allow_duplicate_slots=False):
    with STORAGE_LOCK: return copy.deepcopy(_get_timetable_ref(allow_duplicate_slots=allow_duplicate_slots))

def load_recycle_bin():
    with STORAGE_LOCK: return copy.deepcopy(_get_recycle_bin_ref())

def load_raw_timetable():
    with STORAGE_LOCK: return _read_timetable_file()

def inspect_timetable_entry_issues():
    with STORAGE_LOCK: return _inspect_timetable_entry_issues_from_data(_read_timetable_file())

def initialize_storage():
    with STORAGE_LOCK:
        clear_storage_cache()
        SHUTDOWN_EVENT.clear()
        _get_timetable_ref()
        _get_recycle_bin_ref()

def clear_storage_cache():
    global TIMETABLE_CACHE, RECYCLE_BIN_CACHE
    TIMETABLE_CACHE = None
    RECYCLE_BIN_CACHE = None

def get_shutdown_event(): return SHUTDOWN_EVENT
def shutdown_storage(): SHUTDOWN_EVENT.set()

def save_timetable(data):
    global TIMETABLE_CACHE
    with STORAGE_LOCK:
        validated = _validate_timetable_entries(data)
        _atomic_write_json_list_file(TIMETABLE_FILE, validated)
        TIMETABLE_CACHE = validated
        return copy.deepcopy(validated)

def save_recycle_bin(data):
    global RECYCLE_BIN_CACHE
    with STORAGE_LOCK:
        validated = _validate_recycle_bin(data)
        _atomic_write_json_list_file(RECYCLE_BIN_FILE, validated)
        RECYCLE_BIN_CACHE = validated
        return copy.deepcopy(validated)

def delete_raw_timetable_entry(index):
    with STORAGE_LOCK:
        raw = _read_timetable_file()
        if not isinstance(index, int): raise TimetableValidationError("index must be an integer.")
        if index < 0 or index >= len(raw): raise TimetableNotFoundError(f"No raw timetable entry found at index {index}.")
        deleted = copy.deepcopy(raw.pop(index))
        _atomic_write_json_list_file(TIMETABLE_FILE, raw)
        clear_storage_cache()
        return deleted

def repair_timetable_entry(index, entry_id, day, start_time, end_time, class_name, classroom_url):
    with STORAGE_LOCK:
        raw = _read_timetable_file()
        if not isinstance(index, int): raise TimetableValidationError("index must be an integer.")
        if index < 0 or index >= len(raw): raise TimetableNotFoundError(f"No raw timetable entry found at index {index}.")
        repair_entry_id = _find_repair_entry_id(raw, index, entry_id, day)
        raw[index] = build_validated_entry(repair_entry_id, day, start_time, end_time, class_name, classroom_url)
        _atomic_write_json_list_file(TIMETABLE_FILE, raw)
        clear_storage_cache()
        return copy.deepcopy(raw[index])

def add_entry(day, start_time, end_time, class_name, url):
    with STORAGE_LOCK:
        day = _validate_day(day)
        start_time, end_time = validate_time_range(start_time, end_time)
        class_name = _validate_text(class_name, "class_name")
        url = _validate_text(url, "classroom_url")
        timetable = _get_timetable_ref()
        _, slot_to_indexes, prefix_to_numbers = _build_timetable_indexes(timetable)
        _ensure_slot_available(slot_to_indexes, day, start_time, end_time)
        prefix = day[:3].lower()
        new_entry = {"id": f"{prefix}_{_get_first_available_number(prefix_to_numbers.get(prefix, set()))}", "day": day, "start_time": start_time, "end_time": end_time, "class_name": class_name, "classroom_url": url, "joined": False}
        timetable.append(new_entry)
        validated = _validate_timetable_entries(timetable)
        _atomic_write_json_list_file(TIMETABLE_FILE, validated)
        TIMETABLE_CACHE[:] = validated
        return copy.deepcopy(new_entry)

def get_entry_by_id(entry_id):
    with STORAGE_LOCK:
        entry_id = _validate_entry_id(entry_id)
        timetable = _get_timetable_ref()
        id_to_index, _, _ = _build_timetable_indexes(timetable)
        if entry_id not in id_to_index: raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
        return copy.deepcopy(timetable[id_to_index[entry_id]])

def get_recycle_record_by_id(recycle_id):
    with STORAGE_LOCK:
        recycle_id = _validate_recycle_id(recycle_id)
        recycle_bin = _get_recycle_bin_ref()
        recycle_index_map = _build_recycle_index_map(recycle_bin)
        if recycle_id not in recycle_index_map: raise TimetableNotFoundError(f"No recycle bin entry found with recycle_id '{recycle_id}'.")
        return copy.deepcopy(recycle_bin[recycle_index_map[recycle_id]])

def edit_entry(entry_id, **kwargs):
    with STORAGE_LOCK:
        entry_id = _validate_entry_id(entry_id)
        updates = _normalize_update_fields(kwargs)
        if not updates: raise TimetableValidationError("Provide at least one field to update.")
        timetable = _get_timetable_ref()
        id_to_index, slot_to_indexes, _ = _build_timetable_indexes(timetable)
        if entry_id not in id_to_index: raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
        current = timetable[id_to_index[entry_id]]
        updated = _validate_entry({"id": current["id"], "day": updates.get("day", current["day"]), "start_time": updates.get("start_time", current["start_time"]), "end_time": updates.get("end_time", current["end_time"]), "class_name": updates.get("class_name", current["class_name"]), "classroom_url": updates.get("classroom_url", current["classroom_url"]), "joined": current["joined"]})
        _ensure_slot_available(slot_to_indexes, updated["day"], updated["start_time"], updated["end_time"], ignore_entry_id=entry_id, id_to_index=id_to_index)
        timetable[id_to_index[entry_id]] = updated
        validated = _validate_timetable_entries(timetable)
        _atomic_write_json_list_file(TIMETABLE_FILE, validated)
        TIMETABLE_CACHE[:] = validated
        return copy.deepcopy(updated)

def set_entry_joined_status(entry_id, joined):
    with STORAGE_LOCK:
        entry_id = _validate_entry_id(entry_id)
        joined = _validate_joined_flag(joined)
        timetable = _get_timetable_ref()
        id_to_index, _, _ = _build_timetable_indexes(timetable)
        if entry_id not in id_to_index: raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
        updated = copy.deepcopy(timetable[id_to_index[entry_id]])
        updated["joined"] = joined
        timetable[id_to_index[entry_id]] = updated
        validated = _validate_timetable_entries(timetable)
        _atomic_write_json_list_file(TIMETABLE_FILE, validated)
        TIMETABLE_CACHE[:] = validated
        return copy.deepcopy(updated)

def delete_entry(entry_id, allow_duplicate_slots=False):
    with STORAGE_LOCK:
        entry_id = _validate_entry_id(entry_id)
        timetable = _get_timetable_ref(allow_duplicate_slots=allow_duplicate_slots)
        recycle_bin = _get_recycle_bin_ref()
        id_to_index, _, _ = _build_timetable_indexes(timetable)
        if entry_id not in id_to_index: raise TimetableNotFoundError(f"No entry found with id '{entry_id}'.")
        deleted = copy.deepcopy(timetable.pop(id_to_index[entry_id]))
        next_bin = max([int(r["recycle_id"].split("_")[1]) for r in recycle_bin], default=0) + 1
        record = {"recycle_id": f"bin_{next_bin}", "deleted_at": datetime.now().isoformat(timespec="seconds"), "entry": deleted}
        recycle_bin.append(record)
        validated_timetable = _validate_timetable_entries(timetable, allow_duplicate_slots=allow_duplicate_slots)
        validated_recycle = _validate_recycle_bin(recycle_bin)
        _atomic_write_json_list_file(TIMETABLE_FILE, validated_timetable)
        _atomic_write_json_list_file(RECYCLE_BIN_FILE, validated_recycle)
        TIMETABLE_CACHE[:] = validated_timetable
        RECYCLE_BIN_CACHE[:] = validated_recycle
        return copy.deepcopy(record)

def restore_entry(recycle_id, allow_duplicate_slots=False):
    with STORAGE_LOCK:
        recycle_id = _validate_recycle_id(recycle_id)
        timetable = _get_timetable_ref(allow_duplicate_slots=allow_duplicate_slots)
        recycle_bin = _get_recycle_bin_ref()
        recycle_index_map = _build_recycle_index_map(recycle_bin)
        if recycle_id not in recycle_index_map: raise TimetableNotFoundError(f"No recycle bin entry found with recycle_id '{recycle_id}'.")
        entry = copy.deepcopy(recycle_bin[recycle_index_map[recycle_id]]["entry"])
        id_to_index, slot_to_indexes, _ = _build_timetable_indexes(timetable)
        if entry["id"] in id_to_index: raise TimetableConflictError(f"Cannot restore entry because id '{entry['id']}' already exists in the timetable.")
        _ensure_slot_available(slot_to_indexes, entry["day"], entry["start_time"], entry["end_time"])
        timetable.append(entry)
        recycle_bin.pop(recycle_index_map[recycle_id])
        validated_timetable = _validate_timetable_entries(timetable, allow_duplicate_slots=allow_duplicate_slots)
        validated_recycle = _validate_recycle_bin(recycle_bin)
        _atomic_write_json_list_file(TIMETABLE_FILE, validated_timetable)
        _atomic_write_json_list_file(RECYCLE_BIN_FILE, validated_recycle)
        TIMETABLE_CACHE[:] = validated_timetable
        RECYCLE_BIN_CACHE[:] = validated_recycle
        return copy.deepcopy(entry)

def permanently_delete_recycle_entry(recycle_id):
    with STORAGE_LOCK:
        recycle_id = _validate_recycle_id(recycle_id)
        recycle_bin = _get_recycle_bin_ref()
        recycle_index_map = _build_recycle_index_map(recycle_bin)
        if recycle_id not in recycle_index_map: raise TimetableNotFoundError(f"No recycle bin entry found with recycle_id '{recycle_id}'.")
        deleted = copy.deepcopy(recycle_bin.pop(recycle_index_map[recycle_id]))
        validated_recycle = _validate_recycle_bin(recycle_bin)
        _atomic_write_json_list_file(RECYCLE_BIN_FILE, validated_recycle)
        RECYCLE_BIN_CACHE[:] = validated_recycle
        return deleted
