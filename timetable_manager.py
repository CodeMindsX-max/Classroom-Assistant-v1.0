import copy
import json
import os
import re
import tempfile
from datetime import datetime


TIMETABLE_FILE = "timetable.json"
RECYCLE_BIN_FILE = "recycle_bin.json"
VALID_FIELDS = {"day", "start_time", "end_time", "class_name", "classroom_url"}
RECYCLE_ID_PATTERN = re.compile(r"^bin_\d+$")
ENTRY_ID_PATTERN = re.compile(r"^[a-z]{3}_\d+$")
TIME_PATTERN = re.compile(r"^\d{2}:\d{2}$")
VALID_DAYS = {
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
}
VALID_YES_VALUES = {"y", "yes"}
VALID_NO_VALUES = {"n", "no"}


class TimetableError(Exception):
    """Base exception for timetable-related errors."""


class TimetableValidationError(TimetableError, ValueError):
    """Raised when user or stored data does not match validation rules."""


class TimetableStorageError(TimetableError, OSError):
    """Raised when reading or writing storage files fails."""


class TimetableNotFoundError(TimetableError, LookupError):
    """Raised when an entry or recycle record cannot be found."""


class TimetableConflictError(TimetableError, ValueError):
    """Raised when a new change conflicts with existing timetable data."""


class DuplicateTimeSlotError(TimetableConflictError):
    """Raised when duplicate timetable slots already exist in stored data."""

    def __init__(self, duplicates):
        self.duplicates = duplicates
        super().__init__("Duplicate timetable slots found in stored data.")


class InvalidTimetableEntriesError(TimetableValidationError):
    """Raised when stored timetable entries are malformed and need repair."""

    def __init__(self, issues):
        self.issues = issues
        issue_count = len(issues)
        first_error = issues[0]["error"] if issues else "Unknown validation error."
        super().__init__(
            f"Invalid timetable entries found in stored data ({issue_count} issue(s)). "
            f"First issue: {first_error}"
        )


TIMETABLE_CACHE = None
RECYCLE_BIN_CACHE = None


# This small gatekeeper checks simple text input and returns a cleaned string
# so later functions can trust the value they receive.
def _validate_text(value, field_name):
    if not isinstance(value, str):
        raise TimetableValidationError(f"{field_name} must be a string.")

    cleaned_value = value.strip()
    if not cleaned_value:
        raise TimetableValidationError(f"{field_name} cannot be empty.")

    return cleaned_value


# This validates required user text input and returns the cleaned value
# so both CLI and future GUI screens can share one required-field rule.
def validate_required_text(value, field_name):
    return _validate_text(value, field_name)


# This validates optional text input and returns a cleaned value or an empty string
# so edit screens can keep old values when the user leaves a field blank.
def validate_optional_text(value):
    if not isinstance(value, str):
        raise TimetableValidationError("Optional input must be a string.")
    return value.strip()


# This keeps recycle bin ids in one safe pattern and returns the cleaned id
# so restore and permanent delete work only with valid recycle references.
def _validate_recycle_id(value):
    cleaned_value = _validate_text(value, "recycle_id")
    if not RECYCLE_ID_PATTERN.fullmatch(cleaned_value):
        raise TimetableValidationError(
            "recycle_id must be in the format 'bin_<number>', for example 'bin_1'."
        )
    return cleaned_value


# This validates recycle-bin ids for UI or API input and returns the cleaned id
# so restore and permanent delete use one shared rule everywhere.
def validate_recycle_id(value):
    return _validate_recycle_id(value)


# This validates one timetable entry id and returns the cleaned id
# so id-based operations always work with the expected stored format.
def _validate_entry_id(value):
    cleaned_value = _validate_text(value, "entry_id")
    if not ENTRY_ID_PATTERN.fullmatch(cleaned_value):
        raise TimetableValidationError(
            "entry_id must be in the format '<day_prefix>_<number>', for example 'mon_1'."
        )
    return cleaned_value


# This validates timetable entry ids for UI or API input and returns the cleaned id
# so edit and delete screens use the same strong id rule as stored data.
def validate_entry_id(value):
    return _validate_entry_id(value)


# This validates one time value and returns the cleaned HH:MM string
# so all timetable operations work with one consistent time format.
def _validate_time(value, field_name):
    cleaned_value = _validate_text(value, field_name)

    if not TIME_PATTERN.fullmatch(cleaned_value):
        raise TimetableValidationError(f"{field_name} must be in HH:MM format.")

    try:
        datetime.strptime(cleaned_value, "%H:%M")
    except ValueError as exc:
        raise TimetableValidationError(f"{field_name} must be in HH:MM format.") from exc

    return cleaned_value


# This validates one time value for user-facing input and returns the cleaned result
# so CLI and GUI forms share the same HH:MM format enforcement.
def validate_time_input(value, field_name="time"):
    return _validate_time(value, field_name)


# This validates the entered day name and returns a normalized day
# so stored timetable data always uses proper weekday names.
def _validate_day(value):
    cleaned_value = _validate_text(value, "day").title()
    if cleaned_value not in VALID_DAYS:
        valid_days_text = ", ".join(sorted(VALID_DAYS))
        raise TimetableValidationError(f"day must be a valid day name: {valid_days_text}.")
    return cleaned_value


# This validates one weekday for user-facing input and returns the normalized day
# so every interface uses the same allowed weekday names.
def validate_day_input(value):
    return _validate_day(value)


# This validates yes/no style answers and returns a boolean result
# so confirmations can share one parser across CLI and future GUI adapters.
def validate_yes_no_input(value):
    cleaned_value = _validate_text(value, "confirmation").lower()
    if cleaned_value in VALID_YES_VALUES:
        return True
    if cleaned_value in VALID_NO_VALUES:
        return False
    raise TimetableValidationError("Please enter y or n.")


# This builds one reusable slot key for lookup and conflict checks
# so slot comparisons stay consistent across add, edit, load, and restore.
def _slot_key(day, start_time, end_time):
    return day, start_time, end_time


# This checks the time gap rule and returns the validated start and end times
# so add and edit can safely build entries with allowed durations only.
def validate_time_range(start_time, end_time):
    validated_start = _validate_time(start_time, "start_time")
    validated_end = _validate_time(end_time, "end_time")

    start_obj = datetime.strptime(validated_start, "%H:%M")
    end_obj = datetime.strptime(validated_end, "%H:%M")
    gap_in_hours = (end_obj - start_obj).total_seconds() / 3600

    if gap_in_hours < 1:
        raise TimetableValidationError(
            "The gap between start_time and end_time must be at least 1 hour."
        )
    if gap_in_hours > 3:
        raise TimetableValidationError(
            "The gap between start_time and end_time must not be more than 3 hours."
        )

    return validated_start, validated_end


# This combines all field validations and returns a fully cleaned entry shape
# so higher-level functions can save trusted timetable data.
def _validate_entry_fields(day, start_time, end_time, class_name, classroom_url):
    validated_day = _validate_day(day)
    validated_start, validated_end = validate_time_range(start_time, end_time)
    validated_name = _validate_text(class_name, "class_name")
    validated_url = _validate_text(classroom_url, "classroom_url")

    return validated_day, validated_start, validated_end, validated_name, validated_url


# This prepares edit fields, maps alias names, and returns normalized updates
# so edit_entry can accept friendly input without corrupting field names.
def _normalize_update_fields(kwargs):
    normalized = dict(kwargs)

    if "url" in normalized:
        if "classroom_url" in normalized:
            raise TimetableValidationError("Use either 'url' or 'classroom_url', not both.")
        normalized["classroom_url"] = normalized.pop("url")

    invalid_fields = set(normalized) - VALID_FIELDS
    if invalid_fields:
        invalid_list = ", ".join(sorted(invalid_fields))
        raise TimetableValidationError(f"Invalid field(s): {invalid_list}")

    return normalized


# This validates one timetable entry object and returns a clean copy
# so the rest of the system works from one trusted entry structure.
def _validate_entry(entry):
    if not isinstance(entry, dict):
        raise TimetableValidationError("Each timetable entry must be an object.")

    validated_id = _validate_entry_id(entry.get("id"))
    (
        validated_day,
        validated_start,
        validated_end,
        validated_name,
        validated_url,
    ) = _validate_entry_fields(
        entry.get("day"),
        entry.get("start_time"),
        entry.get("end_time"),
        entry.get("class_name"),
        entry.get("classroom_url"),
    )

    return {
        "id": validated_id,
        "day": validated_day,
        "start_time": validated_start,
        "end_time": validated_end,
        "class_name": validated_name,
        "classroom_url": validated_url,
    }


# This validates a full entry payload and returns the clean entry dictionary
# so CLI and future GUI repair flows can build one trusted timetable record.
def build_validated_entry(entry_id, day, start_time, end_time, class_name, classroom_url):
    return _validate_entry(
        {
            "id": entry_id,
            "day": day,
            "start_time": start_time,
            "end_time": end_time,
            "class_name": class_name,
            "classroom_url": classroom_url,
        }
    )


# This validates one recycle-bin record and returns a clean copy
# so deleted entries remain safe to inspect, restore, or purge later.
def _validate_recycle_record(record):
    if not isinstance(record, dict):
        raise TimetableValidationError("Each recycle bin record must be an object.")

    validated_recycle_id = _validate_recycle_id(record.get("recycle_id"))
    deleted_at = _validate_text(record.get("deleted_at"), "deleted_at")

    try:
        datetime.fromisoformat(deleted_at)
    except ValueError as exc:
        raise TimetableValidationError("deleted_at must be a valid ISO datetime string.") from exc

    validated_entry = _validate_entry(record.get("entry"))

    return {
        "recycle_id": validated_recycle_id,
        "deleted_at": deleted_at,
        "entry": validated_entry,
    }


# This scans entries once and returns reusable indexes for ids, slots, and prefixes
# so the code avoids repeated linear scans during reads, updates, and id generation.
def _build_timetable_indexes(timetable):
    id_to_index = {}
    slot_to_indexes = {}
    prefix_to_numbers = {}

    for index, entry in enumerate(timetable):
        entry_id = entry["id"]
        if entry_id in id_to_index:
            raise TimetableValidationError(f"Duplicate id found in timetable: {entry_id}")
        id_to_index[entry_id] = index

        slot = _slot_key(entry["day"], entry["start_time"], entry["end_time"])
        slot_to_indexes.setdefault(slot, []).append(index)

        prefix, suffix = entry_id.split("_", 1)
        prefix_to_numbers.setdefault(prefix, set()).add(int(suffix))

    return id_to_index, slot_to_indexes, prefix_to_numbers


# This builds a recycle-id index map and checks for duplicate recycle ids
# so recycle lookup stays fast and invalid stored recycle data is caught early.
def _build_recycle_index_map(recycle_bin):
    recycle_index_map = {}

    for index, record in enumerate(recycle_bin):
        recycle_id = record["recycle_id"]
        if recycle_id in recycle_index_map:
            raise TimetableValidationError(
                f"Duplicate recycle_id found in recycle bin: {recycle_id}"
            )
        recycle_index_map[recycle_id] = index

    return recycle_index_map


# This finds the first free positive number for one day prefix and returns it
# so deleted ids like mon_1 can be reused instead of always creating mon_3.
def _get_first_available_number(used_numbers):
    candidate = 1
    while candidate in used_numbers:
        candidate += 1
    return candidate


# This checks that every slot appears only once and returns nothing on success
# so stored or cached timetable data stays conflict-free for normal operations.
def _ensure_no_duplicate_slots_from_index(timetable, slot_to_indexes):
    duplicates = []

    for indexes in slot_to_indexes.values():
        if len(indexes) > 1:
            duplicates.append([copy.deepcopy(timetable[index]) for index in indexes])

    if duplicates:
        raise DuplicateTimeSlotError(duplicates)


# This checks whether a day/time slot is free and raises if it is occupied
# so add, edit, and restore cannot create duplicate classes in one slot.
def _ensure_slot_available_from_index(slot_to_indexes, day, start_time, end_time, ignore_entry_id=None, id_to_index=None):
    slot = _slot_key(day, start_time, end_time)
    indexes = slot_to_indexes.get(slot, [])

    for index in indexes:
        if ignore_entry_id and id_to_index and id_to_index.get(ignore_entry_id) == index:
            continue
        raise TimetableConflictError(
            f"A timetable entry already exists for {day} from {start_time} to {end_time}."
        )


# This reads a JSON list file and returns its list content
# so both timetable and recycle bin can share one safe file reader.
def _read_json_list_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"{file_path} was not found.") from exc
    except json.JSONDecodeError as exc:
        raise TimetableValidationError(f"{file_path} contains invalid JSON.") from exc
    except OSError as exc:
        raise TimetableStorageError(f"Could not read {file_path}: {exc}") from exc

    if not isinstance(data, list):
        raise TimetableValidationError(f"{file_path} must contain a list of entries.")

    return data


# This thin wrapper reads the timetable source file and returns raw list data
# so timetable loading keeps one clear file-read path.
def _read_timetable_file():
    return _read_json_list_file(TIMETABLE_FILE)


# This thin wrapper reads the recycle-bin source file and returns raw list data
# so recycle-bin loading keeps one clear file-read path.
def _read_recycle_bin_file():
    return _read_json_list_file(RECYCLE_BIN_FILE)


# This writes JSON through a temporary file and swaps it into place
# so saves are safer against partial writes or interrupted program exits.
def _atomic_write_json_list_file(file_path, data):
    directory = os.path.dirname(os.path.abspath(file_path)) or "."
    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=directory,
            delete=False,
            suffix=".tmp",
        ) as temp_file:
            json.dump(data, temp_file, indent=2, ensure_ascii=False)
            temp_path = temp_file.name

        os.replace(temp_path, file_path)
    except TypeError as exc:
        raise TimetableValidationError(
            f"{file_path} contains non-JSON-serializable values: {exc}"
        ) from exc
    except OSError as exc:
        raise TimetableStorageError(f"Could not save {file_path}: {exc}") from exc
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


# This reads raw timetable JSON rows without full validation
# so startup repair flows can inspect and fix malformed stored entries safely.
def load_raw_timetable():
    data = _read_timetable_file()
    if not isinstance(data, list):
        raise TimetableValidationError(f"{TIMETABLE_FILE} must contain a list of entries.")
    return data


# This inspects raw timetable rows and returns a list of invalid-entry issues
# so the app can guide the user to edit or delete broken stored data at startup.
def _inspect_timetable_entry_issues_from_data(raw_timetable):
    issues = []

    for index, entry in enumerate(raw_timetable):
        try:
            _validate_entry(entry)
        except TimetableError as exc:
            issues.append(
                {
                    "index": index,
                    "entry": copy.deepcopy(entry),
                    "error": str(exc),
                }
            )

    return issues


# This inspects raw timetable rows and returns a list of invalid-entry issues
# so the app can guide the user to edit or delete broken stored data at startup.
def inspect_timetable_entry_issues():
    return _inspect_timetable_entry_issues_from_data(load_raw_timetable())


# This removes one raw timetable entry by index and writes the remaining raw data back
# so malformed stored rows can be deleted before normal validated loading resumes.
def delete_raw_timetable_entry(index):
    raw_timetable = load_raw_timetable()

    if not isinstance(index, int):
        raise TimetableValidationError("index must be an integer.")
    if index < 0 or index >= len(raw_timetable):
        raise TimetableNotFoundError(f"No raw timetable entry found at index {index}.")

    deleted_entry = copy.deepcopy(raw_timetable.pop(index))
    _atomic_write_json_list_file(TIMETABLE_FILE, raw_timetable)
    clear_storage_cache()
    return deleted_entry


# This replaces one raw timetable entry with a validated entry and writes raw data back
# so malformed stored rows can be repaired even before the whole file is clean.
def repair_timetable_entry(index, entry_id, day, start_time, end_time, class_name, classroom_url):
    raw_timetable = load_raw_timetable()

    if not isinstance(index, int):
        raise TimetableValidationError("index must be an integer.")
    if index < 0 or index >= len(raw_timetable):
        raise TimetableNotFoundError(f"No raw timetable entry found at index {index}.")

    validated_entry = build_validated_entry(
        entry_id,
        day,
        start_time,
        end_time,
        class_name,
        classroom_url,
    )
    raw_timetable[index] = validated_entry
    _atomic_write_json_list_file(TIMETABLE_FILE, raw_timetable)
    clear_storage_cache()
    return copy.deepcopy(validated_entry)


# This validates every saved timetable record and returns a clean list copy
# so load and save only work with trusted timetable data.
def _validate_timetable_entries(data, allow_duplicate_slots=False):
    if not isinstance(data, list):
        raise TimetableValidationError("Timetable data must be a list.")

    validated_entries = [_validate_entry(entry) for entry in data]
    _, slot_to_indexes, _ = _build_timetable_indexes(validated_entries)
    if not allow_duplicate_slots:
        _ensure_no_duplicate_slots_from_index(validated_entries, slot_to_indexes)
    return validated_entries


# This validates recycle bin records and returns a clean list copy
# so deleted data remains safe for later restore and cleanup operations.
def _validate_recycle_bin(recycle_bin):
    if not isinstance(recycle_bin, list):
        raise TimetableValidationError("Recycle bin data must be a list.")

    validated_records = []

    for record in recycle_bin:
        validated_record = _validate_recycle_record(record)
        validated_records.append(validated_record)

    _build_recycle_index_map(validated_records)
    return validated_records


# This creates a detached copy of list data before exposing it to callers
# so read helpers do not leak internal cache references by mistake.
def _clone_list_data(data):
    return copy.deepcopy(data)


# This loads timetable data into memory one time, validates it, and returns the cache
# so repeated lookups do not keep hitting the disk.
def load_timetable(allow_duplicate_slots=False):
    global TIMETABLE_CACHE

    if TIMETABLE_CACHE is None:
        raw_timetable = _read_timetable_file()
        issues = _inspect_timetable_entry_issues_from_data(raw_timetable)
        if issues:
            raise InvalidTimetableEntriesError(issues)
        TIMETABLE_CACHE = _validate_timetable_entries(
            raw_timetable,
            allow_duplicate_slots=allow_duplicate_slots,
        )

    if not allow_duplicate_slots:
        _, slot_to_indexes, _ = _build_timetable_indexes(TIMETABLE_CACHE)
        _ensure_no_duplicate_slots_from_index(TIMETABLE_CACHE, slot_to_indexes)

    return TIMETABLE_CACHE


# This loads recycle-bin data into memory one time, validates it, and returns the cache
# so restore and permanent delete can work without repeated file reads.
def load_recycle_bin():
    global RECYCLE_BIN_CACHE

    if RECYCLE_BIN_CACHE is None:
        try:
            RECYCLE_BIN_CACHE = _validate_recycle_bin(_read_recycle_bin_file())
        except FileNotFoundError:
            RECYCLE_BIN_CACHE = []
            _atomic_write_json_list_file(RECYCLE_BIN_FILE, RECYCLE_BIN_CACHE)

    return RECYCLE_BIN_CACHE


# This resets in-memory caches and then loads both JSON sources once
# so the application can validate storage at startup before user actions begin.
def initialize_storage():
    clear_storage_cache()
    load_timetable()
    load_recycle_bin()


# This clears in-memory caches so tests or manual reload flows can force fresh disk reads
# when the source files are changed outside the current process.
def clear_storage_cache():
    global TIMETABLE_CACHE, RECYCLE_BIN_CACHE

    TIMETABLE_CACHE = None
    RECYCLE_BIN_CACHE = None


# This validates and writes timetable data back to disk and cache
# so add, edit, and restore changes become permanent safely.
def save_timetable(data):
    global TIMETABLE_CACHE

    validated_entries = _validate_timetable_entries(data)
    _atomic_write_json_list_file(TIMETABLE_FILE, validated_entries)
    TIMETABLE_CACHE = _clone_list_data(validated_entries)
    return TIMETABLE_CACHE


# This validates and writes recycle-bin data back to disk and cache
# so deleted items are stored safely for recovery or permanent cleanup.
def save_recycle_bin(data):
    global RECYCLE_BIN_CACHE

    validated_records = _validate_recycle_bin(data)
    _atomic_write_json_list_file(RECYCLE_BIN_FILE, validated_records)
    RECYCLE_BIN_CACHE = _clone_list_data(validated_records)
    return RECYCLE_BIN_CACHE


# This creates one new timetable entry, saves it, and returns the new record
# so the caller can show the created id and details to the user.
def add_entry(day, start_time, end_time, class_name, url):
    validated_day, validated_start, validated_end, validated_name, validated_url = (
        _validate_entry_fields(day, start_time, end_time, class_name, url)
    )

    timetable = load_timetable()
    id_to_index, slot_to_indexes, prefix_to_numbers = _build_timetable_indexes(timetable)
    _ensure_slot_available_from_index(slot_to_indexes, validated_day, validated_start, validated_end)

    day_prefix = validated_day[:3].lower()
    next_number = _get_first_available_number(prefix_to_numbers.get(day_prefix, set()))
    new_entry = {
        "id": f"{day_prefix}_{next_number}",
        "day": validated_day,
        "start_time": validated_start,
        "end_time": validated_end,
        "class_name": validated_name,
        "classroom_url": validated_url,
    }

    timetable.append(new_entry)
    save_timetable(timetable)
    return copy.deepcopy(new_entry)


# This fetches one timetable entry by id and returns a safe copy of that entry
# so callers can inspect it without mutating the internal cache directly.
def get_entry_by_id(entry_id):
    validated_id = _validate_entry_id(entry_id)
    timetable = load_timetable()
    id_to_index, _, _ = _build_timetable_indexes(timetable)

    if validated_id not in id_to_index:
        raise TimetableNotFoundError(f"No entry found with id '{validated_id}'.")

    return copy.deepcopy(timetable[id_to_index[validated_id]])


# This fetches one recycle-bin record by recycle id and returns a safe copy
# so restore and permanent delete can confirm the target item first.
def get_recycle_record_by_id(recycle_id):
    validated_recycle_id = _validate_recycle_id(recycle_id)
    recycle_bin = load_recycle_bin()
    recycle_index_map = {
        record["recycle_id"]: index for index, record in enumerate(recycle_bin)
    }

    if validated_recycle_id not in recycle_index_map:
        raise TimetableNotFoundError(
            f"No recycle bin entry found with recycle_id '{validated_recycle_id}'."
        )

    return copy.deepcopy(recycle_bin[recycle_index_map[validated_recycle_id]])


# This updates one existing timetable entry, saves the timetable,
# and returns the updated entry so the caller can show the new result.
def edit_entry(entry_id, **kwargs):
    validated_id = _validate_entry_id(entry_id)
    updates = _normalize_update_fields(kwargs)

    if not updates:
        raise TimetableValidationError("Provide at least one field to update.")

    timetable = load_timetable()
    id_to_index, slot_to_indexes, _ = _build_timetable_indexes(timetable)

    if validated_id not in id_to_index:
        raise TimetableNotFoundError(f"No entry found with id '{validated_id}'.")

    entry_index = id_to_index[validated_id]
    current_entry = timetable[entry_index]
    updated_entry = {
        "id": current_entry["id"],
        "day": updates.get("day", current_entry["day"]),
        "start_time": updates.get("start_time", current_entry["start_time"]),
        "end_time": updates.get("end_time", current_entry["end_time"]),
        "class_name": updates.get("class_name", current_entry["class_name"]),
        "classroom_url": updates.get("classroom_url", current_entry["classroom_url"]),
    }

    validated_entry = _validate_entry(updated_entry)
    _ensure_slot_available_from_index(
        slot_to_indexes,
        validated_entry["day"],
        validated_entry["start_time"],
        validated_entry["end_time"],
        ignore_entry_id=validated_id,
        id_to_index=id_to_index,
    )

    timetable[entry_index] = validated_entry
    save_timetable(timetable)
    return copy.deepcopy(validated_entry)


# This soft-deletes a timetable entry, moves it into the recycle bin,
# and returns the recycle record so the caller can show where it went.
def delete_entry(entry_id, allow_duplicate_slots=False):
    validated_id = _validate_entry_id(entry_id)
    timetable = load_timetable(allow_duplicate_slots=allow_duplicate_slots)
    recycle_bin = load_recycle_bin()
    id_to_index, _, _ = _build_timetable_indexes(timetable)

    if validated_id not in id_to_index:
        raise TimetableNotFoundError(f"No entry found with id '{validated_id}'.")

    entry_index = id_to_index[validated_id]
    deleted_entry = copy.deepcopy(timetable.pop(entry_index))

    existing_numbers = [
        int(record["recycle_id"].split("_")[1]) for record in recycle_bin
    ]
    next_recycle_number = max(existing_numbers, default=0) + 1

    recycle_record = {
        "recycle_id": f"bin_{next_recycle_number}",
        "deleted_at": datetime.now().isoformat(timespec="seconds"),
        "entry": deleted_entry,
    }

    recycle_bin.append(recycle_record)
    save_timetable(timetable)
    save_recycle_bin(recycle_bin)
    return copy.deepcopy(recycle_record)


# This restores one deleted entry back into the timetable and returns it
# so the caller can confirm the class is active again.
def restore_entry(recycle_id, allow_duplicate_slots=False):
    validated_recycle_id = _validate_recycle_id(recycle_id)
    timetable = load_timetable(allow_duplicate_slots=allow_duplicate_slots)
    recycle_bin = load_recycle_bin()

    recycle_index_map = {
        record["recycle_id"]: index for index, record in enumerate(recycle_bin)
    }
    if validated_recycle_id not in recycle_index_map:
        raise TimetableNotFoundError(
            f"No recycle bin entry found with recycle_id '{validated_recycle_id}'."
        )

    record_index = recycle_index_map[validated_recycle_id]
    recycle_record = recycle_bin[record_index]
    entry_to_restore = copy.deepcopy(recycle_record["entry"])
    id_to_index, slot_to_indexes, _ = _build_timetable_indexes(timetable)

    if entry_to_restore["id"] in id_to_index:
        raise TimetableConflictError(
            f"Cannot restore entry because id '{entry_to_restore['id']}' already exists in the timetable."
        )

    _ensure_slot_available_from_index(
        slot_to_indexes,
        entry_to_restore["day"],
        entry_to_restore["start_time"],
        entry_to_restore["end_time"],
    )

    timetable.append(entry_to_restore)
    recycle_bin.pop(record_index)
    save_timetable(timetable)
    save_recycle_bin(recycle_bin)
    return copy.deepcopy(entry_to_restore)


# This removes one recycle-bin record forever and returns that removed record
# so the caller can confirm permanent deletion happened.
def permanently_delete_recycle_entry(recycle_id):
    validated_recycle_id = _validate_recycle_id(recycle_id)
    recycle_bin = load_recycle_bin()
    recycle_index_map = {
        record["recycle_id"]: index for index, record in enumerate(recycle_bin)
    }

    if validated_recycle_id not in recycle_index_map:
        raise TimetableNotFoundError(
            f"No recycle bin entry found with recycle_id '{validated_recycle_id}'."
        )

    deleted_record = copy.deepcopy(recycle_bin.pop(recycle_index_map[validated_recycle_id]))
    save_recycle_bin(recycle_bin)
    return deleted_record
