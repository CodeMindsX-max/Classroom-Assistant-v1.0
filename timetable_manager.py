import json
import re
from datetime import datetime


TIMETABLE_FILE = "timetable.json"
RECYCLE_BIN_FILE = "recycle_bin.json"
VALID_FIELDS = {"day", "start_time", "end_time", "class_name", "classroom_url"}
RECYCLE_ID_PATTERN = re.compile(r"^bin_\d+$")
VALID_DAYS = {
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
}


class TimetableError(Exception):
    """Base exception for timetable-related errors."""


class DuplicateTimeSlotError(TimetableError):
    """Raised when duplicate timetable slots already exist in stored data."""

    def __init__(self, duplicates):
        self.duplicates = duplicates
        super().__init__("Duplicate timetable slots found in stored data.")


# This small gatekeeper checks simple text input and returns a cleaned string
# so later functions can trust the value they receive.
def _validate_text(value, field_name):
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string.")

    cleaned_value = value.strip()
    if not cleaned_value:
        raise ValueError(f"{field_name} cannot be empty.")

    return cleaned_value


# This keeps recycle bin ids in one safe pattern and returns the cleaned id
# so restore and permanent delete work only with valid recycle references.
def _validate_recycle_id(value):
    cleaned_value = _validate_text(value, "recycle_id")
    if not RECYCLE_ID_PATTERN.fullmatch(cleaned_value):
        raise ValueError("recycle_id must be in the format 'bin_<number>', for example 'bin_1'.")
    return cleaned_value


# This validates one time value and returns the cleaned HH:MM string
# so all timetable operations work with one consistent time format.
def _validate_time(value, field_name):
    cleaned_value = _validate_text(value, field_name)

    try:
        datetime.strptime(cleaned_value, "%H:%M")
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in HH:MM format.") from exc

    return cleaned_value


# This validates the entered day name and returns a normalized day
# so stored timetable data always uses proper weekday names.
def _validate_day(value):
    cleaned_value = _validate_text(value, "day").title()
    if cleaned_value not in VALID_DAYS:
        valid_days_text = ", ".join(sorted(VALID_DAYS))
        raise ValueError(f"day must be a valid day name: {valid_days_text}.")
    return cleaned_value


# This checks the time gap rule and returns the validated start and end times
# so add and edit can safely build entries with allowed durations only.
def validate_time_range(start_time, end_time):
    """Validate that a timetable slot is at least 1 hour and at most 3 hours."""
    validated_start = _validate_time(start_time, "start_time")
    validated_end = _validate_time(end_time, "end_time")

    start_obj = datetime.strptime(validated_start, "%H:%M")
    end_obj = datetime.strptime(validated_end, "%H:%M")
    gap_in_hours = (end_obj - start_obj).total_seconds() / 3600

    if gap_in_hours < 1:
        raise ValueError("The gap between start_time and end_time must be at least 1 hour.")
    if gap_in_hours > 3:
        raise ValueError("The gap between start_time and end_time must not be more than 3 hours.")

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
            raise ValueError("Use either 'url' or 'classroom_url', not both.")
        normalized["classroom_url"] = normalized.pop("url")

    invalid_fields = set(normalized) - VALID_FIELDS
    if invalid_fields:
        invalid_list = ", ".join(sorted(invalid_fields))
        raise ValueError(f"Invalid field(s): {invalid_list}")

    return normalized


# This verifies that every timetable entry id is unique and raises early
# so id-based lookup, edit, delete, and restore stay reliable.
def _ensure_unique_ids(timetable):
    seen_ids = set()

    for entry in timetable:
        entry_id = entry.get("id")
        if not isinstance(entry_id, str) or not entry_id.strip():
            raise ValueError("Each timetable entry must have a non-empty string id.")
        if entry_id in seen_ids:
            raise ValueError(f"Duplicate id found in timetable: {entry_id}")
        seen_ids.add(entry_id)


# This finds where a timetable entry lives and returns its list index
# so callers can update or remove the exact record they asked for.
def _find_entry_index(timetable, entry_id):
    for index, entry in enumerate(timetable):
        if entry.get("id") == entry_id:
            return index

    raise ValueError(f"No entry found with id '{entry_id}'.")


# This finds a recycle bin record by recycle id and returns its position
# so recycle operations act on the correct deleted item.
def _find_recycle_record_index(recycle_bin, recycle_id):
    for index, record in enumerate(recycle_bin):
        if record.get("recycle_id") == recycle_id:
            return index

    raise ValueError(f"No recycle bin entry found with recycle_id '{recycle_id}'.")


# This scans the timetable and returns groups of duplicate time slots
# so the app can report and clean conflicting stored data.
def _find_duplicate_time_slots(timetable):
    slots = {}

    for entry in timetable:
        slot_key = (
            entry.get("day"),
            entry.get("start_time"),
            entry.get("end_time"),
        )
        slots.setdefault(slot_key, []).append(entry)

    return [entries for entries in slots.values() if len(entries) > 1]


# This stops loading when duplicate timetable slots already exist
# so the user can fix conflicts before normal operations continue.
def _ensure_no_duplicate_slots(timetable):
    duplicates = _find_duplicate_time_slots(timetable)
    if duplicates:
        raise DuplicateTimeSlotError(duplicates)


# This checks whether a day/time slot is free and raises if it is occupied
# so add, edit, and restore cannot create duplicate classes in one slot.
def _ensure_slot_available(timetable, day, start_time, end_time, ignore_entry_id=None):
    for entry in timetable:
        if entry.get("id") == ignore_entry_id:
            continue

        if (
            entry.get("day") == day
            and entry.get("start_time") == start_time
            and entry.get("end_time") == end_time
        ):
            raise ValueError(
                f"A timetable entry already exists for {day} from {start_time} to {end_time}."
            )


# This thin wrapper reads the timetable source file and returns raw list data
# so load_timetable has one clear place to fetch stored timetable records.
def _read_timetable_file():
    return _read_json_list_file(TIMETABLE_FILE)


# This reads a JSON list file and returns its list content
# so both timetable and recycle bin can share one safe file reader.
def _read_json_list_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"{file_path} was not found.") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{file_path} contains invalid JSON.") from exc
    except OSError as exc:
        raise OSError(f"Could not read {file_path}: {exc}") from exc

    if not isinstance(data, list):
        raise ValueError(f"{file_path} must contain a list of entries.")

    return data


# This validates every saved timetable record and returns nothing on success
# so save and load only work with strong, trusted timetable data.
def _validate_timetable_entries(data):
    _ensure_unique_ids(data)
    for entry in data:
        _validate_text(entry.get("id"), "id")
        _validate_entry_fields(
            entry.get("day"),
            entry.get("start_time"),
            entry.get("end_time"),
            entry.get("class_name"),
            entry.get("classroom_url"),
        )


# This validates recycle bin records and their nested entries
# so deleted data can be restored later without hidden corruption.
def _validate_recycle_bin(recycle_bin):
    seen_recycle_ids = set()

    for record in recycle_bin:
        if not isinstance(record, dict):
            raise ValueError("Each recycle bin record must be an object.")

        recycle_id = _validate_recycle_id(record.get("recycle_id"))
        if recycle_id in seen_recycle_ids:
            raise ValueError(f"Duplicate recycle_id found in recycle bin: {recycle_id}")
        seen_recycle_ids.add(recycle_id)

        _validate_text(record.get("deleted_at"), "deleted_at")

        entry = record.get("entry")
        if not isinstance(entry, dict):
            raise ValueError("Each recycle bin record must contain an 'entry' object.")

        _validate_text(entry.get("id"), "id")
        _validate_entry_fields(
            entry.get("day"),
            entry.get("start_time"),
            entry.get("end_time"),
            entry.get("class_name"),
            entry.get("classroom_url"),
        )


# This loads timetable data, validates it, and returns the clean timetable list
# so the rest of the program always works from verified classroom data.
def load_timetable(allow_duplicate_slots=False):
    """Read and return timetable data from the JSON file."""
    data = _read_timetable_file()
    _validate_timetable_entries(data)
    if not allow_duplicate_slots:
        _ensure_no_duplicate_slots(data)
    return data


# This loads deleted records from the recycle bin and returns that list
# so restore and permanent delete can work from validated deleted entries.
def load_recycle_bin():
    """Read and return recycle bin records from the JSON file."""
    try:
        recycle_bin = _read_json_list_file(RECYCLE_BIN_FILE)
    except FileNotFoundError:
        save_recycle_bin([])
        recycle_bin = []

    _validate_recycle_bin(recycle_bin)
    return recycle_bin


# This validates and writes timetable data back to the main JSON file
# so successful add, edit, restore, and cleanup changes become permanent.
def save_timetable(data):
    """Write timetable data back to the JSON file."""
    if not isinstance(data, list):
        raise ValueError("Timetable data must be a list.")

    _validate_timetable_entries(data)

    try:
        with open(TIMETABLE_FILE, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
    except TypeError as exc:
        raise ValueError(f"Timetable data contains non-JSON-serializable values: {exc}") from exc
    except OSError as exc:
        raise OSError(f"Could not save {TIMETABLE_FILE}: {exc}") from exc


# This validates and writes recycle bin records to their JSON file
# so deleted items are stored safely for later recovery.
def save_recycle_bin(data):
    """Write recycle bin data back to the recycle JSON file."""
    if not isinstance(data, list):
        raise ValueError("Recycle bin data must be a list.")

    _validate_recycle_bin(data)

    try:
        with open(RECYCLE_BIN_FILE, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
    except TypeError as exc:
        raise ValueError(f"Recycle bin data contains non-JSON-serializable values: {exc}") from exc
    except OSError as exc:
        raise OSError(f"Could not save {RECYCLE_BIN_FILE}: {exc}") from exc


# This creates one new timetable entry, saves it, and returns the new record
# so the caller can show the created id and details to the user.
def add_entry(day, start_time, end_time, class_name, url):
    """Add a new class entry, generate a new id, and save it."""
    validated_day, validated_start, validated_end, validated_name, validated_url = (
        _validate_entry_fields(day, start_time, end_time, class_name, url)
    )

    timetable = load_timetable()
    _ensure_slot_available(timetable, validated_day, validated_start, validated_end)
    day_prefix = validated_day[:3].lower()
    matching_ids = []

    for entry in timetable:
        entry_id = entry.get("id", "")
        if isinstance(entry_id, str) and entry_id.startswith(f"{day_prefix}_"):
            try:
                matching_ids.append(int(entry_id.split("_")[1]))
            except (IndexError, ValueError):
                continue

    next_number = max(matching_ids, default=0) + 1
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
    return new_entry


# This fetches one timetable entry by id and returns that entry dictionary
# so edit and delete can verify the exact class the user selected.
def get_entry_by_id(entry_id):
    """Return a timetable entry by id."""
    validated_id = _validate_text(entry_id, "entry_id")
    timetable = load_timetable()
    entry_index = _find_entry_index(timetable, validated_id)
    return timetable[entry_index]


# This fetches one recycle bin record by recycle id and returns that record
# so restore and permanent delete can confirm the target item first.
def get_recycle_record_by_id(recycle_id):
    """Return a recycle bin record by recycle_id."""
    validated_recycle_id = _validate_recycle_id(recycle_id)
    recycle_bin = load_recycle_bin()
    record_index = _find_recycle_record_index(recycle_bin, validated_recycle_id)
    return recycle_bin[record_index]


# This updates one existing timetable entry, saves the timetable,
# and returns the updated entry so the caller can show the new result.
def edit_entry(entry_id, **kwargs):
    """Update an existing entry by id and save the changes.

    Pass only the fields you want to change, for example:
    edit_entry("tue_2", class_name="Physics", start_time="10:00")
    """
    validated_id = _validate_text(entry_id, "entry_id")
    updates = _normalize_update_fields(kwargs)

    if not updates:
        raise ValueError("Provide at least one field to update.")

    timetable = load_timetable()

    for entry in timetable:
        if entry.get("id") == validated_id:
            updated_entry = {
                "day": updates.get("day", entry.get("day")),
                "start_time": updates.get("start_time", entry.get("start_time")),
                "end_time": updates.get("end_time", entry.get("end_time")),
                "class_name": updates.get("class_name", entry.get("class_name")),
                "classroom_url": updates.get("classroom_url", entry.get("classroom_url")),
            }

            (
                updated_day,
                updated_start,
                updated_end,
                updated_name,
                updated_url,
            ) = _validate_entry_fields(
                updated_entry["day"],
                updated_entry["start_time"],
                updated_entry["end_time"],
                updated_entry["class_name"],
                updated_entry["classroom_url"],
            )
            _ensure_slot_available(
                timetable,
                updated_day,
                updated_start,
                updated_end,
                ignore_entry_id=validated_id,
            )

            entry["day"] = updated_day
            entry["start_time"] = updated_start
            entry["end_time"] = updated_end
            entry["class_name"] = updated_name
            entry["classroom_url"] = updated_url

            save_timetable(timetable)
            return entry

    raise ValueError(f"No entry found with id '{validated_id}'.")


# This soft-deletes a timetable entry, moves it into the recycle bin,
# and returns the recycle record so the caller can show where it went.
def delete_entry(entry_id, allow_duplicate_slots=False):
    """Soft-delete an entry by moving it into the recycle bin."""
    validated_id = _validate_text(entry_id, "entry_id")
    timetable = load_timetable(allow_duplicate_slots=allow_duplicate_slots)
    recycle_bin = load_recycle_bin()
    entry_index = _find_entry_index(timetable, validated_id)
    deleted_entry = timetable[entry_index].copy()
    next_recycle_number = len(recycle_bin) + 1

    while any(record.get("recycle_id") == f"bin_{next_recycle_number}" for record in recycle_bin):
        next_recycle_number += 1

    recycle_record = {
        "recycle_id": f"bin_{next_recycle_number}",
        "deleted_at": datetime.now().isoformat(timespec="seconds"),
        "entry": deleted_entry,
    }

    timetable.pop(entry_index)
    recycle_bin.append(recycle_record)
    save_timetable(timetable)
    save_recycle_bin(recycle_bin)
    return recycle_record


# This restores one deleted entry back into the timetable and returns it
# so the caller can confirm the class is active again.
def restore_entry(recycle_id, allow_duplicate_slots=False):
    """Restore a recycle bin entry back into the timetable."""
    validated_recycle_id = _validate_recycle_id(recycle_id)
    timetable = load_timetable(allow_duplicate_slots=allow_duplicate_slots)
    recycle_bin = load_recycle_bin()
    record_index = _find_recycle_record_index(recycle_bin, validated_recycle_id)
    recycle_record = recycle_bin[record_index]
    entry_to_restore = recycle_record["entry"].copy()

    if any(entry.get("id") == entry_to_restore["id"] for entry in timetable):
        raise ValueError(
            f"Cannot restore entry because id '{entry_to_restore['id']}' already exists in the timetable."
        )

    _ensure_slot_available(
        timetable,
        entry_to_restore["day"],
        entry_to_restore["start_time"],
        entry_to_restore["end_time"],
    )

    timetable.append(entry_to_restore)
    recycle_bin.pop(record_index)
    save_timetable(timetable)
    save_recycle_bin(recycle_bin)
    return entry_to_restore


# This removes one recycle bin record forever and returns that removed record
# so the caller can confirm permanent deletion happened.
def permanently_delete_recycle_entry(recycle_id):
    """Remove an entry from the recycle bin permanently."""
    validated_recycle_id = _validate_recycle_id(recycle_id)
    recycle_bin = load_recycle_bin()
    record_index = _find_recycle_record_index(recycle_bin, validated_recycle_id)
    deleted_record = recycle_bin.pop(record_index)
    save_recycle_bin(recycle_bin)
    return deleted_record
