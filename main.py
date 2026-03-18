from datetime import datetime

from timetable_manager import (
    DuplicateTimeSlotError,
    VALID_DAYS,
    add_entry,
    delete_entry,
    edit_entry,
    get_entry_by_id,
    get_recycle_record_by_id,
    initialize_storage,
    load_timetable,
    load_recycle_bin,
    permanently_delete_recycle_entry,
    restore_entry,
    validate_time_range,
)

DAY_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]
CANCEL_KEYWORDS = {"back", "cancel", "menu"}


class UserCancelledOperation(Exception):
    """Raised when the user cancels the current menu action."""


# This watches raw user input and raises a clean cancel signal
# so any feature can stop safely and return to the main menu.
def _check_cancel(value):
    if value.strip().lower() in CANCEL_KEYWORDS:
        raise UserCancelledOperation("Action cancelled. Returning to the main menu.")
    return value


# This asks for required text and returns a non-empty value
# so important ids, names, and urls are not left blank.
def prompt_required(label):
    while True:
        value = _check_cancel(input(f"{label}: ").strip())
        if value:
            return value
        print(f"{label} is required. Type 'back' to return to the main menu.")


# This asks for a weekday, normalizes it, and returns the cleaned day
# so the menu sends valid day names into the manager layer.
def prompt_day(label, allow_blank=False):
    valid_days_text = ", ".join(sorted(VALID_DAYS))

    while True:
        value = _check_cancel(input(f"{label}: ").strip())

        if not value and allow_blank:
            return ""

        normalized_value = value.title()
        if normalized_value in VALID_DAYS:
            return normalized_value

        print(f"Invalid day. Use a full day name: {valid_days_text}. Type 'back' to cancel.")


# This asks for one time value and returns a validated HH:MM string
# so add and edit flows get clean time input from the user.
def prompt_time(label, allow_blank=False):
    while True:
        value = _check_cancel(input(f"{label}: ").strip())

        if not value and allow_blank:
            return ""

        try:
            datetime.strptime(value, "%H:%M")
            return value
        except ValueError:
            print("Invalid time. Use HH:MM format, for example 09:30. Type 'back' to cancel.")


# This asks for optional text and returns it unchanged when present
# so edit can keep old values when the user leaves a field blank.
def prompt_optional(label):
    return _check_cancel(
        input(f"{label} (leave blank to keep current value): ").strip()
    )


# This asks for a yes/no answer and returns True or False
# so risky actions like delete can require explicit confirmation.
def prompt_yes_no(label):
    while True:
        value = _check_cancel(input(f"{label} (y/n): ").strip()).lower()
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        print("Please enter y or n. Type 'back' to cancel.")


# This loads active timetable data and prints it as a table
# so the user can clearly review current class entries.
def show_timetable():
    timetable = load_timetable()

    if not timetable:
        print("No timetable entries found.")
        return

    rows = [
        [
            entry["id"],
            entry["day"],
            entry["start_time"],
            entry["end_time"],
            entry["class_name"],
            entry["classroom_url"],
        ]
        for entry in _sorted_timetable_entries(timetable)
    ]
    print("\nCurrent timetable:")
    _print_table(
        ["ID", "Day", "Start", "End", "Class Name", "Classroom URL"],
        rows,
    )


# This sorts timetable entries by weekday and start time and returns that list
# so every timetable display stays consistent and easy to read.
def _sorted_timetable_entries(timetable):
    day_order_map = {day: index for index, day in enumerate(DAY_ORDER)}
    return sorted(
        timetable,
        key=lambda entry: (
            day_order_map.get(entry["day"], len(DAY_ORDER)),
            entry["start_time"],
            entry["id"],
        ),
    )


# This prints any list of rows as an aligned text table
# so timetable and recycle-bin views share one display format.
def _print_table(headers, rows):
    column_widths = [
        max(len(str(item)) for item in [header] + [row[index] for row in rows])
        for index, header in enumerate(headers)
    ]

    def format_row(row):
        return " | ".join(
            str(value).ljust(column_widths[index]) for index, value in enumerate(row)
        )

    separator = "-+-".join("-" * width for width in column_widths)

    print(format_row(headers))
    print(separator)
    for row in rows:
        print(format_row(row))


# This loads recycle-bin data, prints it, and returns whether data existed
# so callers can avoid asking for recycle ids when the bin is empty.
def show_recycle_bin():
    recycle_bin = load_recycle_bin()

    if not recycle_bin:
        print("Recycle bin is empty.")
        return False

    rows = [
        [
            record["recycle_id"],
            record["entry"]["id"],
            record["entry"]["day"],
            record["entry"]["start_time"],
            record["entry"]["end_time"],
            record["entry"]["class_name"],
            record["deleted_at"],
        ]
        for record in recycle_bin
    ]

    print("\nRecycle bin:")
    _print_table(
        ["Recycle ID", "Entry ID", "Day", "Start", "End", "Class Name", "Deleted At"],
        rows,
    )
    return True


# This guides the user through duplicate-slot cleanup on startup
# and returns whether the app can continue normal work afterward.
def resolve_duplicate_slots(error):
    print("\nDuplicate timetable slots were found in timetable.json.")

    for group_number, duplicate_group in enumerate(error.duplicates, start=1):
        first_entry = duplicate_group[0]
        print(
            f"\nDuplicate group {group_number}: "
            f"{first_entry['day']} {first_entry['start_time']}-{first_entry['end_time']}"
        )
        for entry in duplicate_group:
            print(
                f"  ID: {entry['id']} | Class: {entry['class_name']} | "
                f"URL: {entry['classroom_url']}"
            )

    if not prompt_yes_no("Do you want to delete duplicate entries now?"):
        print("Please clean the duplicate timetable entries before continuing.")
        return False

    for duplicate_group in error.duplicates:
        while True:
            keep_id = prompt_required(
                f"Enter the ID you want to keep for "
                f"{duplicate_group[0]['day']} "
                f"{duplicate_group[0]['start_time']}-{duplicate_group[0]['end_time']}"
            )
            valid_ids = {entry["id"] for entry in duplicate_group}
            if keep_id not in valid_ids:
                print("Invalid ID. Please choose one of the IDs shown above.")
                continue

            for entry in duplicate_group:
                if entry["id"] != keep_id:
                    delete_entry(entry["id"], allow_duplicate_slots=True)
                    print(f"Deleted duplicate entry: {entry['id']}")
            break

    print("Duplicate timetable entries were cleaned successfully.")
    return True


# This collects add-entry input, validates it step by step,
# and creates a new timetable entry through the manager layer.
def handle_add():
    print("\nAdd Entry. Type 'back' at any prompt to return to the main menu.")
    day = prompt_day("Day")
    while True:
        start_time = prompt_time("Start time (HH:MM)")
        end_time = prompt_time("End time (HH:MM)")
        try:
            start_time, end_time = validate_time_range(start_time, end_time)
            break
        except ValueError as exc:
            print(f"Error: {exc}")

    class_name = prompt_required("Class name")
    url = prompt_required("Classroom URL")

    entry = add_entry(day, start_time, end_time, class_name, url)
    print(f"Entry added successfully: {entry['id']}")


# This collects edit input, keeps unchanged values when fields are blank,
# and returns control after updating the selected timetable entry.
def handle_edit():
    print("\nEdit Entry. Type 'back' at any prompt to return to the main menu.")
    entry_id = prompt_required("Entry ID to edit")
    current_entry = get_entry_by_id(entry_id)
    print("Enter only the fields you want to update.")

    updates = {}
    day = prompt_day("New day (leave blank to keep current value)", allow_blank=True)

    while True:
        start_time = prompt_time(
            "New start time (HH:MM) (leave blank to keep current value)",
            allow_blank=True,
        )
        end_time = prompt_time(
            "New end time (HH:MM) (leave blank to keep current value)",
            allow_blank=True,
        )

        proposed_start_time = start_time or current_entry["start_time"]
        proposed_end_time = end_time or current_entry["end_time"]

        try:
            validate_time_range(proposed_start_time, proposed_end_time)
            break
        except ValueError as exc:
            print(f"Error: {exc}")

    class_name = prompt_optional("New class name")
    classroom_url = prompt_optional("New classroom URL")

    if day:
        updates["day"] = day
    if start_time:
        updates["start_time"] = start_time
    if end_time:
        updates["end_time"] = end_time
    if class_name:
        updates["class_name"] = class_name
    if classroom_url:
        updates["classroom_url"] = classroom_url

    if not updates:
        print("No changes entered.")
        return

    updated_entry = edit_entry(entry_id, **updates)
    print(f"Entry updated successfully: {updated_entry['id']}")


# This shows the chosen class, asks for confirmation,
# and moves the entry into the recycle bin if approved.
def handle_delete():
    print("\nDelete Entry. Type 'back' at any prompt to return to the main menu.")
    entry_id = prompt_required("Entry ID to delete")
    entry = get_entry_by_id(entry_id)

    print("\nEntry selected for deletion:")
    _print_table(
        ["ID", "Day", "Start", "End", "Class Name", "Classroom URL"],
        [[
            entry["id"],
            entry["day"],
            entry["start_time"],
            entry["end_time"],
            entry["class_name"],
            entry["classroom_url"],
        ]],
    )

    if not prompt_yes_no("Do you want to move this entry to the recycle bin"):
        print("Deletion cancelled. Entry was not removed.")
        return

    deleted_record = delete_entry(entry_id)
    print(
        f"Entry moved to recycle bin successfully: "
        f"{deleted_record['entry']['id']} -> {deleted_record['recycle_id']}"
    )


# This runs the recycle-bin submenu for viewing, restoring,
# and permanently deleting archived entries.
def handle_recycle_bin():
    print("\nRecycle Bin. Type 'back' at any prompt to return to the main menu.")

    while True:
        print("\nRecycle Bin Menu")
        print("1. Show recycle bin")
        print("2. Restore entry")
        print("3. Delete permanently")
        print("4. Return to main menu")

        choice = _check_cancel(input("Choose an option: ").strip())

        if choice == "1":
            show_recycle_bin()
        elif choice == "2":
            if not show_recycle_bin():
                continue
            recycle_id = prompt_required("Recycle ID to restore")
            record = get_recycle_record_by_id(recycle_id)
            restored_entry = restore_entry(recycle_id)
            print(
                f"Restored entry successfully: "
                f"{record['entry']['id']} from {record['recycle_id']}"
            )
            print(f"Restored timetable entry id: {restored_entry['id']}")
        elif choice == "3":
            if not show_recycle_bin():
                continue
            recycle_id = prompt_required("Recycle ID to delete permanently")
            record = get_recycle_record_by_id(recycle_id)
            if not prompt_yes_no(
                f"Are you sure you want to permanently delete {record['entry']['id']} from the recycle bin"
            ):
                print("Permanent deletion cancelled.")
                continue
            deleted_record = permanently_delete_recycle_entry(recycle_id)
            print(
                f"Permanently deleted recycle bin entry: "
                f"{deleted_record['recycle_id']}"
            )
        elif choice == "4":
            print("Returning to the main menu.")
            return
        else:
            print("Invalid choice. Please enter a number from 1 to 4.")


# This is the main program loop that validates startup data,
# shows the menu, and routes the user into each feature.
def main():
    try:
        initialize_storage()
    except DuplicateTimeSlotError as exc:
        if not resolve_duplicate_slots(exc):
            return
    except Exception as exc:
        print(f"Error: {exc}")
        return

    while True:
        print("\nTimetable Manager")
        print("1. Show timetable")
        print("2. Add entry")
        print("3. Edit entry")
        print("4. Delete entry")
        print("5. Recycle bin")
        print("6. Exit")

        choice = input("Choose an option: ").strip()

        try:
            if choice == "1":
                show_timetable()
            elif choice == "2":
                handle_add()
            elif choice == "3":
                handle_edit()
            elif choice == "4":
                handle_delete()
            elif choice == "5":
                handle_recycle_bin()
            elif choice == "6":
                print("Goodbye.")
                break
            else:
                print("Invalid choice. Please enter a number from 1 to 6.")
        except UserCancelledOperation as exc:
            print(exc)
        except DuplicateTimeSlotError as exc:
            resolve_duplicate_slots(exc)
        except Exception as exc:
            print(f"Error: {exc}")


if __name__ == "__main__":
    main()
