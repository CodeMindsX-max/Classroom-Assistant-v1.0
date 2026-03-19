from app_logger_manager import get_app_logger, log_error, log_event, log_info, log_warning, shutdown_logging
from timetable_manager import (
    DuplicateTimeSlotError,
    InvalidTimetableEntriesError,
    add_entry,
    delete_entry,
    delete_raw_timetable_entry,
    edit_entry,
    get_entry_by_id,
    get_recycle_record_by_id,
    initialize_storage,
    load_timetable,
    load_recycle_bin,
    clear_recycle_bin,
    permanently_delete_recycle_entry,
    repair_timetable_entry,
    restore_entry,
    shutdown_storage,
    validate_day_input,
    validate_optional_text,
    validate_required_text,
    validate_time_input,
    validate_time_range,
    validate_yes_no_input,
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
DAY_ORDER_MAP = {day: index for index, day in enumerate(DAY_ORDER)}
CANCEL_KEYWORDS = {"back", "cancel", "menu"}
LOGGER = get_app_logger("main")


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
        try:
            return validate_required_text(value, label)
        except ValueError as exc:
            print(f"{exc} Type 'back' to return to the main menu.")


# This asks for a weekday, normalizes it, and returns the cleaned day
# so the menu sends valid day names into the manager layer.
def prompt_day(label, allow_blank=False):
    while True:
        value = _check_cancel(input(f"{label}: ").strip())

        if not value and allow_blank:
            return ""

        try:
            return validate_day_input(value)
        except ValueError as exc:
            print(f"{exc} Type 'back' to cancel.")


# This asks for one time value and returns a validated HH:MM string
# so add and edit flows get clean time input from the user.
def prompt_time(label, allow_blank=False):
    while True:
        value = _check_cancel(input(f"{label}: ").strip())

        if not value and allow_blank:
            return ""

        try:
            return validate_time_input(value, label)
        except ValueError as exc:
            print(f"{exc} Type 'back' to cancel.")


# This asks for optional text and returns it unchanged when present
# so edit can keep old values when the user leaves a field blank.
def prompt_optional(label):
    value = _check_cancel(
        input(f"{label} (leave blank to keep current value): ").strip()
    )
    return validate_optional_text(value)


# This asks for a yes/no answer and returns True or False
# so risky actions like delete can require explicit confirmation.
def prompt_yes_no(label):
    while True:
        value = _check_cancel(input(f"{label} (y/n): ").strip())
        try:
            return validate_yes_no_input(value)
        except ValueError as exc:
            print(f"{exc} Type 'back' to cancel.")


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
    return sorted(
        timetable,
        key=lambda entry: (
            DAY_ORDER_MAP.get(entry["day"], len(DAY_ORDER)),
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
    while True:
        log_warning(
            LOGGER,
            "Startup duplicate-slot repair is required.",
            what="Detected duplicate timetable slots during startup validation.",
            where="main.resolve_duplicate_slots",
            why="Stored timetable data contains conflicting slots that block safe startup.",
            context={"duplicate_group_count": len(error.duplicates)},
        )
        print("\nDuplicate timetable slots were found in SQLite storage.")

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
            log_warning(
                LOGGER,
                "User deferred duplicate-slot cleanup.",
                what="Startup duplicate-slot repair was declined for this loop iteration.",
                where="main.resolve_duplicate_slots",
                why="The user chose not to repair blocking duplicate timetable data yet.",
            )
            print("This data must be repaired before the program can continue.")
            continue

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

        log_event(
            LOGGER,
            "Startup duplicate-slot cleanup completed.",
            what="Removed blocking duplicate timetable entries during startup repair.",
            where="main.resolve_duplicate_slots",
            why="The user resolved duplicate timetable data so normal startup could continue.",
        )
        print("Duplicate timetable entries were cleaned successfully.")
        return True


# This guides the user through malformed stored timetable rows on startup
# and returns whether the app can continue after those bad entries are fixed or removed.
def resolve_invalid_timetable_entries(error):
    try:
        while True:
            log_warning(
                LOGGER,
                "Startup invalid-entry repair is required.",
                what="Detected invalid stored timetable entries during startup validation.",
                where="main.resolve_invalid_timetable_entries",
                why="Stored timetable rows failed manager validation and must be repaired or removed.",
                context={"issue_count": len(error.issues)},
            )
            print("\nInvalid timetable entries were found in SQLite storage.")

            current_issues = error.issues
            if not current_issues:
                print("All invalid timetable entries were repaired successfully.")
                return True

            for issue_number, listed_issue in enumerate(current_issues, start=1):
                print(f"\nInvalid entry {issue_number} at position {listed_issue['index']}:")
                print(f"  Error: {listed_issue['error']}")
                print(f"  Stored data: {listed_issue['entry']}")

            if not prompt_yes_no("Do you want to repair or delete these invalid entries now?"):
                log_warning(
                    LOGGER,
                    "User deferred invalid-entry cleanup.",
                    what="Startup invalid-entry repair was declined for this loop iteration.",
                    where="main.resolve_invalid_timetable_entries",
                    why="The user chose not to repair blocking invalid timetable data yet.",
                )
                print("This data must be repaired before the program can continue.")
                continue

            issue = current_issues[0]
            issue_entry = issue["entry"]
            invalid_fields = set(issue.get("fields", []))
            print(f"\nWorking on invalid entry at position {issue['index']}:")
            print(f"  Error: {issue['error']}")
            print(f"  Stored data: {issue['entry']}")

            action = prompt_required("Type 'edit' to repair or 'delete' to remove this entry").lower()
            if action == "delete":
                deleted_entry = delete_raw_timetable_entry(issue["index"])
                log_event(
                    LOGGER,
                    "Deleted one invalid startup entry.",
                    what="Removed a malformed timetable row during startup repair.",
                    where="main.resolve_invalid_timetable_entries",
                    why="The user chose deletion instead of editing for a blocking invalid row.",
                    context={"index": issue["index"], "entry_id": deleted_entry.get("id")},
                )
                print(f"Deleted invalid entry: {deleted_entry}")
            elif action == "edit":
                while True:
                    day = issue_entry.get("day", "")
                    start_time = issue_entry.get("start_time", "")
                    end_time = issue_entry.get("end_time", "")
                    class_name = issue_entry.get("class_name", "")
                    classroom_url = issue_entry.get("classroom_url", "")

                    if "day" in invalid_fields:
                        day = prompt_day("Correct day")

                    if "start_time" in invalid_fields or "end_time" in invalid_fields:
                        start_time = prompt_time("Correct start time (HH:MM)")
                        end_time = prompt_time("Correct end time (HH:MM)")

                    if "class_name" in invalid_fields:
                        class_name = prompt_required("Correct class name")

                    if "classroom_url" in invalid_fields:
                        classroom_url = prompt_required("Correct classroom URL")

                    try:
                        repaired_entry = repair_timetable_entry(
                            issue["index"],
                            issue_entry.get("id", ""),
                            day,
                            start_time,
                            end_time,
                            class_name,
                            classroom_url,
                        )
                        log_event(
                            LOGGER,
                            "Repaired one invalid startup entry.",
                            what="Updated a malformed timetable row during startup repair.",
                            where="main.resolve_invalid_timetable_entries",
                            why="The user supplied corrected values for the invalid stored row.",
                            context={"index": issue["index"], "entry_id": repaired_entry["id"], "fields": sorted(invalid_fields)},
                        )
                        print(f"Repaired invalid entry: {repaired_entry['id']}")
                        break
                    except Exception as exc:
                        log_warning(
                            LOGGER,
                            "Startup entry repair attempt failed.",
                            what="A startup repair submission was rejected.",
                            where="main.resolve_invalid_timetable_entries",
                            why=str(exc),
                            context={"index": issue["index"], "fields": sorted(invalid_fields)},
                        )
                        print(f"Error: {exc}")
            else:
                print("Please enter either 'edit' or 'delete'.")
                continue

            try:
                initialize_storage()
                log_event(
                    LOGGER,
                    "Startup invalid-entry cleanup completed.",
                    what="Resolved blocking invalid timetable data during startup repair.",
                    where="main.resolve_invalid_timetable_entries",
                    why="The stored timetable became valid again and startup can continue.",
                )
                print("Invalid timetable entries were repaired successfully.")
                return True
            except InvalidTimetableEntriesError as exc:
                error = exc
                continue
            except DuplicateTimeSlotError as exc:
                return resolve_duplicate_slots(exc)
            except Exception as exc:
                log_error(
                    LOGGER,
                    "Startup repair failed during reinitialization.",
                    what="Storage reinitialization failed after a startup repair attempt.",
                    where="main.resolve_invalid_timetable_entries",
                    why=str(exc),
                    exc_info=True,
                )
                print(f"Error: {exc}")
                return False
    except UserCancelledOperation as exc:
        log_info(
            LOGGER,
            "User cancelled startup repair flow.",
            what="Startup repair returned to the caller because the user cancelled the interaction.",
            where="main.resolve_invalid_timetable_entries",
            why=str(exc),
        )
        print(exc)
        return False


# This collects add-entry input, validates it step by step,
# and creates a new timetable entry through the manager layer.
def handle_add():
    log_info(LOGGER, "Entered add-entry flow.", what="Opened the add-entry CLI flow.", where="main.handle_add", why="The user selected the add-entry feature.")
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
    log_info(LOGGER, "Entered edit-entry flow.", what="Opened the edit-entry CLI flow.", where="main.handle_edit", why="The user selected the edit-entry feature.")
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
        log_info(LOGGER, "Edit-entry flow ended with no changes.", what="The edit-entry CLI flow received no field updates.", where="main.handle_edit", why="The user left every editable field blank.")
        print("No changes entered.")
        return

    updated_entry = edit_entry(entry_id, **updates)
    print(f"Entry updated successfully: {updated_entry['id']}")


# This shows the chosen class, asks for confirmation,
# and moves the entry into the recycle bin if approved.
def handle_delete():
    log_info(LOGGER, "Entered delete-entry flow.", what="Opened the delete-entry CLI flow.", where="main.handle_delete", why="The user selected the delete-entry feature.")
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
        log_info(LOGGER, "Deletion was cancelled by the user.", what="The delete-entry CLI flow was confirmed as no-op.", where="main.handle_delete", why="The user rejected the delete confirmation prompt.", context={"entry_id": entry["id"]})
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
    log_info(LOGGER, "Entered recycle-bin flow.", what="Opened the recycle-bin CLI flow.", where="main.handle_recycle_bin", why="The user selected the recycle-bin feature.")
    print("\nRecycle Bin. Type 'back' at any prompt to return to the main menu.")

    while True:
        print("\nRecycle Bin Menu")
        print("1. Show recycle bin")
        print("2. Restore entry")
        print("3. Delete permanently")
        print("4. Clear recycle bin")
        print("5. Return to main menu")

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
                log_info(LOGGER, "Permanent recycle-bin deletion was cancelled.", what="The recycle-bin CLI flow kept a record after confirmation was declined.", where="main.handle_recycle_bin", why="The user rejected the permanent deletion confirmation prompt.", context={"recycle_id": recycle_id, "entry_id": record["entry"]["id"]})
                print("Permanent deletion cancelled.")
                continue
            deleted_record = permanently_delete_recycle_entry(recycle_id)
            print(
                f"Permanently deleted recycle bin entry: "
                f"{deleted_record['recycle_id']}"
            )
        elif choice == "4":
            if not show_recycle_bin():
                continue
            if not prompt_yes_no(
                "Are you sure you want to permanently clear the entire recycle bin"
            ):
                log_info(LOGGER, "Recycle-bin clear was cancelled.", what="The recycle-bin CLI flow kept all archive records after full-clear confirmation was declined.", where="main.handle_recycle_bin", why="The user rejected the recycle-bin clear confirmation prompt.")
                print("Recycle bin clear cancelled.")
                continue
            deleted_records = clear_recycle_bin()
            print(
                f"Permanently deleted all recycle bin entries: {len(deleted_records)} record(s)"
            )
        elif choice == "5":
            print("Returning to the main menu.")
            return
        else:
            print("Invalid choice. Please enter a number from 1 to 5.")


# This is the main program loop that validates startup data,
# shows the menu, and routes the user into each feature.
def main():
    try:
        try:
            log_info(LOGGER, "CLI startup started.", what="Application startup validation began.", where="main.main", why="The timetable manager program was launched.")
            initialize_storage()
        except InvalidTimetableEntriesError as exc:
            log_warning(LOGGER, "Startup found invalid stored timetable entries.", what="Startup validation blocked execution because invalid timetable rows were found.", where="main.main", why=str(exc), context={"issue_count": len(exc.issues)})
            if not resolve_invalid_timetable_entries(exc):
                return
        except DuplicateTimeSlotError as exc:
            log_warning(LOGGER, "Startup found duplicate timetable slots.", what="Startup validation blocked execution because duplicate timetable slots were found.", where="main.main", why=str(exc), context={"duplicate_group_count": len(exc.duplicates)})
            if not resolve_duplicate_slots(exc):
                return
        except Exception as exc:
            log_error(LOGGER, "Startup failed unexpectedly.", what="Application startup stopped because initialization raised an unexpected exception.", where="main.main", why=str(exc), exc_info=True)
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
                    log_info(LOGGER, "User selected show timetable.", what="CLI menu routed to the timetable display feature.", where="main.main", why="The user chose menu option 1.")
                    show_timetable()
                elif choice == "2":
                    log_info(LOGGER, "User selected add entry.", what="CLI menu routed to the add-entry feature.", where="main.main", why="The user chose menu option 2.")
                    handle_add()
                elif choice == "3":
                    log_info(LOGGER, "User selected edit entry.", what="CLI menu routed to the edit-entry feature.", where="main.main", why="The user chose menu option 3.")
                    handle_edit()
                elif choice == "4":
                    log_info(LOGGER, "User selected delete entry.", what="CLI menu routed to the delete-entry feature.", where="main.main", why="The user chose menu option 4.")
                    handle_delete()
                elif choice == "5":
                    log_info(LOGGER, "User selected recycle bin.", what="CLI menu routed to the recycle-bin feature.", where="main.main", why="The user chose menu option 5.")
                    handle_recycle_bin()
                elif choice == "6":
                    log_info(LOGGER, "User selected exit.", what="CLI menu started graceful shutdown.", where="main.main", why="The user chose menu option 6.")
                    print("Goodbye.")
                    break
                else:
                    log_warning(LOGGER, "User entered an invalid menu choice.", what="CLI menu rejected an unsupported option.", where="main.main", why="The entered menu value did not match any supported feature.", context={"choice": choice})
                    print("Invalid choice. Please enter a number from 1 to 6.")
            except UserCancelledOperation as exc:
                log_info(LOGGER, "User cancelled an in-progress CLI action.", what="A feature flow returned to the menu because the user used a cancel keyword.", where="main.main", why=str(exc))
                print(exc)
            except InvalidTimetableEntriesError as exc:
                log_warning(LOGGER, "An action surfaced invalid stored timetable data.", what="Runtime validation found invalid timetable rows and blocked the current operation.", where="main.main", why=str(exc), context={"issue_count": len(exc.issues)})
                if not resolve_invalid_timetable_entries(exc):
                    break
            except DuplicateTimeSlotError as exc:
                log_warning(LOGGER, "An action surfaced duplicate timetable slots.", what="Runtime validation found duplicate timetable slots and blocked the current operation.", where="main.main", why=str(exc), context={"duplicate_group_count": len(exc.duplicates)})
                resolve_duplicate_slots(exc)
            except Exception as exc:
                log_error(LOGGER, "A CLI action failed unexpectedly.", what="A runtime exception escaped the current menu action.", where="main.main", why=str(exc), exc_info=True)
                print(f"Error: {exc}")
    finally:
        shutdown_storage()
        shutdown_logging()


if __name__ == "__main__":
    main()
