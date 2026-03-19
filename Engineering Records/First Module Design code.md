\#main.py

(

from datetime import datetime



from timetable\_manager import (

&#x20;   DuplicateTimeSlotError,

&#x20;   VALID\_DAYS,

&#x20;   add\_entry,

&#x20;   delete\_entry,

&#x20;   edit\_entry,

&#x20;   get\_entry\_by\_id,

&#x20;   get\_recycle\_record\_by\_id,

&#x20;   load\_timetable,

&#x20;   load\_recycle\_bin,

&#x20;   permanently\_delete\_recycle\_entry,

&#x20;   restore\_entry,

&#x20;   validate\_time\_range,

)



DAY\_ORDER = \[

&#x20;   "Monday",

&#x20;   "Tuesday",

&#x20;   "Wednesday",

&#x20;   "Thursday",

&#x20;   "Friday",

&#x20;   "Saturday",

&#x20;   "Sunday",

]

CANCEL\_KEYWORDS = {"back", "cancel", "menu"}





class UserCancelledOperation(Exception):

&#x20;   """Raised when the user cancels the current menu action."""





\# This watches raw user input and raises a clean cancel signal

\# so any feature can stop safely and return to the main menu.

def \_check\_cancel(value):

&#x20;   if value.strip().lower() in CANCEL\_KEYWORDS:

&#x20;       raise UserCancelledOperation("Action cancelled. Returning to the main menu.")

&#x20;   return value





\# This asks for required text and returns a non-empty value

\# so important ids, names, and urls are not left blank.

def prompt\_required(label):

&#x20;   while True:

&#x20;       value = \_check\_cancel(input(f"{label}: ").strip())

&#x20;       if value:

&#x20;           return value

&#x20;       print(f"{label} is required. Type 'back' to return to the main menu.")





\# This asks for a weekday, normalizes it, and returns the cleaned day

\# so the menu sends valid day names into the manager layer.

def prompt\_day(label, allow\_blank=False):

&#x20;   valid\_days\_text = ", ".join(sorted(VALID\_DAYS))



&#x20;   while True:

&#x20;       value = \_check\_cancel(input(f"{label}: ").strip())



&#x20;       if not value and allow\_blank:

&#x20;           return ""



&#x20;       normalized\_value = value.title()

&#x20;       if normalized\_value in VALID\_DAYS:

&#x20;           return normalized\_value



&#x20;       print(f"Invalid day. Use a full day name: {valid\_days\_text}. Type 'back' to cancel.")





\# This asks for one time value and returns a validated HH:MM string

\# so add and edit flows get clean time input from the user.

def prompt\_time(label, allow\_blank=False):

&#x20;   while True:

&#x20;       value = \_check\_cancel(input(f"{label}: ").strip())



&#x20;       if not value and allow\_blank:

&#x20;           return ""



&#x20;       try:

&#x20;           datetime.strptime(value, "%H:%M")

&#x20;           return value

&#x20;       except ValueError:

&#x20;           print("Invalid time. Use HH:MM format, for example 09:30. Type 'back' to cancel.")





\# This asks for optional text and returns it unchanged when present

\# so edit can keep old values when the user leaves a field blank.

def prompt\_optional(label):

&#x20;   return \_check\_cancel(

&#x20;       input(f"{label} (leave blank to keep current value): ").strip()

&#x20;   )





\# This asks for a yes/no answer and returns True or False

\# so risky actions like delete can require explicit confirmation.

def prompt\_yes\_no(label):

&#x20;   while True:

&#x20;       value = \_check\_cancel(input(f"{label} (y/n): ").strip()).lower()

&#x20;       if value in {"y", "yes"}:

&#x20;           return True

&#x20;       if value in {"n", "no"}:

&#x20;           return False

&#x20;       print("Please enter y or n. Type 'back' to cancel.")





\# This loads active timetable data and prints it as a table

\# so the user can clearly review current class entries.

def show\_timetable():

&#x20;   timetable = load\_timetable()



&#x20;   if not timetable:

&#x20;       print("No timetable entries found.")

&#x20;       return



&#x20;   rows = \[

&#x20;       \[

&#x20;           entry\["id"],

&#x20;           entry\["day"],

&#x20;           entry\["start\_time"],

&#x20;           entry\["end\_time"],

&#x20;           entry\["class\_name"],

&#x20;           entry\["classroom\_url"],

&#x20;       ]

&#x20;       for entry in \_sorted\_timetable\_entries(timetable)

&#x20;   ]

&#x20;   print("\\nCurrent timetable:")

&#x20;   \_print\_table(

&#x20;       \["ID", "Day", "Start", "End", "Class Name", "Classroom URL"],

&#x20;       rows,

&#x20;   )





\# This sorts timetable entries by weekday and start time and returns that list

\# so every timetable display stays consistent and easy to read.

def \_sorted\_timetable\_entries(timetable):

&#x20;   day\_order\_map = {day: index for index, day in enumerate(DAY\_ORDER)}

&#x20;   return sorted(

&#x20;       timetable,

&#x20;       key=lambda entry: (

&#x20;           day\_order\_map.get(entry\["day"], len(DAY\_ORDER)),

&#x20;           entry\["start\_time"],

&#x20;           entry\["id"],

&#x20;       ),

&#x20;   )





\# This prints any list of rows as an aligned text table

\# so timetable and recycle-bin views share one display format.

def \_print\_table(headers, rows):

&#x20;   column\_widths = \[

&#x20;       max(len(str(item)) for item in \[header] + \[row\[index] for row in rows])

&#x20;       for index, header in enumerate(headers)

&#x20;   ]



&#x20;   def format\_row(row):

&#x20;       return " | ".join(

&#x20;           str(value).ljust(column\_widths\[index]) for index, value in enumerate(row)

&#x20;       )



&#x20;   separator = "-+-".join("-" \* width for width in column\_widths)



&#x20;   print(format\_row(headers))

&#x20;   print(separator)

&#x20;   for row in rows:

&#x20;       print(format\_row(row))





\# This loads recycle-bin data, prints it, and returns whether data existed

\# so callers can avoid asking for recycle ids when the bin is empty.

def show\_recycle\_bin():

&#x20;   recycle\_bin = load\_recycle\_bin()



&#x20;   if not recycle\_bin:

&#x20;       print("Recycle bin is empty.")

&#x20;       return False



&#x20;   rows = \[

&#x20;       \[

&#x20;           record\["recycle\_id"],

&#x20;           record\["entry"]\["id"],

&#x20;           record\["entry"]\["day"],

&#x20;           record\["entry"]\["start\_time"],

&#x20;           record\["entry"]\["end\_time"],

&#x20;           record\["entry"]\["class\_name"],

&#x20;           record\["deleted\_at"],

&#x20;       ]

&#x20;       for record in recycle\_bin

&#x20;   ]



&#x20;   print("\\nRecycle bin:")

&#x20;   \_print\_table(

&#x20;       \["Recycle ID", "Entry ID", "Day", "Start", "End", "Class Name", "Deleted At"],

&#x20;       rows,

&#x20;   )

&#x20;   return True





\# This guides the user through duplicate-slot cleanup on startup

\# and returns whether the app can continue normal work afterward.

def resolve\_duplicate\_slots(error):

&#x20;   print("\\nDuplicate timetable slots were found in timetable.json.")



&#x20;   for group\_number, duplicate\_group in enumerate(error.duplicates, start=1):

&#x20;       first\_entry = duplicate\_group\[0]

&#x20;       print(

&#x20;           f"\\nDuplicate group {group\_number}: "

&#x20;           f"{first\_entry\['day']} {first\_entry\['start\_time']}-{first\_entry\['end\_time']}"

&#x20;       )

&#x20;       for entry in duplicate\_group:

&#x20;           print(

&#x20;               f"  ID: {entry\['id']} | Class: {entry\['class\_name']} | "

&#x20;               f"URL: {entry\['classroom\_url']}"

&#x20;           )



&#x20;   if not prompt\_yes\_no("Do you want to delete duplicate entries now?"):

&#x20;       print("Please clean the duplicate timetable entries before continuing.")

&#x20;       return False



&#x20;   for duplicate\_group in error.duplicates:

&#x20;       while True:

&#x20;           keep\_id = prompt\_required(

&#x20;               f"Enter the ID you want to keep for "

&#x20;               f"{duplicate\_group\[0]\['day']} "

&#x20;               f"{duplicate\_group\[0]\['start\_time']}-{duplicate\_group\[0]\['end\_time']}"

&#x20;           )

&#x20;           valid\_ids = {entry\["id"] for entry in duplicate\_group}

&#x20;           if keep\_id not in valid\_ids:

&#x20;               print("Invalid ID. Please choose one of the IDs shown above.")

&#x20;               continue



&#x20;           for entry in duplicate\_group:

&#x20;               if entry\["id"] != keep\_id:

&#x20;                   delete\_entry(entry\["id"], allow\_duplicate\_slots=True)

&#x20;                   print(f"Deleted duplicate entry: {entry\['id']}")

&#x20;           break



&#x20;   print("Duplicate timetable entries were cleaned successfully.")

&#x20;   return True





\# This collects add-entry input, validates it step by step,

\# and creates a new timetable entry through the manager layer.

def handle\_add():

&#x20;   print("\\nAdd Entry. Type 'back' at any prompt to return to the main menu.")

&#x20;   day = prompt\_day("Day")

&#x20;   while True:

&#x20;       start\_time = prompt\_time("Start time (HH:MM)")

&#x20;       end\_time = prompt\_time("End time (HH:MM)")

&#x20;       try:

&#x20;           start\_time, end\_time = validate\_time\_range(start\_time, end\_time)

&#x20;           break

&#x20;       except ValueError as exc:

&#x20;           print(f"Error: {exc}")



&#x20;   class\_name = prompt\_required("Class name")

&#x20;   url = prompt\_required("Classroom URL")



&#x20;   entry = add\_entry(day, start\_time, end\_time, class\_name, url)

&#x20;   print(f"Entry added successfully: {entry\['id']}")





\# This collects edit input, keeps unchanged values when fields are blank,

\# and returns control after updating the selected timetable entry.

def handle\_edit():

&#x20;   print("\\nEdit Entry. Type 'back' at any prompt to return to the main menu.")

&#x20;   entry\_id = prompt\_required("Entry ID to edit")

&#x20;   current\_entry = get\_entry\_by\_id(entry\_id)

&#x20;   print("Enter only the fields you want to update.")



&#x20;   updates = {}

&#x20;   day = prompt\_day("New day (leave blank to keep current value)", allow\_blank=True)



&#x20;   while True:

&#x20;       start\_time = prompt\_time(

&#x20;           "New start time (HH:MM) (leave blank to keep current value)",

&#x20;           allow\_blank=True,

&#x20;       )

&#x20;       end\_time = prompt\_time(

&#x20;           "New end time (HH:MM) (leave blank to keep current value)",

&#x20;           allow\_blank=True,

&#x20;       )



&#x20;       proposed\_start\_time = start\_time or current\_entry\["start\_time"]

&#x20;       proposed\_end\_time = end\_time or current\_entry\["end\_time"]



&#x20;       try:

&#x20;           validate\_time\_range(proposed\_start\_time, proposed\_end\_time)

&#x20;           break

&#x20;       except ValueError as exc:

&#x20;           print(f"Error: {exc}")



&#x20;   class\_name = prompt\_optional("New class name")

&#x20;   classroom\_url = prompt\_optional("New classroom URL")



&#x20;   if day:

&#x20;       updates\["day"] = day

&#x20;   if start\_time:

&#x20;       updates\["start\_time"] = start\_time

&#x20;   if end\_time:

&#x20;       updates\["end\_time"] = end\_time

&#x20;   if class\_name:

&#x20;       updates\["class\_name"] = class\_name

&#x20;   if classroom\_url:

&#x20;       updates\["classroom\_url"] = classroom\_url



&#x20;   if not updates:

&#x20;       print("No changes entered.")

&#x20;       return



&#x20;   updated\_entry = edit\_entry(entry\_id, \*\*updates)

&#x20;   print(f"Entry updated successfully: {updated\_entry\['id']}")





\# This shows the chosen class, asks for confirmation,

\# and moves the entry into the recycle bin if approved.

def handle\_delete():

&#x20;   print("\\nDelete Entry. Type 'back' at any prompt to return to the main menu.")

&#x20;   entry\_id = prompt\_required("Entry ID to delete")

&#x20;   entry = get\_entry\_by\_id(entry\_id)



&#x20;   print("\\nEntry selected for deletion:")

&#x20;   \_print\_table(

&#x20;       \["ID", "Day", "Start", "End", "Class Name", "Classroom URL"],

&#x20;       \[\[

&#x20;           entry\["id"],

&#x20;           entry\["day"],

&#x20;           entry\["start\_time"],

&#x20;           entry\["end\_time"],

&#x20;           entry\["class\_name"],

&#x20;           entry\["classroom\_url"],

&#x20;       ]],

&#x20;   )



&#x20;   if not prompt\_yes\_no("Do you want to move this entry to the recycle bin"):

&#x20;       print("Deletion cancelled. Entry was not removed.")

&#x20;       return



&#x20;   deleted\_record = delete\_entry(entry\_id)

&#x20;   print(

&#x20;       f"Entry moved to recycle bin successfully: "

&#x20;       f"{deleted\_record\['entry']\['id']} -> {deleted\_record\['recycle\_id']}"

&#x20;   )





\# This runs the recycle-bin submenu for viewing, restoring,

\# and permanently deleting archived entries.

def handle\_recycle\_bin():

&#x20;   print("\\nRecycle Bin. Type 'back' at any prompt to return to the main menu.")



&#x20;   while True:

&#x20;       print("\\nRecycle Bin Menu")

&#x20;       print("1. Show recycle bin")

&#x20;       print("2. Restore entry")

&#x20;       print("3. Delete permanently")

&#x20;       print("4. Return to main menu")



&#x20;       choice = \_check\_cancel(input("Choose an option: ").strip())



&#x20;       if choice == "1":

&#x20;           show\_recycle\_bin()

&#x20;       elif choice == "2":

&#x20;           if not show\_recycle\_bin():

&#x20;               continue

&#x20;           recycle\_id = prompt\_required("Recycle ID to restore")

&#x20;           record = get\_recycle\_record\_by\_id(recycle\_id)

&#x20;           restored\_entry = restore\_entry(recycle\_id)

&#x20;           print(

&#x20;               f"Restored entry successfully: "

&#x20;               f"{record\['entry']\['id']} from {record\['recycle\_id']}"

&#x20;           )

&#x20;           print(f"Restored timetable entry id: {restored\_entry\['id']}")

&#x20;       elif choice == "3":

&#x20;           if not show\_recycle\_bin():

&#x20;               continue

&#x20;           recycle\_id = prompt\_required("Recycle ID to delete permanently")

&#x20;           record = get\_recycle\_record\_by\_id(recycle\_id)

&#x20;           if not prompt\_yes\_no(

&#x20;               f"Are you sure you want to permanently delete {record\['entry']\['id']} from the recycle bin"

&#x20;           ):

&#x20;               print("Permanent deletion cancelled.")

&#x20;               continue

&#x20;           deleted\_record = permanently\_delete\_recycle\_entry(recycle\_id)

&#x20;           print(

&#x20;               f"Permanently deleted recycle bin entry: "

&#x20;               f"{deleted\_record\['recycle\_id']}"

&#x20;           )

&#x20;       elif choice == "4":

&#x20;           print("Returning to the main menu.")

&#x20;           return

&#x20;       else:

&#x20;           print("Invalid choice. Please enter a number from 1 to 4.")





\# This is the main program loop that validates startup data,

\# shows the menu, and routes the user into each feature.

def main():

&#x20;   while True:

&#x20;       try:

&#x20;           load\_timetable()

&#x20;       except DuplicateTimeSlotError as exc:

&#x20;           if not resolve\_duplicate\_slots(exc):

&#x20;               break

&#x20;           continue

&#x20;       except Exception as exc:

&#x20;           print(f"Error: {exc}")

&#x20;           break



&#x20;       print("\\nTimetable Manager")

&#x20;       print("1. Show timetable")

&#x20;       print("2. Add entry")

&#x20;       print("3. Edit entry")

&#x20;       print("4. Delete entry")

&#x20;       print("5. Recycle bin")

&#x20;       print("6. Exit")



&#x20;       choice = input("Choose an option: ").strip()



&#x20;       try:

&#x20;           if choice == "1":

&#x20;               show\_timetable()

&#x20;           elif choice == "2":

&#x20;               handle\_add()

&#x20;           elif choice == "3":

&#x20;               handle\_edit()

&#x20;           elif choice == "4":

&#x20;               handle\_delete()

&#x20;           elif choice == "5":

&#x20;               handle\_recycle\_bin()

&#x20;           elif choice == "6":

&#x20;               print("Goodbye.")

&#x20;               break

&#x20;           else:

&#x20;               print("Invalid choice. Please enter a number from 1 to 6.")

&#x20;       except UserCancelledOperation as exc:

&#x20;           print(exc)

&#x20;       except DuplicateTimeSlotError as exc:

&#x20;           resolve\_duplicate\_slots(exc)

&#x20;       except Exception as exc:

&#x20;           print(f"Error: {exc}")





if \_\_name\_\_ == "\_\_main\_\_":

&#x20;   main()



)



**#timetable.json**

(

\[

&#x20; {

&#x20;   "id": "tue\_1",

&#x20;   "day": "Tuesday",

&#x20;   "start\_time": "08:00",

&#x20;   "end\_time": "09:00",

&#x20;   "class\_name": "Machine Learning",

&#x20;   "classroom\_url": "https://classroom.google.com/c/ML2026"

&#x20; },

&#x20; {

&#x20;   "id": "tue\_2",

&#x20;   "day": "Tuesday",

&#x20;   "start\_time": "09:00",

&#x20;   "end\_time": "10:00",

&#x20;   "class\_name": "Programming for AI",

&#x20;   "classroom\_url": "https://classroom.google.com/c/PAI2026"

&#x20; },

&#x20; {

&#x20;   "id": "tue\_3",

&#x20;   "day": "Tuesday",

&#x20;   "start\_time": "10:00",

&#x20;   "end\_time": "11:00",

&#x20;   "class\_name": "Expository Writing",

&#x20;   "classroom\_url": "https://classroom.google.com/c/EW2026"

&#x20; },

&#x20; {

&#x20;   "id": "tue\_4",

&#x20;   "day": "Tuesday",

&#x20;   "start\_time": "11:00",

&#x20;   "end\_time": "12:00",

&#x20;   "class\_name": "Translation of Quran",

&#x20;   "classroom\_url": "https://classroom.google.com/c/TQ2026"

&#x20; },

&#x20; {

&#x20;   "id": "tue\_5",

&#x20;   "day": "Tuesday",

&#x20;   "start\_time": "12:00",

&#x20;   "end\_time": "13:00",

&#x20;   "class\_name": "Theory of Automata",

&#x20;   "classroom\_url": "https://classroom.google.com/c/TOA2026"

&#x20; },

&#x20; {

&#x20;   "id": "wed\_1",

&#x20;   "day": "Wednesday",

&#x20;   "start\_time": "08:00",

&#x20;   "end\_time": "09:00",

&#x20;   "class\_name": "Programming for AI",

&#x20;   "classroom\_url": "https://classroom.google.com/c/PAI2026"

&#x20; },

&#x20; {

&#x20;   "id": "wed\_2",

&#x20;   "day": "Wednesday",

&#x20;   "start\_time": "09:00",

&#x20;   "end\_time": "10:00",

&#x20;   "class\_name": "Islamic Studies",

&#x20;   "classroom\_url": "https://classroom.google.com/c/IS2026"

&#x20; },

&#x20; {

&#x20;   "id": "wed\_3",

&#x20;   "day": "Wednesday",

&#x20;   "start\_time": "10:00",

&#x20;   "end\_time": "11:00",

&#x20;   "class\_name": "Programming for AI",

&#x20;   "classroom\_url": "https://classroom.google.com/c/PAI2026"

&#x20; },

&#x20; {

&#x20;   "id": "thu\_1",

&#x20;   "day": "Thursday",

&#x20;   "start\_time": "08:00",

&#x20;   "end\_time": "09:00",

&#x20;   "class\_name": "Theory of Automata",

&#x20;   "classroom\_url": "https://classroom.google.com/c/TOA2026"

&#x20; },

&#x20; {

&#x20;   "id": "thu\_2",

&#x20;   "day": "Thursday",

&#x20;   "start\_time": "09:00",

&#x20;   "end\_time": "10:00",

&#x20;   "class\_name": "COAL",

&#x20;   "classroom\_url": "https://classroom.google.com/c/COAL2026"

&#x20; },

&#x20; {

&#x20;   "id": "thu\_3",

&#x20;   "day": "Thursday",

&#x20;   "start\_time": "10:00",

&#x20;   "end\_time": "11:00",

&#x20;   "class\_name": "Machine Learning",

&#x20;   "classroom\_url": "https://classroom.google.com/c/ML2026"

&#x20; },

&#x20; {

&#x20;   "id": "thu\_4",

&#x20;   "day": "Thursday",

&#x20;   "start\_time": "11:00",

&#x20;   "end\_time": "12:00",

&#x20;   "class\_name": "COAL",

&#x20;   "classroom\_url": "https://classroom.google.com/c/COAL2026"

&#x20; },

&#x20; {

&#x20;   "id": "fri\_1",

&#x20;   "day": "Friday",

&#x20;   "start\_time": "08:00",

&#x20;   "end\_time": "09:00",

&#x20;   "class\_name": "Expository Writing",

&#x20;   "classroom\_url": "https://classroom.google.com/c/EW2026"

&#x20; },

&#x20; {

&#x20;   "id": "fri\_2",

&#x20;   "day": "Friday",

&#x20;   "start\_time": "09:00",

&#x20;   "end\_time": "10:00",

&#x20;   "class\_name": "Machine Learning",

&#x20;   "classroom\_url": "https://classroom.google.com/c/ML2026"

&#x20; },

&#x20; {

&#x20;   "id": "fri\_3",

&#x20;   "day": "Friday",

&#x20;   "start\_time": "10:00",

&#x20;   "end\_time": "11:00",

&#x20;   "class\_name": "COAL",

&#x20;   "classroom\_url": "https://classroom.google.com/c/COAL2026"

&#x20; },

&#x20; {

&#x20;   "id": "mon\_1",

&#x20;   "day": "Monday",

&#x20;   "start\_time": "08:00",

&#x20;   "end\_time": "09:00",

&#x20;   "class\_name": "ML",

&#x20;   "classroom\_url": "https://meet.google.com/vhz-dfxm-uca"

&#x20; },

&#x20; {

&#x20;   "id": "mon\_2",

&#x20;   "day": "Monday",

&#x20;   "start\_time": "10:00",

&#x20;   "end\_time": "11:00",

&#x20;   "class\_name": "COAL",

&#x20;   "classroom\_url": "https://meet.google.com/vhz-dfxm-uca"

&#x20; }

]

)



**#updated timetable\_manager()**

(

import copy, json, os, re, tempfile, threading

from datetime import datetime



from app\_logger\_manager import (

&#x20;   get\_app\_logger,

&#x20;   log\_debug,

&#x20;   log\_error,

&#x20;   log\_event,

&#x20;   log\_info,

&#x20;   log\_warning,

)



TIMETABLE\_FILE = "timetable.json"

RECYCLE\_BIN\_FILE = "recycle\_bin.json"

VALID\_FIELDS = {"day", "start\_time", "end\_time", "class\_name", "classroom\_url"}

RECYCLE\_ID\_PATTERN = re.compile(r"^bin\_\\d+$")

ENTRY\_ID\_PATTERN = re.compile(r"^\[a-z]{3}\_\\d+$")

TIME\_PATTERN = re.compile(r"^\\d{2}:\\d{2}$")

VALID\_DAYS = {"Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"}

VALID\_YES\_VALUES = {"y", "yes"}

VALID\_NO\_VALUES = {"n", "no"}





class TimetableError(Exception): ...

class TimetableValidationError(TimetableError, ValueError): ...

class TimetableStorageError(TimetableError, OSError): ...

class TimetableNotFoundError(TimetableError, LookupError): ...

class TimetableConflictError(TimetableError, ValueError): ...





class DuplicateTimeSlotError(TimetableConflictError):

&#x20;   def \_\_init\_\_(self, duplicates):

&#x20;       self.duplicates = duplicates

&#x20;       super().\_\_init\_\_("Duplicate timetable slots found in stored data.")





class InvalidTimetableEntriesError(TimetableValidationError):

&#x20;   def \_\_init\_\_(self, issues):

&#x20;       self.issues = issues

&#x20;       first = issues\[0]\["error"] if issues else "Unknown validation error."

&#x20;       super().\_\_init\_\_(f"Invalid timetable entries found in stored data ({len(issues)} issue(s)). First issue: {first}")





TIMETABLE\_CACHE = None

RECYCLE\_BIN\_CACHE = None

TIMETABLE\_ID\_INDEX = None

TIMETABLE\_SLOT\_INDEX = None

TIMETABLE\_PREFIX\_INDEX = None

RECYCLE\_INDEX\_MAP = None

STORAGE\_LOCK = threading.RLock()

SHUTDOWN\_EVENT = threading.Event()

LOGGER = get\_app\_logger("timetable\_manager")





def \_where(function\_name): return f"timetable\_manager.{function\_name}"



def \_safe\_entry\_context(entry):

&#x20;   if not isinstance(entry, dict):

&#x20;       return {"entry\_type": type(entry).\_\_name\_\_}

&#x20;   return {key: entry.get(key) for key in ("id", "day", "start\_time", "end\_time", "class\_name", "joined") if key in entry}



def \_safe\_record\_context(record):

&#x20;   if not isinstance(record, dict):

&#x20;       return {"record\_type": type(record).\_\_name\_\_}

&#x20;   return {"recycle\_id": record.get("recycle\_id"), "deleted\_at": record.get("deleted\_at"), "entry": \_safe\_entry\_context(record.get("entry"))}



def \_safe\_issue\_context(issues):

&#x20;   if not issues:

&#x20;       return {}

&#x20;   first\_issue = issues\[0]

&#x20;   return {

&#x20;       "issue\_count": len(issues),

&#x20;       "first\_issue": {"index": first\_issue.get("index"), "fields": first\_issue.get("fields", \[]), "error": first\_issue.get("error")},

&#x20;   }



def \_log\_known\_issue(function\_name, message, exc, context=None):

&#x20;   log\_warning(LOGGER, message, what=message, where=\_where(function\_name), why=str(exc), context=context)



def \_log\_unexpected\_issue(function\_name, message, context=None):

&#x20;   log\_error(LOGGER, message, what=message, where=\_where(function\_name), why="Unexpected exception escaped a timetable manager operation.", context=context, exc\_info=True)



def \_clone\_entry(entry): return dict(entry)

def \_clone\_timetable(timetable): return \[\_clone\_entry(entry) for entry in timetable]

def \_clone\_recycle\_record(record): return {"recycle\_id": record\["recycle\_id"], "deleted\_at": record\["deleted\_at"], "entry": \_clone\_entry(record\["entry"])}

def \_clone\_recycle\_bin(recycle\_bin): return \[\_clone\_recycle\_record(record) for record in recycle\_bin]





def \_validate\_text(value, field\_name):

&#x20;   if not isinstance(value, str):

&#x20;       raise TimetableValidationError(f"{field\_name} must be a string.")

&#x20;   value = value.strip()

&#x20;   if not value:

&#x20;       raise TimetableValidationError(f"{field\_name} cannot be empty.")

&#x20;   return value



def validate\_required\_text(value, field\_name): return \_validate\_text(value, field\_name)

def validate\_optional\_text(value):

&#x20;   if not isinstance(value, str):

&#x20;       raise TimetableValidationError("Optional input must be a string.")

&#x20;   return value.strip()



def \_validate\_recycle\_id(value):

&#x20;   value = \_validate\_text(value, "recycle\_id")

&#x20;   if not RECYCLE\_ID\_PATTERN.fullmatch(value):

&#x20;       raise TimetableValidationError("recycle\_id must be in the format 'bin\_<number>', for example 'bin\_1'.")

&#x20;   return value



def validate\_recycle\_id(value): return \_validate\_recycle\_id(value)



def \_validate\_entry\_id(value):

&#x20;   value = \_validate\_text(value, "entry\_id")

&#x20;   if not ENTRY\_ID\_PATTERN.fullmatch(value):

&#x20;       raise TimetableValidationError("entry\_id must be in the format '<day\_prefix>\_<number>', for example 'mon\_1'.")

&#x20;   return value



def validate\_entry\_id(value): return \_validate\_entry\_id(value)



def \_parse\_time\_to\_minutes(value, field\_name):

&#x20;   value = \_validate\_text(value, field\_name)

&#x20;   if not TIME\_PATTERN.fullmatch(value):

&#x20;       raise TimetableValidationError(f"{field\_name} must be in HH:MM format.")

&#x20;   hours = int(value\[:2])

&#x20;   minutes = int(value\[3:5])

&#x20;   if hours > 23 or minutes > 59:

&#x20;       raise TimetableValidationError(f"{field\_name} must be in HH:MM format.")

&#x20;   return value, hours \* 60 + minutes



def \_validate\_time(value, field\_name):

&#x20;   value, \_ = \_parse\_time\_to\_minutes(value, field\_name)

&#x20;   return value



def validate\_time\_input(value, field\_name="time"): return \_validate\_time(value, field\_name)



def \_validate\_day(value):

&#x20;   value = \_validate\_text(value, "day").title()

&#x20;   if value not in VALID\_DAYS:

&#x20;       raise TimetableValidationError(f"day must be a valid day name: {', '.join(sorted(VALID\_DAYS))}.")

&#x20;   return value



def validate\_day\_input(value): return \_validate\_day(value)



def validate\_yes\_no\_input(value):

&#x20;   value = \_validate\_text(value, "confirmation").lower()

&#x20;   if value in VALID\_YES\_VALUES: return True

&#x20;   if value in VALID\_NO\_VALUES: return False

&#x20;   raise TimetableValidationError("Please enter y or n.")



def \_validate\_joined\_flag(value):

&#x20;   if not isinstance(value, bool):

&#x20;       raise TimetableValidationError("joined must be a boolean value.")

&#x20;   return value



def validate\_time\_range(start\_time, end\_time):

&#x20;   start\_time, start\_minutes = \_parse\_time\_to\_minutes(start\_time, "start\_time")

&#x20;   end\_time, end\_minutes = \_parse\_time\_to\_minutes(end\_time, "end\_time")

&#x20;   gap\_minutes = end\_minutes - start\_minutes

&#x20;   if gap\_minutes < 60:

&#x20;       raise TimetableValidationError("The gap between start\_time and end\_time must be at least 1 hour.")

&#x20;   if gap\_minutes > 180:

&#x20;       raise TimetableValidationError("The gap between start\_time and end\_time must not be more than 3 hours.")

&#x20;   return start\_time, end\_time



def \_normalize\_update\_fields(kwargs):

&#x20;   normalized = dict(kwargs)

&#x20;   if "url" in normalized:

&#x20;       if "classroom\_url" in normalized:

&#x20;           raise TimetableValidationError("Use either 'url' or 'classroom\_url', not both.")

&#x20;       normalized\["classroom\_url"] = normalized.pop("url")

&#x20;   invalid\_fields = set(normalized) - VALID\_FIELDS

&#x20;   if invalid\_fields:

&#x20;       raise TimetableValidationError(f"Invalid field(s): {', '.join(sorted(invalid\_fields))}")

&#x20;   return normalized



def \_validate\_entry(entry):

&#x20;   if not isinstance(entry, dict):

&#x20;       raise TimetableValidationError("Each timetable entry must be an object.")

&#x20;   start\_time, end\_time = validate\_time\_range(entry.get("start\_time"), entry.get("end\_time"))

&#x20;   return {

&#x20;       "id": \_validate\_entry\_id(entry.get("id")),

&#x20;       "day": \_validate\_day(entry.get("day")),

&#x20;       "start\_time": start\_time,

&#x20;       "end\_time": end\_time,

&#x20;       "class\_name": \_validate\_text(entry.get("class\_name"), "class\_name"),

&#x20;       "classroom\_url": \_validate\_text(entry.get("classroom\_url"), "classroom\_url"),

&#x20;       "joined": \_validate\_joined\_flag(entry.get("joined", False)),

&#x20;   }



def build\_validated\_entry(entry\_id, day, start\_time, end\_time, class\_name, classroom\_url):

&#x20;   return \_validate\_entry({"id": entry\_id, "day": day, "start\_time": start\_time, "end\_time": end\_time, "class\_name": class\_name, "classroom\_url": classroom\_url, "joined": False})



def \_validate\_recycle\_record(record):

&#x20;   if not isinstance(record, dict):

&#x20;       raise TimetableValidationError("Each recycle bin record must be an object.")

&#x20;   deleted\_at = \_validate\_text(record.get("deleted\_at"), "deleted\_at")

&#x20;   try:

&#x20;       datetime.fromisoformat(deleted\_at)

&#x20;   except ValueError as exc:

&#x20;       raise TimetableValidationError("deleted\_at must be a valid ISO datetime string.") from exc

&#x20;   return {"recycle\_id": \_validate\_recycle\_id(record.get("recycle\_id")), "deleted\_at": deleted\_at, "entry": \_validate\_entry(record.get("entry"))}



def \_slot\_key(entry): return entry\["day"], entry\["start\_time"], entry\["end\_time"]



def \_build\_timetable\_indexes(timetable):

&#x20;   id\_to\_index, slot\_to\_indexes, prefix\_to\_numbers = {}, {}, {}

&#x20;   for i, entry in enumerate(timetable):

&#x20;       entry\_id = entry\["id"]

&#x20;       if entry\_id in id\_to\_index: raise TimetableValidationError(f"Duplicate id found in timetable: {entry\_id}")

&#x20;       id\_to\_index\[entry\_id] = i

&#x20;       slot\_to\_indexes.setdefault(\_slot\_key(entry), \[]).append(i)

&#x20;       prefix, suffix = entry\_id.split("\_", 1)

&#x20;       prefix\_to\_numbers.setdefault(prefix, set()).add(int(suffix))

&#x20;   return id\_to\_index, slot\_to\_indexes, prefix\_to\_numbers



def \_build\_recycle\_index\_map(recycle\_bin):

&#x20;   result = {}

&#x20;   for i, record in enumerate(recycle\_bin):

&#x20;       rid = record\["recycle\_id"]

&#x20;       if rid in result: raise TimetableValidationError(f"Duplicate recycle\_id found in recycle bin: {rid}")

&#x20;       result\[rid] = i

&#x20;   return result



def \_refresh\_timetable\_cache(validated):

&#x20;   global TIMETABLE\_CACHE, TIMETABLE\_ID\_INDEX, TIMETABLE\_SLOT\_INDEX, TIMETABLE\_PREFIX\_INDEX

&#x20;   TIMETABLE\_CACHE = validated

&#x20;   TIMETABLE\_ID\_INDEX, TIMETABLE\_SLOT\_INDEX, TIMETABLE\_PREFIX\_INDEX = \_build\_timetable\_indexes(validated)



def \_refresh\_recycle\_bin\_cache(validated):

&#x20;   global RECYCLE\_BIN\_CACHE, RECYCLE\_INDEX\_MAP

&#x20;   RECYCLE\_BIN\_CACHE = validated

&#x20;   RECYCLE\_INDEX\_MAP = \_build\_recycle\_index\_map(validated)



def \_get\_timetable\_indexes(allow\_duplicate\_slots=False):

&#x20;   \_get\_timetable\_ref(allow\_duplicate\_slots=allow\_duplicate\_slots)

&#x20;   return TIMETABLE\_ID\_INDEX, TIMETABLE\_SLOT\_INDEX, TIMETABLE\_PREFIX\_INDEX



def \_get\_recycle\_index\_map():

&#x20;   \_get\_recycle\_bin\_ref()

&#x20;   return RECYCLE\_INDEX\_MAP



def \_get\_first\_available\_number(used\_numbers):

&#x20;   n = 1

&#x20;   while n in used\_numbers: n += 1

&#x20;   return n



def \_find\_repair\_entry\_id(raw\_entries, index\_to\_replace, proposed\_id, day):

&#x20;   try:

&#x20;       return \_validate\_entry\_id(proposed\_id)

&#x20;   except TimetableValidationError:

&#x20;       pass



&#x20;   prefix = \_validate\_day(day)\[:3].lower()

&#x20;   used\_numbers = set()



&#x20;   for index, entry in enumerate(raw\_entries):

&#x20;       if index == index\_to\_replace or not isinstance(entry, dict):

&#x20;           continue

&#x20;       entry\_id = entry.get("id")

&#x20;       if not isinstance(entry\_id, str):

&#x20;           continue

&#x20;       match = ENTRY\_ID\_PATTERN.fullmatch(entry\_id.strip())

&#x20;       if not match:

&#x20;           continue

&#x20;       entry\_prefix, suffix = entry\_id.strip().split("\_", 1)

&#x20;       if entry\_prefix == prefix:

&#x20;           used\_numbers.add(int(suffix))



&#x20;   return f"{prefix}\_{\_get\_first\_available\_number(used\_numbers)}"



def \_ensure\_no\_duplicate\_slots(timetable, slot\_to\_indexes):

&#x20;   duplicates = \[\[copy.deepcopy(timetable\[i]) for i in idxs] for idxs in slot\_to\_indexes.values() if len(idxs) > 1]

&#x20;   if duplicates: raise DuplicateTimeSlotError(duplicates)



def \_ensure\_slot\_available(slot\_to\_indexes, day, start\_time, end\_time, ignore\_entry\_id=None, id\_to\_index=None):

&#x20;   for index in slot\_to\_indexes.get((day, start\_time, end\_time), \[]):

&#x20;       if ignore\_entry\_id and id\_to\_index and id\_to\_index.get(ignore\_entry\_id) == index: continue

&#x20;       raise TimetableConflictError(f"A timetable entry already exists for {day} from {start\_time} to {end\_time}.")



def \_read\_json\_list\_file(path):

&#x20;   try:

&#x20;       with open(path, "r", encoding="utf-8") as file:

&#x20;           data = json.load(file)

&#x20;   except FileNotFoundError as exc:

&#x20;       log\_warning(LOGGER, f"Storage file was not found: {path}", what="Storage read failed because the file was missing.", where=\_where("\_read\_json\_list\_file"), why="The requested JSON file does not exist yet.", context={"path": path})

&#x20;       raise FileNotFoundError(f"{path} was not found.") from exc

&#x20;   except json.JSONDecodeError as exc:

&#x20;       log\_error(LOGGER, f"Storage file contains invalid JSON: {path}", what="Storage read failed because JSON parsing failed.", where=\_where("\_read\_json\_list\_file"), why="The JSON file is corrupted or malformed.", context={"path": path}, exc\_info=True)

&#x20;       raise TimetableValidationError(f"{path} contains invalid JSON.") from exc

&#x20;   except OSError as exc:

&#x20;       log\_error(LOGGER, f"Storage file could not be read: {path}", what="Storage read failed because the file system rejected the read.", where=\_where("\_read\_json\_list\_file"), why=str(exc), context={"path": path}, exc\_info=True)

&#x20;       raise TimetableStorageError(f"Could not read {path}: {exc}") from exc

&#x20;   if not isinstance(data, list):

&#x20;       log\_warning(LOGGER, f"Storage file does not contain a list: {path}", what="Storage validation failed after reading JSON data.", where=\_where("\_read\_json\_list\_file"), why="The top-level JSON structure must be a list.", context={"path": path, "data\_type": type(data).\_\_name\_\_})

&#x20;       raise TimetableValidationError(f"{path} must contain a list of entries.")

&#x20;   log\_debug(LOGGER, f"Read JSON storage file: {path}", what="Loaded JSON data from disk.", where=\_where("\_read\_json\_list\_file"), why="A cache miss required reading structured data from storage.", context={"path": path, "entry\_count": len(data)})

&#x20;   return data



def \_read\_timetable\_file():

&#x20;   return \_read\_json\_list\_file(TIMETABLE\_FILE)



def \_read\_recycle\_bin\_file():

&#x20;   return \_read\_json\_list\_file(RECYCLE\_BIN\_FILE)



def \_atomic\_write\_json\_list\_file(path, data):

&#x20;   directory = os.path.dirname(os.path.abspath(path)) or "."

&#x20;   temp\_path = None

&#x20;   try:

&#x20;       with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=directory, delete=False, suffix=".tmp") as temp\_file:

&#x20;           json.dump(data, temp\_file, indent=2, ensure\_ascii=False)

&#x20;           temp\_path = temp\_file.name

&#x20;       os.replace(temp\_path, path)

&#x20;   except TypeError as exc:

&#x20;       log\_error(LOGGER, f"Storage write rejected non-serializable data for {path}", what="Storage write failed because the data could not be serialized to JSON.", where=\_where("\_atomic\_write\_json\_list\_file"), why=str(exc), context={"path": path}, exc\_info=True)

&#x20;       raise TimetableValidationError(f"{path} contains non-JSON-serializable values: {exc}") from exc

&#x20;   except OSError as exc:

&#x20;       log\_error(LOGGER, f"Storage write failed for {path}", what="Storage write failed while replacing the JSON file.", where=\_where("\_atomic\_write\_json\_list\_file"), why=str(exc), context={"path": path, "temporary\_path": temp\_path}, exc\_info=True)

&#x20;       raise TimetableStorageError(f"Could not save {path}: {exc}") from exc

&#x20;   finally:

&#x20;       if temp\_path and os.path.exists(temp\_path):

&#x20;           try: os.remove(temp\_path)

&#x20;           except OSError: pass

&#x20;   log\_debug(LOGGER, f"Wrote JSON storage file: {path}", what="Persisted JSON data to disk.", where=\_where("\_atomic\_write\_json\_list\_file"), why="A validated storage update needed to be committed atomically.", context={"path": path, "entry\_count": len(data)})



def \_validate\_timetable\_entries(data, allow\_duplicate\_slots=False):

&#x20;   if not isinstance(data, list): raise TimetableValidationError("Timetable data must be a list.")

&#x20;   validated = \[\_validate\_entry(entry) for entry in data]

&#x20;   \_, slot\_to\_indexes, \_ = \_build\_timetable\_indexes(validated)

&#x20;   if not allow\_duplicate\_slots: \_ensure\_no\_duplicate\_slots(validated, slot\_to\_indexes)

&#x20;   return validated



def \_validate\_recycle\_bin(data):

&#x20;   if not isinstance(data, list): raise TimetableValidationError("Recycle bin data must be a list.")

&#x20;   validated = \[\_validate\_recycle\_record(record) for record in data]

&#x20;   \_build\_recycle\_index\_map(validated)

&#x20;   return validated



def \_inspect\_timetable\_entry\_issues\_from\_data(raw\_timetable):

&#x20;   issues = \[]

&#x20;   for index, entry in enumerate(raw\_timetable):

&#x20;       try: \_validate\_entry(entry)

&#x20;       except TimetableError as exc:

&#x20;           issues.append({

&#x20;               "index": index,

&#x20;               "entry": copy.deepcopy(entry),

&#x20;               "error": str(exc),

&#x20;               "fields": \_collect\_invalid\_entry\_fields(entry),

&#x20;           })

&#x20;   return issues



def \_collect\_invalid\_entry\_fields(entry):

&#x20;   if not isinstance(entry, dict):

&#x20;       return \["day", "start\_time", "end\_time", "class\_name", "classroom\_url"]



&#x20;   invalid\_fields = \[]



&#x20;   try:

&#x20;       \_validate\_entry\_id(entry.get("id"))

&#x20;   except TimetableError:

&#x20;       invalid\_fields.append("id")



&#x20;   try:

&#x20;       \_validate\_day(entry.get("day"))

&#x20;   except TimetableError:

&#x20;       invalid\_fields.append("day")



&#x20;   time\_invalid = False

&#x20;   try:

&#x20;       \_validate\_time(entry.get("start\_time"), "start\_time")

&#x20;   except TimetableError:

&#x20;       time\_invalid = True

&#x20;   try:

&#x20;       \_validate\_time(entry.get("end\_time"), "end\_time")

&#x20;   except TimetableError:

&#x20;       time\_invalid = True

&#x20;   if not time\_invalid:

&#x20;       try:

&#x20;           validate\_time\_range(entry.get("start\_time"), entry.get("end\_time"))

&#x20;       except TimetableError:

&#x20;           time\_invalid = True

&#x20;   if time\_invalid:

&#x20;       invalid\_fields.extend(\["start\_time", "end\_time"])



&#x20;   try:

&#x20;       \_validate\_text(entry.get("class\_name"), "class\_name")

&#x20;   except TimetableError:

&#x20;       invalid\_fields.append("class\_name")



&#x20;   try:

&#x20;       \_validate\_text(entry.get("classroom\_url"), "classroom\_url")

&#x20;   except TimetableError:

&#x20;       invalid\_fields.append("classroom\_url")



&#x20;   try:

&#x20;       \_validate\_joined\_flag(entry.get("joined", False))

&#x20;   except TimetableError:

&#x20;       invalid\_fields.append("joined")



&#x20;   ordered\_fields = \[]

&#x20;   for field in \["id", "day", "start\_time", "end\_time", "class\_name", "classroom\_url", "joined"]:

&#x20;       if field in invalid\_fields:

&#x20;           ordered\_fields.append(field)

&#x20;   return ordered\_fields



def \_get\_timetable\_ref(allow\_duplicate\_slots=False):

&#x20;   global TIMETABLE\_CACHE

&#x20;   if TIMETABLE\_CACHE is None:

&#x20;       raw = \_read\_timetable\_file()

&#x20;       issues = \_inspect\_timetable\_entry\_issues\_from\_data(raw)

&#x20;       if issues:

&#x20;           log\_error(LOGGER, "Timetable cache load found invalid stored entries.", what="Rejected invalid timetable data during cache initialization.", where=\_where("\_get\_timetable\_ref"), why="Stored timetable rows failed validation before being cached.", context=\_safe\_issue\_context(issues))

&#x20;           raise InvalidTimetableEntriesError(issues)

&#x20;       validated = \_validate\_timetable\_entries(raw, allow\_duplicate\_slots=allow\_duplicate\_slots)

&#x20;       \_refresh\_timetable\_cache(validated)

&#x20;       log\_info(LOGGER, "Timetable cache initialized from disk.", what="Loaded timetable data into memory.", where=\_where("\_get\_timetable\_ref"), why="The timetable cache was empty and needed initialization.", context={"entry\_count": len(TIMETABLE\_CACHE), "allow\_duplicate\_slots": allow\_duplicate\_slots})

&#x20;   if not allow\_duplicate\_slots:

&#x20;       try:

&#x20;           \_ensure\_no\_duplicate\_slots(TIMETABLE\_CACHE, TIMETABLE\_SLOT\_INDEX)

&#x20;       except DuplicateTimeSlotError as exc:

&#x20;           log\_error(LOGGER, "Timetable cache validation found duplicate time slots.", what="Rejected duplicate timetable slots during cache use.", where=\_where("\_get\_timetable\_ref"), why=str(exc), context={"duplicate\_group\_count": len(exc.duplicates)})

&#x20;           raise

&#x20;   return TIMETABLE\_CACHE



def \_get\_recycle\_bin\_ref():

&#x20;   global RECYCLE\_BIN\_CACHE

&#x20;   if RECYCLE\_BIN\_CACHE is None:

&#x20;       try:

&#x20;           validated = \_validate\_recycle\_bin(\_read\_recycle\_bin\_file())

&#x20;       except FileNotFoundError:

&#x20;           validated = \[]

&#x20;           \_atomic\_write\_json\_list\_file(RECYCLE\_BIN\_FILE, validated)

&#x20;           \_refresh\_recycle\_bin\_cache(validated)

&#x20;           log\_info(LOGGER, "Recycle bin file was created automatically.", what="Initialized an empty recycle bin file.", where=\_where("\_get\_recycle\_bin\_ref"), why="The recycle bin did not exist yet and had to be created safely.", context={"path": RECYCLE\_BIN\_FILE})

&#x20;       else:

&#x20;           \_refresh\_recycle\_bin\_cache(validated)

&#x20;           log\_info(LOGGER, "Recycle bin cache initialized from disk.", what="Loaded recycle bin data into memory.", where=\_where("\_get\_recycle\_bin\_ref"), why="The recycle bin cache was empty and needed initialization.", context={"entry\_count": len(RECYCLE\_BIN\_CACHE)})

&#x20;   return RECYCLE\_BIN\_CACHE



def load\_timetable(allow\_duplicate\_slots=False):

&#x20;   with STORAGE\_LOCK: return \_clone\_timetable(\_get\_timetable\_ref(allow\_duplicate\_slots=allow\_duplicate\_slots))



def load\_recycle\_bin():

&#x20;   with STORAGE\_LOCK: return \_clone\_recycle\_bin(\_get\_recycle\_bin\_ref())



def load\_raw\_timetable():

&#x20;   with STORAGE\_LOCK: return \_read\_timetable\_file()



def inspect\_timetable\_entry\_issues():

&#x20;   with STORAGE\_LOCK: return \_inspect\_timetable\_entry\_issues\_from\_data(\_read\_timetable\_file())



def initialize\_storage():

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           clear\_storage\_cache()

&#x20;           SHUTDOWN\_EVENT.clear()

&#x20;           timetable = \_get\_timetable\_ref()

&#x20;           recycle\_bin = \_get\_recycle\_bin\_ref()

&#x20;           log\_info(LOGGER, "Storage initialization completed.", what="Initialized timetable and recycle-bin caches.", where=\_where("initialize\_storage"), why="The application startup sequence requested a clean synchronized storage state.", context={"timetable\_count": len(timetable), "recycle\_bin\_count": len(recycle\_bin)})

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("initialize\_storage", "Storage initialization failed.", exc)

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("initialize\_storage", "Storage initialization failed unexpectedly.")

&#x20;           raise



def clear\_storage\_cache():

&#x20;   global TIMETABLE\_CACHE, RECYCLE\_BIN\_CACHE, TIMETABLE\_ID\_INDEX, TIMETABLE\_SLOT\_INDEX, TIMETABLE\_PREFIX\_INDEX, RECYCLE\_INDEX\_MAP

&#x20;   TIMETABLE\_CACHE = None

&#x20;   RECYCLE\_BIN\_CACHE = None

&#x20;   TIMETABLE\_ID\_INDEX = None

&#x20;   TIMETABLE\_SLOT\_INDEX = None

&#x20;   TIMETABLE\_PREFIX\_INDEX = None

&#x20;   RECYCLE\_INDEX\_MAP = None



def get\_shutdown\_event(): return SHUTDOWN\_EVENT

def shutdown\_storage():

&#x20;   SHUTDOWN\_EVENT.set()

&#x20;   log\_info(LOGGER, "Storage shutdown signal set.", what="Marked the timetable storage layer for shutdown.", where=\_where("shutdown\_storage"), why="The application is exiting or stopping background work safely.")



def save\_timetable(data):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           validated = \_validate\_timetable\_entries(data)

&#x20;           \_atomic\_write\_json\_list\_file(TIMETABLE\_FILE, validated)

&#x20;           \_refresh\_timetable\_cache(validated)

&#x20;           log\_event(LOGGER, "Saved timetable data to disk.", what="Committed timetable changes.", where=\_where("save\_timetable"), why="Validated timetable data needed to be persisted.", context={"entry\_count": len(validated)})

&#x20;           return \_clone\_timetable(validated)

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("save\_timetable", "Saving timetable data failed.", exc, {"entry\_count": len(data) if isinstance(data, list) else None})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("save\_timetable", "Saving timetable data failed unexpectedly.")

&#x20;           raise



def save\_recycle\_bin(data):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           validated = \_validate\_recycle\_bin(data)

&#x20;           \_atomic\_write\_json\_list\_file(RECYCLE\_BIN\_FILE, validated)

&#x20;           \_refresh\_recycle\_bin\_cache(validated)

&#x20;           log\_event(LOGGER, "Saved recycle-bin data to disk.", what="Committed recycle-bin changes.", where=\_where("save\_recycle\_bin"), why="Validated recycle-bin data needed to be persisted.", context={"entry\_count": len(validated)})

&#x20;           return \_clone\_recycle\_bin(validated)

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("save\_recycle\_bin", "Saving recycle-bin data failed.", exc, {"entry\_count": len(data) if isinstance(data, list) else None})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("save\_recycle\_bin", "Saving recycle-bin data failed unexpectedly.")

&#x20;           raise



def delete\_raw\_timetable\_entry(index):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           raw = \_read\_timetable\_file()

&#x20;           if not isinstance(index, int): raise TimetableValidationError("index must be an integer.")

&#x20;           if index < 0 or index >= len(raw): raise TimetableNotFoundError(f"No raw timetable entry found at index {index}.")

&#x20;           deleted = copy.deepcopy(raw.pop(index))

&#x20;           \_atomic\_write\_json\_list\_file(TIMETABLE\_FILE, raw)

&#x20;           clear\_storage\_cache()

&#x20;           log\_event(LOGGER, "Deleted an invalid raw timetable entry.", what="Removed a malformed timetable row directly from storage.", where=\_where("delete\_raw\_timetable\_entry"), why="Startup repair removed a stored row that could not be kept safely.", context={"index": index, "entry": \_safe\_entry\_context(deleted)})

&#x20;           return deleted

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("delete\_raw\_timetable\_entry", "Deleting a raw timetable entry failed.", exc, {"index": index})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("delete\_raw\_timetable\_entry", "Deleting a raw timetable entry failed unexpectedly.", {"index": index})

&#x20;           raise



def repair\_timetable\_entry(index, entry\_id, day, start\_time, end\_time, class\_name, classroom\_url):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           raw = \_read\_timetable\_file()

&#x20;           if not isinstance(index, int): raise TimetableValidationError("index must be an integer.")

&#x20;           if index < 0 or index >= len(raw): raise TimetableNotFoundError(f"No raw timetable entry found at index {index}.")

&#x20;           repair\_entry\_id = \_find\_repair\_entry\_id(raw, index, entry\_id, day)

&#x20;           raw\[index] = build\_validated\_entry(repair\_entry\_id, day, start\_time, end\_time, class\_name, classroom\_url)

&#x20;           \_atomic\_write\_json\_list\_file(TIMETABLE\_FILE, raw)

&#x20;           clear\_storage\_cache()

&#x20;           log\_event(LOGGER, "Repaired an invalid timetable entry in storage.", what="Updated a malformed stored timetable row.", where=\_where("repair\_timetable\_entry"), why="Startup repair replaced invalid stored values with validated values.", context={"index": index, "entry": \_safe\_entry\_context(raw\[index])})

&#x20;           return copy.deepcopy(raw\[index])

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("repair\_timetable\_entry", "Repairing a raw timetable entry failed.", exc, {"index": index, "entry\_id": entry\_id})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("repair\_timetable\_entry", "Repairing a raw timetable entry failed unexpectedly.", {"index": index, "entry\_id": entry\_id})

&#x20;           raise



def add\_entry(day, start\_time, end\_time, class\_name, url):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           day = \_validate\_day(day)

&#x20;           start\_time, end\_time = validate\_time\_range(start\_time, end\_time)

&#x20;           class\_name = \_validate\_text(class\_name, "class\_name")

&#x20;           url = \_validate\_text(url, "classroom\_url")

&#x20;           timetable = \_get\_timetable\_ref()

&#x20;           \_, slot\_to\_indexes, prefix\_to\_numbers = \_get\_timetable\_indexes()

&#x20;           \_ensure\_slot\_available(slot\_to\_indexes, day, start\_time, end\_time)

&#x20;           prefix = day\[:3].lower()

&#x20;           new\_entry = {"id": f"{prefix}\_{\_get\_first\_available\_number(prefix\_to\_numbers.get(prefix, set()))}", "day": day, "start\_time": start\_time, "end\_time": end\_time, "class\_name": class\_name, "classroom\_url": url, "joined": False}

&#x20;           timetable.append(new\_entry)

&#x20;           validated = \_validate\_timetable\_entries(timetable)

&#x20;           \_atomic\_write\_json\_list\_file(TIMETABLE\_FILE, validated)

&#x20;           \_refresh\_timetable\_cache(validated)

&#x20;           log\_event(LOGGER, "Added a timetable entry.", what="Created a new timetable slot.", where=\_where("add\_entry"), why="A new class entry was validated and saved.", context={"entry": \_safe\_entry\_context(new\_entry)})

&#x20;           return \_clone\_entry(new\_entry)

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("add\_entry", "Adding a timetable entry failed.", exc, {"day": day, "start\_time": start\_time, "end\_time": end\_time, "class\_name": class\_name})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("add\_entry", "Adding a timetable entry failed unexpectedly.", {"day": day, "start\_time": start\_time, "end\_time": end\_time, "class\_name": class\_name})

&#x20;           raise



def get\_entry\_by\_id(entry\_id):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           entry\_id = \_validate\_entry\_id(entry\_id)

&#x20;           timetable = \_get\_timetable\_ref()

&#x20;           id\_to\_index, \_, \_ = \_get\_timetable\_indexes()

&#x20;           if entry\_id not in id\_to\_index: raise TimetableNotFoundError(f"No entry found with id '{entry\_id}'.")

&#x20;           return \_clone\_entry(timetable\[id\_to\_index\[entry\_id]])

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("get\_entry\_by\_id", "Entry lookup failed.", exc, {"entry\_id": entry\_id})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("get\_entry\_by\_id", "Entry lookup failed unexpectedly.", {"entry\_id": entry\_id})

&#x20;           raise



def get\_recycle\_record\_by\_id(recycle\_id):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           recycle\_id = \_validate\_recycle\_id(recycle\_id)

&#x20;           recycle\_bin = \_get\_recycle\_bin\_ref()

&#x20;           recycle\_index\_map = \_get\_recycle\_index\_map()

&#x20;           if recycle\_id not in recycle\_index\_map: raise TimetableNotFoundError(f"No recycle bin entry found with recycle\_id '{recycle\_id}'.")

&#x20;           return \_clone\_recycle\_record(recycle\_bin\[recycle\_index\_map\[recycle\_id]])

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("get\_recycle\_record\_by\_id", "Recycle-bin lookup failed.", exc, {"recycle\_id": recycle\_id})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("get\_recycle\_record\_by\_id", "Recycle-bin lookup failed unexpectedly.", {"recycle\_id": recycle\_id})

&#x20;           raise



def edit\_entry(entry\_id, \*\*kwargs):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           entry\_id = \_validate\_entry\_id(entry\_id)

&#x20;           updates = \_normalize\_update\_fields(kwargs)

&#x20;           if not updates: raise TimetableValidationError("Provide at least one field to update.")

&#x20;           timetable = \_get\_timetable\_ref()

&#x20;           id\_to\_index, slot\_to\_indexes, \_ = \_get\_timetable\_indexes()

&#x20;           if entry\_id not in id\_to\_index: raise TimetableNotFoundError(f"No entry found with id '{entry\_id}'.")

&#x20;           current = timetable\[id\_to\_index\[entry\_id]]

&#x20;           updated = \_validate\_entry({"id": current\["id"], "day": updates.get("day", current\["day"]), "start\_time": updates.get("start\_time", current\["start\_time"]), "end\_time": updates.get("end\_time", current\["end\_time"]), "class\_name": updates.get("class\_name", current\["class\_name"]), "classroom\_url": updates.get("classroom\_url", current\["classroom\_url"]), "joined": current\["joined"]})

&#x20;           \_ensure\_slot\_available(slot\_to\_indexes, updated\["day"], updated\["start\_time"], updated\["end\_time"], ignore\_entry\_id=entry\_id, id\_to\_index=id\_to\_index)

&#x20;           timetable\[id\_to\_index\[entry\_id]] = updated

&#x20;           validated = \_validate\_timetable\_entries(timetable)

&#x20;           \_atomic\_write\_json\_list\_file(TIMETABLE\_FILE, validated)

&#x20;           \_refresh\_timetable\_cache(validated)

&#x20;           log\_event(LOGGER, "Edited a timetable entry.", what="Updated an existing timetable slot.", where=\_where("edit\_entry"), why="User or UI changes were validated and saved.", context={"entry\_id": entry\_id, "updated\_fields": sorted(updates), "entry": \_safe\_entry\_context(updated)})

&#x20;           return \_clone\_entry(updated)

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("edit\_entry", "Editing a timetable entry failed.", exc, {"entry\_id": entry\_id, "updated\_fields": sorted(kwargs)})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("edit\_entry", "Editing a timetable entry failed unexpectedly.", {"entry\_id": entry\_id, "updated\_fields": sorted(kwargs)})

&#x20;           raise



def set\_entry\_joined\_status(entry\_id, joined):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           entry\_id = \_validate\_entry\_id(entry\_id)

&#x20;           joined = \_validate\_joined\_flag(joined)

&#x20;           timetable = \_get\_timetable\_ref()

&#x20;           id\_to\_index, \_, \_ = \_get\_timetable\_indexes()

&#x20;           if entry\_id not in id\_to\_index: raise TimetableNotFoundError(f"No entry found with id '{entry\_id}'.")

&#x20;           updated = \_clone\_entry(timetable\[id\_to\_index\[entry\_id]])

&#x20;           updated\["joined"] = joined

&#x20;           timetable\[id\_to\_index\[entry\_id]] = updated

&#x20;           validated = \_validate\_timetable\_entries(timetable)

&#x20;           \_atomic\_write\_json\_list\_file(TIMETABLE\_FILE, validated)

&#x20;           \_refresh\_timetable\_cache(validated)

&#x20;           log\_event(LOGGER, "Updated joined status for a timetable entry.", what="Marked a class as joined or not joined.", where=\_where("set\_entry\_joined\_status"), why="Background automation or future scheduler state changed.", context={"entry\_id": entry\_id, "joined": joined})

&#x20;           return \_clone\_entry(updated)

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("set\_entry\_joined\_status", "Updating joined status failed.", exc, {"entry\_id": entry\_id, "joined": joined})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("set\_entry\_joined\_status", "Updating joined status failed unexpectedly.", {"entry\_id": entry\_id, "joined": joined})

&#x20;           raise



def delete\_entry(entry\_id, allow\_duplicate\_slots=False):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           entry\_id = \_validate\_entry\_id(entry\_id)

&#x20;           timetable = \_get\_timetable\_ref(allow\_duplicate\_slots=allow\_duplicate\_slots)

&#x20;           recycle\_bin = \_get\_recycle\_bin\_ref()

&#x20;           id\_to\_index, \_, \_ = \_get\_timetable\_indexes(allow\_duplicate\_slots=allow\_duplicate\_slots)

&#x20;           if entry\_id not in id\_to\_index: raise TimetableNotFoundError(f"No entry found with id '{entry\_id}'.")

&#x20;           deleted = \_clone\_entry(timetable.pop(id\_to\_index\[entry\_id]))

&#x20;           next\_bin = max(\[int(r\["recycle\_id"].split("\_")\[1]) for r in recycle\_bin], default=0) + 1

&#x20;           record = {"recycle\_id": f"bin\_{next\_bin}", "deleted\_at": datetime.now().isoformat(timespec="seconds"), "entry": deleted}

&#x20;           recycle\_bin.append(record)

&#x20;           validated\_timetable = \_validate\_timetable\_entries(timetable, allow\_duplicate\_slots=allow\_duplicate\_slots)

&#x20;           validated\_recycle = \_validate\_recycle\_bin(recycle\_bin)

&#x20;           \_atomic\_write\_json\_list\_file(TIMETABLE\_FILE, validated\_timetable)

&#x20;           \_atomic\_write\_json\_list\_file(RECYCLE\_BIN\_FILE, validated\_recycle)

&#x20;           \_refresh\_timetable\_cache(validated\_timetable)

&#x20;           \_refresh\_recycle\_bin\_cache(validated\_recycle)

&#x20;           log\_event(LOGGER, "Moved a timetable entry to the recycle bin.", what="Soft-deleted a timetable entry.", where=\_where("delete\_entry"), why="The entry was removed from the active timetable but kept for recovery.", context={"entry\_id": entry\_id, "recycle\_record": \_safe\_record\_context(record)})

&#x20;           return \_clone\_recycle\_record(record)

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("delete\_entry", "Deleting a timetable entry failed.", exc, {"entry\_id": entry\_id, "allow\_duplicate\_slots": allow\_duplicate\_slots})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("delete\_entry", "Deleting a timetable entry failed unexpectedly.", {"entry\_id": entry\_id, "allow\_duplicate\_slots": allow\_duplicate\_slots})

&#x20;           raise



def restore\_entry(recycle\_id, allow\_duplicate\_slots=False):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           recycle\_id = \_validate\_recycle\_id(recycle\_id)

&#x20;           timetable = \_get\_timetable\_ref(allow\_duplicate\_slots=allow\_duplicate\_slots)

&#x20;           recycle\_bin = \_get\_recycle\_bin\_ref()

&#x20;           recycle\_index\_map = \_get\_recycle\_index\_map()

&#x20;           if recycle\_id not in recycle\_index\_map: raise TimetableNotFoundError(f"No recycle bin entry found with recycle\_id '{recycle\_id}'.")

&#x20;           entry = \_clone\_entry(recycle\_bin\[recycle\_index\_map\[recycle\_id]]\["entry"])

&#x20;           id\_to\_index, slot\_to\_indexes, \_ = \_get\_timetable\_indexes(allow\_duplicate\_slots=allow\_duplicate\_slots)

&#x20;           if entry\["id"] in id\_to\_index: raise TimetableConflictError(f"Cannot restore entry because id '{entry\['id']}' already exists in the timetable.")

&#x20;           \_ensure\_slot\_available(slot\_to\_indexes, entry\["day"], entry\["start\_time"], entry\["end\_time"])

&#x20;           timetable.append(entry)

&#x20;           recycle\_bin.pop(recycle\_index\_map\[recycle\_id])

&#x20;           validated\_timetable = \_validate\_timetable\_entries(timetable, allow\_duplicate\_slots=allow\_duplicate\_slots)

&#x20;           validated\_recycle = \_validate\_recycle\_bin(recycle\_bin)

&#x20;           \_atomic\_write\_json\_list\_file(TIMETABLE\_FILE, validated\_timetable)

&#x20;           \_atomic\_write\_json\_list\_file(RECYCLE\_BIN\_FILE, validated\_recycle)

&#x20;           \_refresh\_timetable\_cache(validated\_timetable)

&#x20;           \_refresh\_recycle\_bin\_cache(validated\_recycle)

&#x20;           log\_event(LOGGER, "Restored a recycle-bin entry into the timetable.", what="Recovered a previously deleted timetable entry.", where=\_where("restore\_entry"), why="The recycle-bin record passed validation and no conflicts blocked restoration.", context={"recycle\_id": recycle\_id, "entry": \_safe\_entry\_context(entry)})

&#x20;           return \_clone\_entry(entry)

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("restore\_entry", "Restoring a recycle-bin entry failed.", exc, {"recycle\_id": recycle\_id, "allow\_duplicate\_slots": allow\_duplicate\_slots})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("restore\_entry", "Restoring a recycle-bin entry failed unexpectedly.", {"recycle\_id": recycle\_id, "allow\_duplicate\_slots": allow\_duplicate\_slots})

&#x20;           raise



def permanently\_delete\_recycle\_entry(recycle\_id):

&#x20;   with STORAGE\_LOCK:

&#x20;       try:

&#x20;           recycle\_id = \_validate\_recycle\_id(recycle\_id)

&#x20;           recycle\_bin = \_get\_recycle\_bin\_ref()

&#x20;           recycle\_index\_map = \_get\_recycle\_index\_map()

&#x20;           if recycle\_id not in recycle\_index\_map: raise TimetableNotFoundError(f"No recycle bin entry found with recycle\_id '{recycle\_id}'.")

&#x20;           deleted = \_clone\_recycle\_record(recycle\_bin.pop(recycle\_index\_map\[recycle\_id]))

&#x20;           validated\_recycle = \_validate\_recycle\_bin(recycle\_bin)

&#x20;           \_atomic\_write\_json\_list\_file(RECYCLE\_BIN\_FILE, validated\_recycle)

&#x20;           \_refresh\_recycle\_bin\_cache(validated\_recycle)

&#x20;           log\_event(LOGGER, "Permanently deleted a recycle-bin record.", what="Removed a recycle-bin entry permanently.", where=\_where("permanently\_delete\_recycle\_entry"), why="The recycle-bin record was intentionally removed and cannot be restored now.", context={"recycle\_record": \_safe\_record\_context(deleted)})

&#x20;           return deleted

&#x20;       except TimetableError as exc:

&#x20;           \_log\_known\_issue("permanently\_delete\_recycle\_entry", "Permanent recycle-bin deletion failed.", exc, {"recycle\_id": recycle\_id})

&#x20;           raise

&#x20;       except Exception:

&#x20;           \_log\_unexpected\_issue("permanently\_delete\_recycle\_entry", "Permanent recycle-bin deletion failed unexpectedly.", {"recycle\_id": recycle\_id})

&#x20;           raise



)





