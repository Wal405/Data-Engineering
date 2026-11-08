
import json
from pathlib import Path
from collections import defaultdict

root = Path("data/vendor_api")

# Keep each session and the file where it was found.
sessions_by_id = defaultdict(list)

for path in sorted(root.glob("*.json")):
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    for session in data.get("sessions", []):
        session_id = session.get("session_id")

        if session_id is not None:
            sessions_by_id[session_id].append(
                {
                    "file": path.name,
                    "record": session,
                }
            )

duplicate_count = 0
identical_count = 0
conflicting_count = 0

print("========== DUPLICATE SESSION INVESTIGATION ==========")

for session_id, entries in sessions_by_id.items():
    if len(entries) <= 1:
        continue

    duplicate_count += 1

    first_record = entries[0]["record"]
    all_identical = True

    for entry in entries[1:]:
        if entry["record"] != first_record:
            all_identical = False
            break

    if all_identical:
        identical_count += 1
    else:
        conflicting_count += 1

    print(f"\nSession ID: {session_id}")
    print("Files:", [entry["file"] for entry in entries])
    print("Records identical:", all_identical)

    if not all_identical:
        for entry in entries:
            print(entry["file"], entry["record"])

print("\n========== SUMMARY ==========")
print("Repeated session IDs:", duplicate_count)
print("Groups with identical records:", identical_count)
print("Groups with conflicting records:", conflicting_count)
