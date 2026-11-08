
import csv
import json
from pathlib import Path
from collections import Counter

DATA = Path("data")


# --------------------------------------------------
# 1. Profile device-sync JSON files
# --------------------------------------------------
def profile_device_sync():
    root = DATA / "device_sync"

    file_count = 0
    total_samples = 0
    bad_files = []
    count_mismatches = []
    missing_metadata = Counter()
    missing_sample_fields = Counter()
    firmware_counts = Counter()
    app_counts = Counter()
    drop_counts = Counter()
    sample_types = {
        "hr": Counter(),
        "cadence": Counter(),
        "lat": Counter(),
        "lon": Counter(),
        "t": Counter(),
    }

    required_metadata = [
        "batch_id",
        "device_id",
        "firmware",
        "app_version",
        "uploaded_at",
        "sample_count",
        "samples",
    ]

    required_sample_fields = ["t", "hr", "cadence"]

    for path in sorted(root.glob("drop_*/*.json")):
        file_count += 1
        drop_counts[path.parent.name] += 1

        try:
            with path.open("r", encoding="utf-8") as file:
                data = json.load(file)

            if not isinstance(data, dict):
                bad_files.append((str(path), "Top-level JSON is not an object"))
                continue

            for field in required_metadata:
                if field not in data or data[field] is None:
                    missing_metadata[field] += 1

            samples = data.get("samples")

            if not isinstance(samples, list):
                bad_files.append((str(path), "'samples' is missing or is not a list"))
                continue

            actual_count = len(samples)
            total_samples += actual_count

            if data.get("sample_count") != actual_count:
                count_mismatches.append(
                    (
                        str(path),
                        data.get("sample_count"),
                        actual_count,
                    )
                )

            firmware_counts[str(data.get("firmware", "MISSING"))] += 1
            app_counts[str(data.get("app_version", "MISSING"))] += 1

            for sample in samples:
                if not isinstance(sample, dict):
                    missing_sample_fields["INVALID_SAMPLE_OBJECT"] += 1
                    continue

                for field in required_sample_fields:
                    if field not in sample or sample[field] is None:
                        missing_sample_fields[field] += 1

                for field in sample_types:
                    if field in sample and sample[field] is not None:
                        sample_types[field][type(sample[field]).__name__] += 1

        except (json.JSONDecodeError, OSError, TypeError) as error:
            bad_files.append((str(path), str(error)))

    print("\n========== DEVICE SYNC ==========")
    print("Files inspected:", file_count)
    print("Files by drop:", dict(drop_counts))
    print("Total samples:", total_samples)
    print("Firmware versions:", dict(firmware_counts))
    print("App versions:", dict(app_counts))
    print("Files with missing metadata fields:", dict(missing_metadata))
    print("Missing sample fields:", dict(missing_sample_fields))

    print("\nSample data types:")
    for field, counts in sample_types.items():
        print(f"{field}: {dict(counts)}")

    print("\nSample-count mismatches:", len(count_mismatches))
    for mismatch in count_mismatches[:10]:
        print("Mismatch (file, declared, actual):", mismatch)

    print("\nUnreadable or invalid files:", len(bad_files))
    for problem in bad_files[:10]:
        print("Problem:", problem)


# --------------------------------------------------
# 2. Profile vendor API JSON files
# --------------------------------------------------
def profile_vendor_api():
    root = DATA / "vendor_api"

    file_count = 0
    total_sessions = 0
    session_ids = []
    bad_files = []
    page_numbers = []
    session_fields = Counter()
    session_field_types = {}
    missing_session_fields = Counter()

    required_fields = [
        "session_id",
        "device_id",
        "sport",
        "start_time",
        "end_time",
        "duration_s",
    ]

    for path in sorted(root.glob("*.json")):
        file_count += 1

        try:
            with path.open("r", encoding="utf-8") as file:
                data = json.load(file)

            if not isinstance(data, dict):
                bad_files.append((str(path), "Top-level JSON is not an object"))
                continue

            page_numbers.append(data.get("page"))

            sessions = data.get("sessions")

            if not isinstance(sessions, list):
                bad_files.append((str(path), "'sessions' is missing or is not a list"))
                continue

            total_sessions += len(sessions)

            for session in sessions:
                if not isinstance(session, dict):
                    bad_files.append((str(path), "Invalid session record"))
                    continue

                session_ids.append(session.get("session_id"))

                for field in required_fields:
                    if field not in session or session[field] is None:
                        missing_session_fields[field] += 1

                for field, value in session.items():
                    session_fields[field] += 1

                    if value is not None:
                        if field not in session_field_types:
                            session_field_types[field] = Counter()

                        session_field_types[field][type(value).__name__] += 1

        except (json.JSONDecodeError, OSError, TypeError) as error:
            bad_files.append((str(path), str(error)))

    id_counts = Counter(session_ids)
    duplicate_ids = {}

    for session_id, count in id_counts.items():
        if session_id is not None and count > 1:
            duplicate_ids[session_id] = count

    print("\n========== VENDOR API ==========")
    print("Files inspected:", file_count)
    print("Page numbers:", page_numbers)
    print("Total session records:", total_sessions)
    print("Unique non-null session IDs:", len(set(
        session_id for session_id in session_ids
        if session_id is not None
    )))
    print("Duplicate session IDs:", duplicate_ids)
    print("Missing session fields:", dict(missing_session_fields))

    print("\nSession field occurrence counts:")
    print(dict(session_fields))

    print("\nSession field data types:")
    for field, counts in session_field_types.items():
        print(f"{field}: {dict(counts)}")

    print("\nUnreadable or invalid files:", len(bad_files))
    for problem in bad_files[:10]:
        print("Problem:", problem)


# --------------------------------------------------
# 3. Profile CSV files
# --------------------------------------------------
def profile_csv(path):
    print(f"\n========== CSV: {path} ==========")

    if not path.exists():
        print("File not found")
        return

    try:
        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:
            reader = csv.DictReader(file)

            print("Columns:", reader.fieldnames)

            row_count = 0
            missing_values = Counter()
            column_types = {}

            for row in reader:
                row_count += 1

                for column, value in row.items():
                    if value is None or value.strip() == "":
                        missing_values[column] += 1
                    else:
                        if column not in column_types:
                            column_types[column] = Counter()

                        column_types[column][type(value).__name__] += 1

                if row_count <= 3:
                    print("Example row:", row)

            print("Total data rows:", row_count)
            print("Blank or missing values:", dict(missing_values))
            print("Read-time value types:", {
                column: dict(counts)
                for column, counts in column_types.items()
            })

    except (OSError, csv.Error, UnicodeDecodeError) as error:
        print("Could not read CSV:", error)


# --------------------------------------------------
# 4. Inspect firmware release notes
# --------------------------------------------------
def profile_firmware_notes():
    path = DATA / "firmware" / "release_notes.md"

    print("\n========== FIRMWARE RELEASE NOTES ==========")

    if not path.exists():
        print("File not found:", path)
        return

    try:
        text = path.read_text(encoding="utf-8-sig")
        print(text)
    except (OSError, UnicodeDecodeError) as error:
        print("Could not read firmware notes:", error)


# --------------------------------------------------
# 5. Run the profiling functions
# --------------------------------------------------
if __name__ == "__main__":
    print("STRIDE PERFORMANCE LAB")
    print("RAW DATA INSPECTION AND PROFILING")
    print("Raw files will not be modified.")

    if not DATA.exists():
        raise FileNotFoundError(
            "The data directory was not found. "
            "Run this script from the project root."
        )

    profile_device_sync()
    profile_vendor_api()

    profile_csv(DATA / "roster" / "roster_2026-07-01.csv")
    profile_csv(DATA / "roster" / "roster_2026-09-01.csv")
    profile_csv(DATA / "coach_app" / "session_labels.csv")

    profile_firmware_notes()

    print("\n========== PROFILING FINISHED ==========")
