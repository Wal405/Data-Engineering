
import json
from pathlib import Path
from collections import Counter

data_folder = Path("data/device_sync")

total_files = 0
total_samples = 0
sample_count_mismatches = []
missing_metadata = Counter()
metadata_types = {}
firmware_counts = Counter()
app_version_counts = Counter()
json_errors = []

required_metadata = [
    "batch_id",
    "device_id",
    "firmware",
    "app_version",
    "uploaded_at",
    "sample_count",
    "samples",
]

for file_path in data_folder.glob("drop_*/*.json"):
    total_files += 1

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except (json.JSONDecodeError, OSError) as error:
        json_errors.append((str(file_path), str(error)))
        continue

    # Check required metadata fields
    for field in required_metadata:
        if field not in data or data[field] is None:
            missing_metadata[field] += 1

    # Count firmware and app versions
    firmware_counts[data.get("firmware")] += 1
    app_version_counts[data.get("app_version")] += 1

    # Check metadata value types
    for field in required_metadata:
        if field in data:
            value_type = type(data[field]).__name__
            if field not in metadata_types:
                metadata_types[field] = Counter()
            metadata_types[field][value_type] += 1

    # Check sample count
    samples = data.get("samples")

    if not isinstance(samples, list):
        sample_count_mismatches.append(
            (str(file_path), "samples is not a list")
        )
        continue

    actual_count = len(samples)
    total_samples += actual_count
    declared_count = data.get("sample_count")

    if declared_count != actual_count:
        sample_count_mismatches.append(
            (str(file_path), declared_count, actual_count)
        )

print("========== METADATA AUDIT ==========")
print("Files inspected:", total_files)
print("Total samples:", total_samples)
print("Missing metadata:", dict(missing_metadata))
print("Firmware versions:", dict(firmware_counts))
print("App versions:", dict(app_version_counts))

print("\nMetadata types:")
for field, counts in metadata_types.items():
    print(field, dict(counts))

print("\nSample-count mismatches:", len(sample_count_mismatches))
for issue in sample_count_mismatches[:10]:
    print(issue)

print("\nUnreadable JSON files:", len(json_errors))
for issue in json_errors[:10]:
    print(issue)
