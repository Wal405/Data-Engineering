
import json
from pathlib import Path
from collections import Counter, defaultdict

root = Path("data/device_sync")

pattern_counts = Counter()
files_by_pattern = defaultdict(list)
errors = []

for path in sorted(root.glob("drop_*/*.json")):
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        samples = data.get("samples", [])

        keys = set()

        for sample in samples:
            if isinstance(sample, dict):
                keys.update(sample.keys())

        has_hr = "hr" in keys
        has_heart_rate = "heart_rate" in keys
        has_flat_gps = "lat" in keys or "lon" in keys
        has_nested_gps = "gps" in keys

        if has_hr and not has_heart_rate:
            hr_pattern = "hr"
        elif has_heart_rate and not has_hr:
            hr_pattern = "heart_rate"
        elif has_hr and has_heart_rate:
            hr_pattern = "both"
        else:
            hr_pattern = "neither"

        if has_flat_gps and has_nested_gps:
            gps_pattern = "flat_and_nested"
        elif has_flat_gps:
            gps_pattern = "flat"
        elif has_nested_gps:
            gps_pattern = "nested"
        else:
            gps_pattern = "no_gps_keys"

        pattern = (hr_pattern, gps_pattern)
        pattern_counts[pattern] += 1
        files_by_pattern[pattern].append(str(path))

    except (OSError, json.JSONDecodeError) as error:
        errors.append((str(path), str(error)))

print("========== SCHEMA PATTERN SUMMARY ==========")

for pattern, count in pattern_counts.most_common():
    print(
        f"\nHeart-rate format: {pattern[0]}"
        f" | GPS format: {pattern[1]}"
        f" | Files: {count}"
    )

    for path in files_by_pattern[pattern][:5]:
        print("  Example:", path)

    if count > 5:
        print(f"  ... and {count - 5} more files")

print("\n========== FILE ERRORS ==========")

if errors:
    for path, error in errors:
        print(path, "->", error)
else:
    print("No unreadable JSON files.")

print("\nRaw files were not modified.")
