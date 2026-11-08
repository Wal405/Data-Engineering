
import json
from pathlib import Path
from collections import Counter, defaultdict

root = Path("data/device_sync")

# Store counts for each firmware/app combination.
groups = defaultdict(Counter)
drop_counts = defaultdict(Counter)

for path in sorted(root.glob("drop_*/*.json")):
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    firmware = data.get("firmware", "MISSING")
    app_version = data.get("app_version", "MISSING")
    drop = path.parent.name

    key = (firmware, app_version)

    for sample in data.get("samples", []):
        if not isinstance(sample, dict):
            continue

        has_hr = sample.get("hr") is not None

        if has_hr:
            groups[key]["hr_present"] += 1
            drop_counts[drop]["hr_present"] += 1
        else:
            groups[key]["hr_missing"] += 1
            drop_counts[drop]["hr_missing"] += 1

        groups[key]["total"] += 1
        drop_counts[drop]["total"] += 1

print("========== MISSING HR BY FIRMWARE AND APP ==========")

for key, counts in sorted(groups.items()):
    total = counts["total"]
    missing = counts["hr_missing"]
    percentage = missing / total * 100 if total else 0

    print(
        f"Firmware={key[0]}, App={key[1]} | "
        f"Total={total}, Missing HR={missing}, "
        f"Missing %={percentage:.2f}"
    )

print("\n========== MISSING HR BY DROP ==========")

for drop, counts in sorted(drop_counts.items()):
    total = counts["total"]
    missing = counts["hr_missing"]
    percentage = missing / total * 100 if total else 0

    print(
        f"{drop} | Total={total}, Missing HR={missing}, "
        f"Missing %={percentage:.2f}"
    )
