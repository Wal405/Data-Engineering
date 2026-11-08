
import json
from pathlib import Path

file_path = Path("data/device_sync/drop_1/B0092074e-5.json")

with file_path.open("r", encoding="utf-8") as file:
    data = json.load(file)

print("Data type:", type(data).__name__)

if isinstance(data, dict):
    print("Top-level keys:", list(data.keys()))

    for key, value in data.items():
        if isinstance(value, list):
            print(f"\n{key}: {len(value)} records")

            for record in value[:3]:
                print(record)

        elif isinstance(value, dict):
            print(f"\n{key}: dictionary with keys {list(value.keys())}")

        else:
            print(f"{key}: {value}")
elif isinstance(data, list):
    print("Total records:", len(data))
    print("First 3 records:")
    for record in data[:3]:
        print(record)
