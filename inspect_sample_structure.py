
import json

old_file = r"data\device_sync\drop_1\B0092074e-5.json"
new_file = r"data\device_sync\drop_2\B0fb67e04-4.json"

files = [old_file, new_file]

for file_path in files:
    print("\nFILE:", file_path)

    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    print("Top-level keys:", list(data.keys()))

    samples = data.get("samples", [])

    if len(samples) > 0:
        print("First sample:")
        print(json.dumps(samples[0], indent=4))
    else:
        print("No samples found.")
