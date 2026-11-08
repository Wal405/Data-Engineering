
from pathlib import Path

root = Path("data")

folders = [
    "device_sync/drop_1",
    "device_sync/drop_2",
    "device_sync/drop_3",
    "vendor_api",
    "roster",
    "coach_app",
]

for folder in folders:
    folder_path = root / folder

    file_count = 0

    for path in folder_path.iterdir():
        if path.is_file():
            file_count = file_count + 1

    print(folder, ":", file_count, "files")
