
import os

paths = [
    "data/README.md",
    "data/firmware/release_notes.md"
]

for path in paths:

    print(f"\n{'=' * 15} {path} {'=' * 15}")

    if not os.path.exists(path):
        print("File not found")
        continue

    with open(path, "r", encoding="utf-8") as file:
        content = file.read()

    print(content)
