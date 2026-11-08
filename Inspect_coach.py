
import csv

path = "data/coach_app/session_labels.csv"

with open(path, "r", encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    print("Columns:", reader.fieldnames)

    row_count = 0

    for row in reader:
        row_count += 1

        if row_count <= 5:
            print(row)

    print("\nTotal label records:", row_count)
