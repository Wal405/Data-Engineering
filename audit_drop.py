
import json
from pathlib import Path
from collections import Counter, defaultdict

BASE_DIR = Path("data")
DROP_NAMES = ["drop_1", "drop_2", "drop_3"]


def get_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__


def walk_json(value, path, stats, file_fields):
    """Collect leaf fields, their types, and example values."""

    if isinstance(value, dict):
        if not value:
            stats[path or "(root)"]["types"]["empty_object"] += 1
            file_fields.add(path or "(root)")
            return

        for key, item in value.items():
            if path:
                field_path = path + "." + key
            else:
                field_path = key

            walk_json(item, field_path, stats, file_fields)

    elif isinstance(value, list):
        if not value:
            stats[path or "(root)"]["types"]["empty_array"] += 1
            file_fields.add(path or "(root)")
            return

        for item in value:
            if isinstance(item, (dict, list)):
                walk_json(item, path + "[]", stats, file_fields)
            else:
                field_path = path + "[]"
                record_value(item, field_path, stats, file_fields)

    else:
        record_value(value, path or "(root)", stats, file_fields)


def record_value(value, field_path, stats, file_fields):
    stats[field_path]["types"][get_type(value)] += 1
    stats[field_path]["occurrences"] += 1

    example = repr(value)
    if len(stats[field_path]["examples"]) < 5:
        if example not in stats[field_path]["examples"]:
            stats[field_path]["examples"].append(example)

    file_fields.add(field_path)


def main():
    all_drop_fields = {}
    all_schema_groups = {}
    all_errors = []

    for drop_name in DROP_NAMES:
        drop_path = BASE_DIR / "device_sync" / drop_name

        if not drop_path.exists():
            print(f"\nERROR: Folder not found: {drop_path}")
            continue

        json_files = sorted(drop_path.glob("*.json"))

        field_stats = defaultdict(
            lambda: {
                "types": Counter(),
                "occurrences": 0,
                "examples": []
            }
        )

        field_file_counts = Counter()
        schema_groups = defaultdict(list)
        file_count = 0

        print("\n" + "=" * 75)
        print(f"AUDITING {drop_name.upper()}")
        print("=" * 75)

        for file_path in json_files:
            try:
                with file_path.open("r", encoding="utf-8") as file:
                    data = json.load(file)
            except (json.JSONDecodeError, OSError) as error:
                all_errors.append((str(file_path), str(error)))
                continue

            file_count += 1
            file_fields = set()

            walk_json(data, "", field_stats, file_fields)

            for field in file_fields:
                field_file_counts[field] += 1

            signature = tuple(sorted(file_fields))
            schema_groups[signature].append(file_path.name)

        print(f"JSON files successfully checked: {file_count}")
        print(f"Different field paths found: {len(field_stats)}")

        print("\nFIELD DETAILS")
        print("-" * 75)

        for field in sorted(field_stats):
            details = field_stats[field]
            print(f"\nField: {field}")
            print(
                f"  Files containing field: "
                f"{field_file_counts[field]}/{file_count}"
            )
            print(f"  Values found: {details['occurrences']}")
            print(f"  Value types: {dict(details['types'])}")
            print(f"  Example values: {details['examples']}")

        print("\nFILE SCHEMA GROUPS")
        print("-" * 75)

        for signature, filenames in sorted(
            schema_groups.items(),
            key=lambda item: (-len(item[1]), item[1][0])
        ):
            print(f"\nFiles with this structure: {len(filenames)}")
            print("Fields:", ", ".join(signature))
            print("Filenames:", ", ".join(filenames))

        all_drop_fields[drop_name] = field_file_counts
        all_schema_groups[drop_name] = schema_groups

    print("\n" + "=" * 75)
    print("FIELD PRESENCE COMPARISON ACROSS DROPS")
    print("=" * 75)

    every_field = set()

    for counts in all_drop_fields.values():
        every_field.update(counts.keys())

    print(f"\n{'Field path':<35} {'drop_1':>10} {'drop_2':>10} {'drop_3':>10}")
    print("-" * 70)

    for field in sorted(every_field):
        values = []

        for drop_name in DROP_NAMES:
            counts = all_drop_fields.get(drop_name, Counter())
            values.append(str(counts.get(field, 0)))

        print(f"{field:<35} {values[0]:>10} {values[1]:>10} {values[2]:>10}")

    print("\n" + "=" * 75)
    print("JSON ERRORS")
    print("=" * 75)

    if all_errors:
        for filename, error in all_errors:
            print(f"{filename}: {error}")
    else:
        print("No JSON reading or parsing errors found.")


if __name__ == "__main__":
    main()
