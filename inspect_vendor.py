
import json

path = "data/vendor_api/sessions_page_1.json"

with open(path, "r", encoding="utf-8") as file:
    data = json.load(file)

session = data["sessions"][0]

print("Session fields:")
for key, value in session.items():
    print(f"{key}: {value}")
