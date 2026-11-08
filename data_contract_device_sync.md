# StrideBand Device-Sync Data Contract (v1.0.0)

## 1. Specification Overview
This data contract defines the structural, semantic, and transmission expectations for batch payloads uploaded by the StrideBand companion app to the central analytics warehouse.

- **Publisher**: StrideBand Mobile Engineering & Companion App Team
- **Consumer**: Analytics & Sports Science Data Engineering
- **Version**: `1.0.0`
- **Protocol**: JSON payload over HTTPS

---

## 2. Payload Schema

### Top-Level Batch Object
| Field | Type | Required | Description | Constraints |
|---|---|---|---|---|
| `batch_id` | `String` | Yes | Globally unique UUID for the upload batch. | Non-empty, unique |
| `device_id` | `String` | Yes | Unique hardware identifier for the StrideBand watch. | Pattern: `^D[0-9]{3,}$` |
| `firmware` | `String` | Yes | Firmware version running on the watch. | Semantic Version (e.g., `2.4.0`) |
| `app_version`| `String` | Yes | Companion app version running on phone. | Semantic Version (e.g., `5.2.0`) |
| `uploaded_at` | `String (ISO 8601)` | Yes | UTC timestamp when upload completed. | Format: `YYYY-MM-DDTHH:MM:SSZ` |
| `sample_count`| `Integer` | Yes | Declared count of samples in `samples` array. | Must **strictly equal** `len(samples)` |
| `samples` | `Array[Object]` | Yes | Time-series stream of sensor observations. | Min items: 1 |

### Sample Object (`samples[]`)
| Field | Type | Required | Description | Constraints / Units |
|---|---|---|---|---|
| `t` | `String (ISO 8601)` | Yes | Observation timestamp in athlete local time. | Subsecond precision allowed (`.500`) |
| `hr` | `Integer` or `null` | Yes | Heart rate in beats per minute (BPM). | `30 <= hr <= 250` or `null` (auto-pause/dropout) |
| `cadence` | `Integer` | Yes | Running cadence in steps per minute. | **Numeric Integer** (never string); `0 <= cadence <= 260` |
| `gps` | `Object` | Yes | Location metadata object. | Must contain `lat`, `lon`, `hdop` |
| `gps.lat` | `Float` | Yes | WGS84 Latitude. | `-90.0 <= lat <= 90.0` |
| `gps.lon` | `Float` | Yes | WGS84 Longitude. | `-180.0 <= lon <= 180.0` |
| `gps.hdop` | `Float` | Yes | Horizontal Dilution of Precision. | `0.0 <= hdop <= 50.0` |

---

## 3. Strict Rules & Breaking Change Policy

1. **Key Renaming Forbidden**:
   - The heart rate field must remain named `hr`. Renaming to `heart_rate` in minor releases without deprecation notice is a **contract violation**.
2. **Type Stability**:
   - `cadence` must always be sent as an integer (e.g. `116`), never as a string (`"116"`).
3. **Array Count Consistency**:
   - Payloads where `sample_count != len(samples)` will be rejected at the gateway.

---

## 4. What This Contract PREVENTS

1. **Schema Renaming Breaks (e.g., `hr` vs. `heart_rate`)**:
   - Automated contract testing in CI/CD will block any mobile release that renames keys without a major version change.
2. **Type Incompatibilities (e.g., Cadence String vs Integer)**:
   - Enforcing strict integer types on `cadence` prevents downstream runtime type errors and arithmetic failures.
3. **Duplicate Batch Ingestion & Payload Corruption**:
   - Requiring a unique `batch_id` and strict verification that `sample_count == len(samples)` eliminates corrupted or truncated uploads.

---

## 5. What This Contract CANNOT Prevent

1. **Sensor Dropouts and Auto-Pause Gaps**:
   - A contract cannot force a wrist sensor to read blood flow when the athlete sweats, loosens the watch strap, or stops at a red light. The sensor legitimately records `hr: null` during these intervals.
2. **Hardware Reassignment (e.g., Hamza swapping watches with Mehak)**:
   - The payload only knows `device_id: "D013"`. It cannot know who is physically wearing the device. That must be resolved through programme administration and slowly changing roster dimensions.
