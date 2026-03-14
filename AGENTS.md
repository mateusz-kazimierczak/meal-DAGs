# Meal DAGs — CLAUDE.md

## Project Overview
Apache Airflow 3.0 DAG project that automates meal planning workflows: the daily meal matrix update, updating a Google Sheets report, and sending email/push notifications to users about their meals.

## Tech Stack
- **Orchestration**: Apache Airflow 3.0.6 (Python 3.11)
- **Package Manager**: uv (`pyproject.toml`)
- **Database Source**: MongoDB (via `airflow.providers.mongo`, connection ID: `mongoid`)
- **Spreadsheet**: Google Sheets API v4 (service account)
- **Email**: Resend + React Email (Node.js subprocess)
- **Push Notifications**: Expo FCM v1 (Node.js subprocess)
- **Error Tracking**: Rollbar

## DAGs

### `daily_meals_update`
- **File**: `dags/meals/daily_update/dag_daily_update.py`
- **Schedule**: Daily at 8:30 AM Toronto time
- **Tasks**: `fetch_data` → `process_users` → `[insert_bigquery, update_days]` → `save_users`
- **What it does**: Advances every user's meal matrix by one day, upserts today/tomorrow/next-week Day documents in MongoDB, and writes meal-change audit records to BigQuery. Email/push notifications are intentionally excluded — handled by `meal_notifications`.
- **Airflow Variables required**: `GCP_AUTH` (GCP service account JSON string)

### `meal_sheet_update_dag`
- **File**: `dags/meals/sheet_editor/dag_sheet_editing.py`
- **Schedule**: Daily at 8:35 AM EST
- **Tasks**: `extract_meal_data` → `update_google_sheet`
- **What it does**: Pulls today's actual meals + tomorrow's predicted meals from MongoDB, then writes a formatted summary table into the monthly Google Sheet tab

### `meal_notifications`
- **File**: `dags/meals/notifications/notifications.py`
- **Schedule**: 7:30 AM, 12:00 PM, 7:30 PM EST
- **Tasks**: `get_relevant_users` → `[send_email_notifications_task, send_mobile_notifications_task]`
- **What it does**: Determines which users need notifications (based on their meal schedule + preferences), then delegates to Node.js subprocesses for email and push delivery

## Key Files
- `dags/meals/daily_update/dag_daily_update.py` — Daily meal matrix update DAG
- `dags/meals/_common/mongo_utils/` — Reusable MongoDB query utilities
- `dags/meals/sheet_editor/sheet_utils/` — Google Sheets helpers (auth, sheet management, templates)
- `dags/meals/sheet_editor/sheet_utils/create_meal_diet_template/` — Template generation (data prep, formatting, stats)
- `dags/meals/notifications/get_relevant_users/` — User query + notification logic
- `dags/meals/notifications/send_emails/` — Node.js email sender (React Email + Resend)
- `dags/meals/notifications/mobile_notifications/` — Node.js push sender (expo-server-sdk)

## Required Infrastructure
| Resource | Details |
|---|---|
| Airflow connection | `mongoid` → MongoDB `test` database |
| Google service account | `/home/mateusz/secrets/sheets_sa.json` |
| Spreadsheet ID | `1QmqeeDAaM2Cyv_zpQB9xYfcLEpBjLmqXuODqpbDfQZw` |
| Node.js | Path: `/home/mateusz/.nvm/versions/node/v22.19.0/bin/node` |

## Airflow Variables

All config is stored in two JSON Variables — one per environment. Manage them at Admin → Variables in the Airflow UI.

**Variable names:** `config.dev` and `config.prod`

**Expected JSON structure:**
```json
{
    "MONGO_DB":       "test",
    "RESEND_API_KEY": "re_...",
    "GCP_AUTH":       { "type": "service_account", ... },
    "GCP_PROJECT":    "ec-meals-462913",
    "BQ_DATASET":     "meal_history",
    "BQ_TABLE":       "HISTORY",
    "SPREADSHEET_ID": "1QmqeeDAaM2Cyv_zpQB9xYfcLEpBjLmqXuODqpbDfQZw",
    "NODE_BIN":       "/home/mateusz/.nvm/versions/node/v22.19.0/bin/node"
}
```

**MongoDB Connections** (Admin → Connections):
| Connection ID | Points to |
|---|---|
| `mongoid_prod` | Production MongoDB |
| `mongoid_dev` | Dev MongoDB |

**Config helper:** `dags/meals/_common/config.py` — `get_config(env)` and `get_mongo_conn_id(env)`

## MongoDB Collections Used
| Collection | Purpose |
|---|---|
| `days` | Daily meal records: date (`D/M/YYYY`), meals, packedMeals, guests |
| `users` | User profiles: 7-day meal schedules, preferences, push tokens |
| `diets` | Available diet types |

## Architecture Notes
- **Inter-task communication**: Python tasks write `notifications.json`; Node.js bash operators read it — JSON file is the handoff between Python and Node.js
- **Date format**: MongoDB stores dates as `D/M/YYYY` strings — must match exactly
- **Timezone**: All scheduling uses `America/New_York` / `America/Toronto` (same zone)
- **Dual-language**: Python handles data orchestration; Node.js handles notification delivery (better SDK ecosystem)
- **Dry-run mode**: Pass `dry_run=True` DAG parameter to inspect data without modifying the sheet

## Local Development
```bash
# Install dependencies
uv sync

# Run Airflow locally
airflow standalone

# Test a specific DAG task
airflow tasks test meal_sheet_update_dag extract_meal_data 2025-11-01
```

## Notification Logic
- **Morning (7:30 AM)**: Notify about today's meals
- **Noon (12:00 PM)**: Notify about tomorrow's meals
- **Evening (7:30 PM)**: Notify about next-day packed meals
- Users must have `notifications.enabled = true` and a valid device token or email for delivery
