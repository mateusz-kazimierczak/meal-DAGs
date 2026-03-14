# Meal DAGs

Apache Airflow workflow project that automates two recurring tasks for the EC Meals system:

1. **Sheet Update** — Pulls today's meal data from MongoDB and writes a formatted summary to Google Sheets
2. **Notifications** — Sends targeted meal reminders to users via email and push notifications

## What It Does

### Daily Sheet Update (8:35 AM)
Queries MongoDB for today's confirmed meals and tomorrow's predicted meals (based on user schedules), then writes a structured table into the monthly Google Sheet tab. Includes diet breakdowns, packed meal sections, and running averages.

### Meal Notifications (7:30 AM / 12:00 PM / 7:30 PM)
Checks which users are scheduled for meals at the relevant time, determines if they have alerts or reports due, and sends notifications via:
- Email (Resend + React Email templates)
- iOS/Android push (Expo push notifications via FCM v1)

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 3.0.6 |
| Language | Python 3.11 |
| Package manager | uv |
| Database | MongoDB (via Airflow provider) |
| Spreadsheet | Google Sheets API v4 |
| Email | Resend + React Email (Node.js) |
| Push notifications | expo-server-sdk (Node.js) |
| Error tracking | Rollbar |

## Prerequisites

- Python 3.11
- [uv](https://github.com/astral-sh/uv) package manager
- Node.js 22 (at `/home/mateusz/.nvm/versions/node/v22.19.0/bin/node`)
- Airflow with a MongoDB connection configured as `mongoid`
- Google service account JSON at `/home/mateusz/secrets/sheets_sa.json`
- `RESEND_API_KEY` environment variable set

## Getting Started

```bash
# Install Python dependencies
uv sync

# Initialize Airflow (first time only)
airflow db init
airflow users create --username admin --password admin --role Admin \
  --email admin@example.com --firstname Admin --lastname User

# Start Airflow
airflow standalone
```

Then visit `http://localhost:8080` and enable both DAGs.

## DAG Reference

### `meal_sheet_update_dag`

| Property | Value |
|---|---|
| Schedule | Daily at 8:35 AM EST |
| Start date | 2025-11-01 |
| Catchup | Disabled |

**Parameters:**
- `dry_run` (bool, default `false`) — If true, prints data to logs instead of writing to Google Sheets. Useful for testing.

**Tasks:**
1. `extract_meal_data` — Queries MongoDB for today's Day record and tomorrow's user meal matrices
2. `update_google_sheet` — Writes the formatted meal summary to the correct monthly sheet tab

### `meal_notifications`

| Property | Value |
|---|---|
| Schedule | 7:30 AM, 12:00 PM, 7:30 PM EST |
| Start date | 2025-01-01 |
| Catchup | Disabled |

**Notification timing logic:**

| Run time | Notifies about |
|---|---|
| 7:30 AM | Today's meals |
| 12:00 PM | Tomorrow's meals |
| 7:30 PM | Next day's packed meals |

**Tasks:**
1. `get_relevant_users` — Determines who needs a notification, writes `notifications.json`
2. `send_email_notifications_task` — Node.js subprocess reads JSON, sends via Resend
3. `send_mobile_notifications_task` — Node.js subprocess reads JSON, sends via Expo

## Project Structure

```
meal-DAGs/
├── dags/
│   └── meals/
│       ├── _common/
│       │   └── mongo_utils/          # Shared MongoDB query helpers
│       ├── sheet_editor/
│       │   ├── dag_sheet_editing.py  # Sheet update DAG definition
│       │   └── sheet_utils/          # Sheets API helpers + template builder
│       └── notifications/
│           ├── notifications.py      # Notifications DAG definition
│           ├── get_relevant_users/   # User query + notification logic
│           ├── send_emails/          # Node.js email sender
│           └── mobile_notifications/ # Node.js push notification sender
├── pyproject.toml                    # Python dependencies
└── .python-version                   # Python 3.11
```

## MongoDB Data Format

**`days` collection** (date stored as `D/M/YYYY`, no leading zeros):
```json
{
  "date": "13/3/2026",
  "meals": {"B": ["user1", "user2"], "L": [...], "S": [...]},
  "packedMeals": {"P1": [...], "P2": [...], "PS": [...]},
  "guests": [...]
}
```

**`users` collection** (relevant fields):
```json
{
  "name": "...",
  "email": "...",
  "meals": [[...7 days of meal flags...]],
  "preferences": {
    "notifications": {"enabled": true, "schedule": {...}},
    "device": "ExponentPushToken[...]"
  }
}
```

## Google Sheets Structure

Each month gets its own tab (e.g. "March 2026"). The DAG appends a new daily block below the previous one, 3 rows after the last populated entry. Each block includes:
- Date header
- Meal counts per type and diet
- Packed meal section (tomorrow's prediction)
- Running daily average in the header row

## Troubleshooting

**DAG not finding today's data**: Check that the date in MongoDB uses `D/M/YYYY` format (no leading zeros) and that today's Day record was created by the backend cron job.

**Google Sheets auth failing**: Verify the service account JSON exists at `/home/mateusz/secrets/sheets_sa.json` and has Editor access to the spreadsheet.

**Node.js subprocess failing**: Check that Node.js is installed at the expected path. Run `node --version` in the task or adjust the path in the BashOperator.

**Emails not sending**: Verify `RESEND_API_KEY` is in the Airflow environment and that `npm install` has been run inside `dags/meals/notifications/send_emails/`.

**Push notifications failing**: Ensure `npm install` has been run inside `dags/meals/notifications/mobile_notifications/` and Expo push tokens in MongoDB are valid.
