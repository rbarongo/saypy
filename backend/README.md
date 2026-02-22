# Backend (FastAPI)

Requirements:

- Python 3.9+
- Install packages:

```
pip install -r requirements.txt
```

Migration from Access to SQLite:

1. Ensure Microsoft Access ODBC Driver is installed on Windows.
2. Run:

```
python migrate.py "C:\path\to\KSC_20260219_Feb_Uwakili.accdb" "members.db"
```

This will create `members.db` with all Access tables.

Run the API:

```
uvicorn app:app --reload --port 8000
```

Upload endpoint:
- `POST /upload` — multipart file (Excel or CSV). The API maps uploaded columns (case-insensitive) into `members_collection` table columns and inserts rows.
