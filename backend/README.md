# Backend (FastAPI)

Requirements:

- Python 3.9+
- Install packages:

```
pip install -r requirements.txt
```

Environment configuration:

1. Copy `.env.example` to `.env`.
2. Use SQLite (default) or switch to cloud Postgres.

SQLite example:

```
DB_ENGINE=sqlite
SQLITE_PATH=./members.db
```

Cloud Postgres example (single URL):

```
DB_ENGINE=postgres
DATABASE_URL=postgresql://user:password@db-host:5432/dbname
DB_SSLMODE=require
```

Cloud Postgres example (separate entries):

```
DB_ENGINE=postgres
PGHOST=db-host
PGPORT=5432
PGDATABASE=dbname
PGUSER=dbuser
PGPASSWORD=dbpassword
DB_SSLMODE=require
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
