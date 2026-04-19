# Backend (FastAPI)

Requirements:

- Python 3.9+
- Install packages:

```
pip install -r requirements.txt
```

Environment configuration:

1. Copy `.env.example` to `.env`.
2. Use Postgres (default) or switch to SQLite if needed.

Postgres example (single URL):

```
DB_ENGINE=postgres
DATABASE_URL=postgresql://user:password@db-host:5432/dbname
DB_SSLMODE=require
```

Postgres example (separate entries):

```
DB_ENGINE=postgres
PGHOST=db-host
PGPORT=5432
PGDATABASE=dbname
PGUSER=dbuser
PGPASSWORD=dbpassword
DB_SSLMODE=require
```

SQLite example:

```
DB_ENGINE=sqlite
SQLITE_PATH=./members.db
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

## Business Process Updates (Collections Workflow)

### 1. Serial Continuity And Conflict Prevention

- The system normalizes each inserted `members_collection` row so `s3` (daily sequence) and `s1` (composite serial) continue from the latest existing value for the same `date + church`.
- If imported/form-provided sequence values conflict with existing rows, the API automatically allocates the next available sequence.
- This prevents unique-key collisions on `s1` when uploads and manual form entries happen on the same date.

### 2. Post-Submission Locking

- New `members_collection` records are marked as locked after submission.
- Locked records cannot be edited by normal uploader/data-entry roles.
- Unlock operation is controlled by privileged role rights.

### 3. Unlock For Editing

- Endpoint: `POST /members_collection/{row_id}/unlock`
- Required right: `can_unlock_members_collections`
- Intended role: `head_treasurer` (also usable by admin/system admin)
- Unlock events are logged in `collection_action_logs`.

### 4. Deletion Approval Workflow

- Uploaders and other collection-entry users create deletion requests:
	- `POST /members_collection/{row_id}/delete_request`
- Head treasurer/admin reviews pending requests:
	- `GET /members_collection/delete_requests?status=pending`
- Approve and execute delete:
	- `POST /members_collection/delete_requests/{request_id}/approve`
- Reject request:
	- `POST /members_collection/delete_requests/{request_id}/reject`
- Privileged direct delete endpoint (audited):
	- `DELETE /members_collection/{row_id}`

### 5. Auditing

- All key collection workflow actions are written to `collection_action_logs`, including:
	- unlock
	- delete request create
	- delete approval/rejection
	- direct delete
	- collection row update
- Deletion requests and decisions are persisted in `members_collection_delete_requests` with actor and reason fields.

### 6. RBAC Additions

- New built-in role: `head_treasurer`
- New rights:
	- `can_delete_members_collections`
	- `can_unlock_members_collections`
- `head_treasurer` is seeded with both rights enabled.
