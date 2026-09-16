# Saypy Core API (`api/`)

A fresh FastAPI + SQLAlchemy 2.0 backend, independent from `backend_old/`. This is
the start of the system's rebuild — currently covers **members management**
(members, families, spouse/parent relationships, family tree) plus the minimal
auth/users/churches needed to run it.

- Runs on **port 8001** (`backend_old` keeps running on 8000, unaffected).
- Own Postgres database: `saypy_core` on the same local server as `chrch_collections`
  (localhost:5433) — no data is shared with the legacy system.
- Own JWT auth, independent from `backend_old`'s token system — log in via `/auth/login`.

## Setup

```bash
cd api
cp .env.example .env   # edit JWT_SECRET_KEY / SEED_ADMIN_PASSWORD for real use
python3 -m venv .venv  # if .venv doesn't already exist
.venv/bin/pip install -r requirements.txt

# create the database once:
PGPASSWORD=omi12345 psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE saypy_core;"

# apply the schema:
.venv/bin/python -m alembic upgrade head
```

## Run

```bash
cd api
.venv/bin/uvicorn app.main:app --reload --port 8001
```

On first startup, the app seeds:
- Churches from `SEED_CHURCH_NAMES` in `.env`.
- A `system_admin` user from `SEED_ADMIN_USERNAME` / `SEED_ADMIN_PASSWORD`.

Docs: http://127.0.0.1:8001/docs

## Data model summary

- **Church** — `name`, `code`.
- **User** — auth account; `role` (`system_admin`/`admin`/`viewer`), `church_id` scoping.
- **Family** — a household grouping, scoped to a church.
- **Member** — has both a `household_role` (head_of_family / spouse / child / dependent /
  single_adult — drives family-tree logic) and a `membership_category` (full_member /
  associate_member / baptized / non_baptized / clergy / visitor — church standing).
  Relationships are self-referencing FKs: `family_id`, `spouse_id` (symmetric, unique),
  `father_id`, `mother_id`.

Relationship changes (linking/unlinking a spouse or parent, deleting a member) always go
through `app/services/member_service.py`, which enforces symmetry, same-church, no-cycle,
and family_id reconciliation rules — never edit those columns directly via `PUT /members/{id}`.

## Key endpoints

- `POST /auth/login`, `GET /auth/me`
- `GET/POST /churches`
- `GET/POST /members`, `GET/PUT/DELETE /members/{id}`
- `POST /members/{id}/link-spouse|unlink-spouse|link-parents|unlink-parent`
- `GET /members/{id}/family-tree`
- `GET/POST /families`, `GET/PUT/DELETE /families/{id}`, `GET /families/{id}/tree`

## Migrations

Schema changes go through Alembic:

```bash
.venv/bin/python -m alembic revision --autogenerate -m "describe the change"
.venv/bin/python -m alembic upgrade head
```
