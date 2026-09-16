# Docker deployment guide

This project ships two Compose stacks:

- **`docker-compose.yml`** — development. Live-reloading backend (`uvicorn --reload`)
  and frontend (Vite dev server), both bind-mounted from the host.
- **`docker-compose.prod.yml`** — production. Backend runs `uvicorn` with 4
  workers; frontend is built once into static files and served by nginx.

PostgreSQL is **not** containerized in either stack — both connect out to
whatever Postgres server `DATABASE_URL` points at (this project's own local
dev Postgres cluster on port 5433, or a real server in production).

## 1. Environment files

| File | Used for | Committed? |
|---|---|---|
| `.env.example` | Documents every variable both stacks read | Yes |
| `.env.dev` | Real dev-appropriate values, secrets encrypted | Yes (dev-only secrets, not sensitive) |
| `.env.prod` | Placeholder template — fill in before deploying | Yes (as a template) |
| `.env` | Default file — a copy of `.env.dev` | Yes |
| `.secrets.key` | Fernet key used to decrypt `enc:...` values | **No — gitignored** |

Docker Compose only auto-loads a file literally named `.env` — it does **not**
read `.env.dev`/`.env.prod` unless you pass `--env-file` explicitly. That's
why a root `.env` (a plain copy of `.env.dev`) exists: running `docker compose
up` with no flags "just works" for local development, while `.env.dev` stays
the readable, source-of-truth copy. If you change one, keep the other in
sync (`cp .env.dev .env`).

### Variables

See the comments in `.env.example` for the full list. In short:

- `DATABASE_URL` — full SQLAlchemy URL to Postgres. Supports `enc:...` (see below).
- `JWT_SECRET_KEY` — HMAC signing key for auth tokens. Supports `enc:...`.
- `JWT_ALGORITHM`, `JWT_EXPIRE_MINUTES` — auth token config.
- `CORS_ORIGINS` — comma-separated allowed origins, or `*`.
- `SEED_ADMIN_USERNAME` / `SEED_ADMIN_PASSWORD` — the admin account
  auto-created on first startup if it doesn't already exist. Password supports
  `enc:...`.
- `SEED_CHURCH_NAMES` — comma-separated churches auto-created on first startup.
- `VITE_API_BASE_URL` / `VITE_MEMBERS_API_BASE_URL` — **optional, leave blank
  by default.** The frontend already falls back to
  `<the host/domain the browser used to load the page>:8001` automatically
  (see `App.jsx` and `components/members-core/api.js`), and both compose
  files publish the backend on port 8001 on the same host as the frontend —
  so these only need to be set if your backend is reachable somewhere else
  (a different subdomain, a proxy in front of only one service, etc). Note
  these are **build-time** values for the production image (Vite bakes them
  into the static bundle) — changing them means rebuilding the frontend image.

## 2. Encrypting secrets

`DATABASE_URL`, `JWT_SECRET_KEY` and `SEED_ADMIN_PASSWORD` may each be stored
either as plain text or as `enc:<fernet-token>` — decryption happens
transparently in `api/app/settings.py` on startup (see `app/env_crypto.py`).
Anything else in the env files stays plain text.

```bash
cd api

# 1. Generate a key (do this once per environment — dev and prod should NOT
#    share a key). Save the output to a file named .secrets.key at the
#    project root.
python manage_secrets.py genkey > ../.secrets.key

# 2. Encrypt a value
python manage_secrets.py encrypt "postgresql+psycopg://user:pass@host:5432/dbname"
# -> enc:gAAAAABq....

# 3. (optional) verify round-trip
python manage_secrets.py decrypt "enc:gAAAAABq...."
```

Paste the `enc:...` output into the matching variable in your `.env.dev` /
`.env.prod` file.

`ENV_SECRETS_KEY_FILE` is already set inside both compose files to point at
the key mounted read-only into the container at `/app/.secrets.key` — you
don't need to set it yourself. Running the app outside Docker instead? Point
`ENV_SECRETS_KEY_FILE` at the key file's real path, or set `ENV_SECRETS_KEY`
directly to its contents, or just drop a `.secrets.key` file at the project
root (the default location `app/env_crypto.py` checks).

**A `.secrets.key` is already checked in at the project root for local dev
convenience**, and `.env.dev`'s encrypted values were produced with it — this
is fine for a throwaway local Postgres/JWT secret, but generate a fresh key
for any real deployment and never reuse the dev key in production.

## 3. Running — development

```bash
docker compose --env-file .env.dev up --build
# or, since .env already mirrors .env.dev:
docker compose up --build
```

- Frontend (Vite dev server, hot reload): http://localhost:5175
- Backend (FastAPI, auto-reload): http://localhost:8001 — docs at
  http://localhost:8001/docs
- The backend container uses `network_mode: host` (Linux only) so it can
  reach a Postgres server bound to `127.0.0.1` on the same machine, which is
  how this project's own local dev Postgres (port 5433) is set up. If your
  Postgres is on a genuinely separate host, you can safely remove
  `network_mode: host` from `docker-compose.yml` and use `ports: ["8001:8001"]`
  instead.
- Both services bind-mount the host source directories, so edits to
  `api/` or `frontend/` are picked up live without rebuilding.

Stop with `docker compose down` (or `Ctrl+C` then `docker compose down`).

## 4. Running — production

```bash
docker compose -f docker-compose.prod.yml --env-file .env.prod up --build -d
```

- Frontend (static build, served by nginx): http://your-host:8080
- Backend (FastAPI, 4 uvicorn workers): http://your-host:8001

View logs:

```bash
docker compose -f docker-compose.prod.yml logs -f backend
docker compose -f docker-compose.prod.yml logs -f frontend
```

Stop:

```bash
docker compose -f docker-compose.prod.yml down
```

### If Postgres runs on the same host as the containers

`docker-compose.prod.yml` adds `extra_hosts: host.docker.internal:host-gateway`
so the backend container can reach the host machine even in production. If
your "production" Postgres is actually on the same box as the containers,
set `DATABASE_URL`'s host to `host.docker.internal` and make sure Postgres
accepts connections from the Docker bridge subnet (by default it only listens
on `127.0.0.1`) — check `listen_addresses` in `postgresql.conf` and add a
matching line to `pg_hba.conf`.

## 5. Migrations & seed data

Both images run `api/entrypoint.sh` on startup, which:

1. Waits (up to 60s) for `DATABASE_URL`'s host:port to become reachable.
2. Runs `alembic upgrade head`.
3. Executes the container's `CMD` (uvicorn).

Seed data (the admin user + churches from `SEED_ADMIN_*`/`SEED_CHURCH_NAMES`)
is **not** a separate entrypoint step — it's already wired into the FastAPI
app's own `lifespan` startup handler (`app/main.py` → `app/seed.py`) and runs
every time the app starts, using get-or-create logic that's safe to re-run.

## 6. Management commands

```bash
# Rebuild after changing requirements.txt / package.json
docker compose up --build

# Run a one-off command inside the backend container (e.g. a manual migration)
docker compose exec backend alembic upgrade head
docker compose exec backend python manage_secrets.py encrypt "..."

# Reset dev containers/volumes entirely (does NOT touch your real Postgres data,
# since Postgres isn't containerized — this only removes app containers/images)
docker compose down
docker compose up --build --force-recreate
```

## 7. Troubleshooting

- **Frontend container doesn't pick up a newly added npm dependency** — the
  dev Dockerfile's `node_modules` is an anonymous volume that persists across
  restarts and can go stale after editing `package.json` on the host. Force a
  rebuild: `docker compose up --build --force-recreate frontend`, or
  `docker compose down && docker volume prune` if that's not enough (this
  removes anonymous volumes for stopped services only).
- **Backend can't reach Postgres** — `entrypoint.sh` prints a warning and
  continues after 60s rather than failing outright; the actual error will
  then surface loudly from `alembic upgrade head` right after. Check
  `DATABASE_URL`'s host/port, and that Postgres's `pg_hba.conf` allows the
  connecting address (`127.0.0.1` for dev's `network_mode: host`, the Docker
  bridge subnet for prod's default networking).
- **"Found an encrypted .env value (enc:...) but no decryption key is
  configured"** — `.secrets.key` isn't mounted, or `ENV_SECRETS_KEY_FILE`
  points somewhere else. Both compose files already mount
  `./.secrets.key:/app/.secrets.key:ro` and set `ENV_SECRETS_KEY_FILE`
  accordingly — check the file actually exists at the project root.
- **Frontend can reach the backend on localhost during dev but not after
  deploying** — set `VITE_API_BASE_URL`/`VITE_MEMBERS_API_BASE_URL` explicitly
  (see §1) if the backend truly isn't reachable at `<frontend's own
  host>:8001`, then rebuild the frontend image (these are baked in at build
  time, not read at runtime).
