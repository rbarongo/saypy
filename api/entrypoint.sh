#!/bin/sh
set -e

# Wait for the database to accept connections. Meaningful whenever Postgres
# and this container start at roughly the same time (e.g. both launched by
# `docker compose up`, or Postgres itself is still coming up on the host) —
# a no-op wait (a couple seconds at most) once it's already reachable, as is
# normally the case for an existing/remote Postgres server.
echo "Waiting for the database..."
python -c "
import sys, time, urllib.parse, socket
sys.path.insert(0, '/app')
from app.settings import settings

url = settings.database_url or ''
if not url:
    sys.exit(0)

parsed = urllib.parse.urlparse(url)
host, port = parsed.hostname, parsed.port or 5432

for _ in range(60):
    try:
        with socket.create_connection((host, port), timeout=2):
            print(f'Database is reachable at {host}:{port}.')
            sys.exit(0)
    except OSError:
        time.sleep(1)

print(f'Warning: could not reach {host}:{port} within 60s — continuing anyway '
      '(the next step, migrations, will fail loudly if it truly is unreachable).')
"

# Run pending Alembic migrations. Safe to run on every startup — it's a no-op
# once the schema is already current.
echo "Running database migrations..."
alembic upgrade head

# NOTE: unlike a typical Django app, there's no separate seed step here —
# app.seed.run() is called automatically from FastAPI's own lifespan handler
# (see app/main.py) on every app startup, using get_or_create-style inserts
# that are safe to run repeatedly.

echo "Executing: $@"
exec "$@"
