"""Small script to exercise the DB layer for SQLite or Postgres.

Usage:
  python test_db.py

It will create a small `members_collection` table (if not exists), insert two sample rows,
then print the detected columns and row count.

This script uses the same environment variables as `db.py`: `DB_ENGINE`, `DATABASE_URL`, `SQLITE_PATH`.
"""

import os
import pandas as pd
from db import insert_dataframe, get_target_columns, get_sqlite_path


def main():
    print("Using DB at:", get_sqlite_path())

    # sample data matching members_collection minimal expected columns
    data = [
        {"collection_code": "import", "s4": "Alice"},
        {"collection_code": "import", "s4": "Bob"},
    ]
    df = pd.DataFrame(data)

    print("Inserting sample rows...")
    insert_dataframe(df)

    cols = get_target_columns()
    print("Detected columns:", cols)

    # Show row count using pandas read_sql (via engine) if available
    try:
        # Prefer using the configured SQLAlchemy engine from db.py
        from sqlalchemy import text
        from db import engine

        with engine.connect() as conn:
            res = conn.execute(text("SELECT COUNT(*) AS c FROM members_collection"))
            # SQLAlchemy result objects differ by version; handle both
            try:
                count = res.scalar()
            except Exception:
                row = res.fetchone()
                count = row[0] if row else None

        print(f"Row count in members_collection: {count}")
    except Exception:
        # Fallback: try direct sqlite3 access to the sqlite file
        try:
            import sqlite3
            path = get_sqlite_path()
            conn = sqlite3.connect(path)
            cur = conn.execute("SELECT COUNT(*) FROM members_collection")
            count = cur.fetchone()[0]
            conn.close()
            print(f"Row count in members_collection: {count}")
        except Exception:
            print("Could not query row count (maybe driver missing). You can open the DB manually to verify.")


if __name__ == '__main__':
    main()
