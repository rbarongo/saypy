import sys
import os
import pandas as pd

def _import_pyodbc_or_raise():
    try:
        import pyodbc
        return pyodbc
    except Exception:
        print("pyodbc is required to read Access databases. Install via pip.")
        raise


def migrate_access_to_target(access_path: str, target: str):
    """
    Migrate tables from a Microsoft Access file to a target database.

    The `target` argument can be either a path to a sqlite file (e.g. ./members.db)
    or a full SQLAlchemy database URL (e.g. postgresql://user:pass@host:5432/dbname).
    """
    pyodbc = _import_pyodbc_or_raise()

    if not os.path.exists(access_path):
        raise FileNotFoundError(f"Access file not found: {access_path}")

    conn_str = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;" % access_path
    print("Connecting to Access DB...")
    cn = pyodbc.connect(conn_str)
    cursor = cn.cursor()

    table_names = []
    for row in cursor.tables(tableType='TABLE'):
        name = row.table_name
        if name is None:
            continue
        if name.startswith('MSys'):
            continue
        table_names.append(name)

    print(f"Found tables: {table_names}")

    # Decide whether target is a URL or sqlite file path
    is_url = target.startswith("postgres://") or target.startswith("postgresql://") or \
             target.startswith("mysql://") or \
             target.startswith("mssql+")

    if is_url:
        # Use SQLAlchemy engine for writing to target databases
        try:
            from sqlalchemy import create_engine
        except Exception:
            print("SQLAlchemy is required to write to remote databases. Install via pip.")
            raise

        engine = create_engine(target)
        for t in table_names:
            print(f"Reading table {t}...")
            df = pd.read_sql_query(f"SELECT * FROM [{t}]", cn)
            print(f"Writing {len(df)} rows to target table {t}...")
            df.to_sql(t, engine, if_exists='replace', index=False)
        engine.dispose()
    else:
        # Assume sqlite file path
        import sqlite3
        sqlite_conn = sqlite3.connect(target)
        for t in table_names:
            print(f"Reading table {t}...")
            df = pd.read_sql_query(f"SELECT * FROM [{t}]", cn)
            print(f"Writing {len(df)} rows to sqlite table {t}...")
            df.to_sql(t, sqlite_conn, if_exists='replace', index=False)
        sqlite_conn.close()

    cn.close()
    print("Migration complete.")


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python migrate.py path/to/source.accdb path/to/target.db_or_db_url")
        sys.exit(1)
    access_file = sys.argv[1]
    target = sys.argv[2]
    migrate_access_to_target(access_file, target)
