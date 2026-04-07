import os
import hashlib
import binascii
import uuid
import zipfile
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus
from datetime import datetime, timedelta
from typing import List
import pandas as pd
from dotenv import load_dotenv

# Use SQLAlchemy to support both SQLite and Postgres via a single API
from sqlalchemy import create_engine, inspect
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime, func, text, Numeric
from sqlalchemy import insert as sql_insert
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, Tuple

BASE_DIR = os.path.dirname(__file__)

# Load backend-local env first so deployments using `backend/.env` are deterministic.
load_dotenv(os.path.join(BASE_DIR, ".env"))
# Also load default-discovery .env (does not override existing values).
load_dotenv()

# Config: DB_ENGINE can be 'sqlite' or 'postgres' (or 'postgresql')
DB_ENGINE = os.getenv("DB_ENGINE", "sqlite").lower()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _build_postgres_url_from_parts() -> str:
    """Build a postgresql:// URL from discrete PG* env vars when DATABASE_URL is absent."""
    host = os.getenv("PGHOST") or os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST")
    dbname = os.getenv("PGDATABASE") or os.getenv("POSTGRES_DB") or os.getenv("DB_NAME")
    user = os.getenv("PGUSER") or os.getenv("POSTGRES_USER") or os.getenv("DB_USER")
    password = os.getenv("PGPASSWORD") or os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD")
    port = os.getenv("PGPORT") or os.getenv("POSTGRES_PORT") or os.getenv("DB_PORT") or "5432"

    if not all([host, dbname, user, password]):
        return ""

    user_escaped = quote_plus(str(user))
    password_escaped = quote_plus(str(password))
    return f"postgresql://{user_escaped}:{password_escaped}@{host}:{port}/{dbname}"

# If DATABASE_URL is set (recommended for Postgres), use it. Otherwise for sqlite build file path.
_sqlite_default_path = os.path.join(BASE_DIR, "members.db")
if DB_ENGINE in ("sqlite", "sqlite3"):
    SQLITE_PATH = os.getenv("SQLITE_PATH", _sqlite_default_path)
    DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{SQLITE_PATH}")
else:
    # For postgres: expect DATABASE_URL like 'postgresql://user:pass@host:port/dbname'
    DATABASE_URL = os.getenv("DATABASE_URL", "") or _build_postgres_url_from_parts()
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = "postgresql://" + DATABASE_URL[len("postgres://"):]
    if not DATABASE_URL:
        raise RuntimeError(
            "DB_ENGINE is postgres but no DB config was found. Set DATABASE_URL or PGHOST/PGDATABASE/PGUSER/PGPASSWORD."
        )

# Create SQLAlchemy engine. For sqlite we need connect_args to allow multithreaded access
engine_kwargs = {}
if DB_ENGINE in ("sqlite", "sqlite3"):
    engine_kwargs = {"connect_args": {"check_same_thread": False}}
else:
    connect_args = {}
    sslmode = os.getenv("DB_SSLMODE", "").strip()
    if sslmode:
        connect_args["sslmode"] = sslmode

    engine_kwargs = {
        "pool_pre_ping": True,
        "pool_size": _env_int("DB_POOL_SIZE", 5),
        "max_overflow": _env_int("DB_MAX_OVERFLOW", 10),
        "pool_timeout": _env_int("DB_POOL_TIMEOUT", 30),
        "pool_recycle": _env_int("DB_POOL_RECYCLE", 1800),
    }
    if connect_args:
        engine_kwargs["connect_args"] = connect_args

engine = create_engine(DATABASE_URL, **engine_kwargs)

def get_sqlite_path() -> str:
    """Return the underlying DB path/URL. For sqlite returns the file path, for others returns DATABASE_URL."""
    if DB_ENGINE in ("sqlite", "sqlite3"):
        return os.path.abspath(SQLITE_PATH)
    return DATABASE_URL


def ensure_db_exists():
    """For sqlite: ensure file exists. For Postgres: attempt a connection to validate access."""
    if DB_ENGINE in ("sqlite", "sqlite3"):
        if not os.path.exists(SQLITE_PATH):
            # Touch the sqlite file by creating an empty engine connection and disposing
            conn = engine.connect()
            conn.close()
    else:
        # Try connecting to Postgres to verify availability
        conn = engine.connect()
        conn.close()


def get_target_columns(table_name: str = "members_collection") -> List[str]:
    """Return target table column names. Empty list if table doesn't exist."""
    ensure_db_exists()
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return []
    cols = [c["name"] for c in inspector.get_columns(table_name)]
    return cols


def insert_dataframe(df: pd.DataFrame, table_name: str = "members_collection") -> None:
    """Insert rows from a DataFrame into the configured database table."""
    ensure_db_exists()
    tn = (table_name or "").lower()
    if tn in ("members_collection", "members_collections"):
        rows = df.to_dict(orient='records') if df is not None else []
        rows = normalize_members_collection_rows(rows, fill_missing_s3=True, recompute_s1=True)
        df = pd.DataFrame(rows)
    # Use pandas to_sql which works with SQLAlchemy engines for both sqlite and postgres
    df.to_sql(table_name, engine, if_exists="append", index=False)


def _coerce_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        s = str(v).strip()
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def _coerce_s1_text(v) -> Optional[str]:
    if v is None:
        return None
    n = _coerce_int(v)
    if n is not None:
        return str(n)
    s = str(v).strip()
    if s == "":
        return None
    return s


def _parse_collection_dt(v) -> Optional[datetime]:
    if v is None:
        return None
    try:
        if isinstance(v, datetime):
            return v
        dt = pd.to_datetime(v, errors='coerce')
        if pd.isna(dt):
            return None
        if hasattr(dt, 'to_pydatetime'):
            return dt.to_pydatetime()
        return dt
    except Exception:
        return None


def compute_members_collection_s1(s2_value, church_value, s3_value) -> Optional[str]:
    """Build s1 in the format YYYYMMDD + church(3) + s3(3)."""
    s2dt = _parse_collection_dt(s2_value)
    church_id = _coerce_int(church_value)
    s3num = _coerce_int(s3_value)
    if s2dt is None or church_id is None or s3num is None:
        return None
    ymd = s2dt.strftime('%Y%m%d')
    return f"{ymd}{int(church_id):03d}{int(s3num):03d}"


def _load_existing_serial_counters(keys: List[Tuple[str, int]]) -> Dict[Tuple[str, int], int]:
    counters: Dict[Tuple[str, int], int] = {}
    if not keys:
        return counters
    try:
        with engine.connect() as conn:
            res = conn.execute(text('SELECT s2, church, s3 FROM members_collection WHERE s2 IS NOT NULL AND church IS NOT NULL'))
            for r in res.fetchall():
                try:
                    dt = _parse_collection_dt(r[0])
                    ch = _coerce_int(r[1])
                    s3v = _coerce_int(r[2])
                    if dt is None or ch is None or s3v is None:
                        continue
                    k = (dt.strftime('%Y-%m-%d'), int(ch))
                    if k not in keys:
                        continue
                    counters[k] = max(counters.get(k, 0), int(s3v))
                except Exception:
                    continue
    except Exception:
        return counters
    return counters


def normalize_members_collection_rows(rows: List[dict], fill_missing_s3: bool = True, recompute_s1: bool = True) -> List[dict]:
    """Normalize members_collection rows: ensure s3 serial and canonical s1 format."""
    if not rows:
        return []

    keys = set()
    for row in rows:
        dt = _parse_collection_dt(row.get('s2') if isinstance(row, dict) else None)
        ch = _coerce_int(row.get('church') if isinstance(row, dict) else None)
        if dt is not None and ch is not None:
            keys.add((dt.strftime('%Y-%m-%d'), int(ch)))

    counters = _load_existing_serial_counters(list(keys))
    out = []
    for row in rows:
        r = dict(row)

        for sk in ('s4', 's10', 's11', 's12', 'source', 'collection_code', 'notes'):
            if r.get(sk) is not None and not isinstance(r.get(sk), str):
                r[sk] = str(r.get(sk))

        s2dt = _parse_collection_dt(r.get('s2'))
        if s2dt is not None:
            r['s2'] = s2dt
        church_id = _coerce_int(r.get('church'))
        if church_id is not None:
            r['church'] = church_id

        s3num = _coerce_int(r.get('s3'))
        if fill_missing_s3 and s3num is None and s2dt is not None and church_id is not None:
            k = (s2dt.strftime('%Y-%m-%d'), int(church_id))
            nxt = counters.get(k, 0) + 1
            counters[k] = nxt
            s3num = nxt

        if s3num is not None:
            r['s3'] = int(s3num)

        if recompute_s1:
            s1 = compute_members_collection_s1(r.get('s2'), r.get('church'), r.get('s3'))
            if s1 is not None:
                r['s1'] = str(s1)

        out.append(r)
    return out


def backfill_members_collection_identifiers() -> int:
    """Backfill s3/s1 for existing rows to canonical format YYYYMMDD + church(3) + s3(3)."""
    ensure_db_exists()
    inspector = inspect(engine)
    if 'members_collection' not in inspector.get_table_names():
        return 0

    with engine.connect() as conn:
        rows = conn.execute(text('SELECT id, s2, church, s3, s1 FROM members_collection ORDER BY id')).fetchall()

    if not rows:
        return 0

    counters: Dict[Tuple[str, int], int] = {}
    normalized = []

    for rid, s2, church, s3, s1 in rows:
        dt = _parse_collection_dt(s2)
        ch = _coerce_int(church)
        s3n = _coerce_int(s3)
        key = (dt.strftime('%Y-%m-%d'), int(ch)) if dt is not None and ch is not None else None
        if key and s3n is not None:
            counters[key] = max(counters.get(key, 0), s3n)
        normalized.append({'id': rid, 'dt': dt, 'church': ch, 's3': s3n, 's1': _coerce_s1_text(s1), 'key': key})

    updated = 0
    with engine.begin() as conn:
        for item in normalized:
            dt = item['dt']
            ch = item['church']
            key = item['key']
            s3n = item['s3']
            if key and s3n is None:
                nxt = counters.get(key, 0) + 1
                counters[key] = nxt
                s3n = nxt

            new_s1 = compute_members_collection_s1(dt, ch, s3n)
            if new_s1 is None:
                continue

            if item['s1'] != str(new_s1) or item['s3'] != s3n:
                conn.execute(
                    text('UPDATE members_collection SET s3=:s3, s1=:s1 WHERE id=:id'),
                    {'s3': int(s3n), 's1': str(new_s1), 'id': item['id']}
                )
                updated += 1
    return updated


# --- Table definitions and helpers ---
metadata = MetaData()

members = Table(
    'members', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('sno', Integer, nullable=False, unique=True),
    Column('MEMBER_NAME', String(300), nullable=True),
    Column('church', Integer, nullable=True),
    Column('MEMBER_ID', Integer, nullable=True),
    Column('FAMILY_ID', Integer, nullable=True),
    Column('DEFAULT_FAMILY_ID', Integer, nullable=True),
    Column('OFFICIAL_MEMBER_ID', Integer, nullable=True),
    Column('pledge', Numeric, nullable=True),
    Column('GROUP_NAME', String(200), nullable=True),
    Column('GROUP_ALIAS', String(200), nullable=True),
    Column('DEFAULT_GROUP_ALIAS', String(200), nullable=True),
    Column('GROUP_LEADER_ID', Integer, nullable=True),
    Column('DEFAULT_GROUP_LEADER_ID', Integer, nullable=True),
    Column('STATUS', String(100), nullable=True),
    Column('PHONE', String(100), nullable=True),
    Column('PHONE2', String(100), nullable=True),
    Column('EMAIL', String(320), nullable=True),
    Column('RESIDENCE', String(400), nullable=True),
    Column('created_at', DateTime, server_default=func.now()),
)


church = Table(
    'church', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(200), nullable=False, unique=True),
    Column('app_name', String(300), nullable=True),  # Custom application name for local admin
)

members_collection = Table(
    'members_collection', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('collection_code', String(200), nullable=False),
    Column('member_id', Integer, ForeignKey('members.id'), nullable=True),
    Column('church', Integer, ForeignKey('church.id'), nullable=True),
    Column('s1', String(32), nullable=True),
    Column('s2', DateTime, nullable=True),
    Column('s3', Integer, nullable=True),
    Column('s4', String(255), nullable=True),
    Column('s5', Numeric, nullable=True),
    Column('s6', Numeric, nullable=True),
    Column('s7', Numeric, nullable=True),
    Column('s8', Numeric, nullable=True),
    Column('s9', Numeric, nullable=True),
    Column('s10', String(255), nullable=True),
    Column('s11', String(255), nullable=True),
    Column('s12', String(255), nullable=True),
    Column('s13', Numeric, nullable=True),
    Column('c1', Numeric, nullable=True),
    Column('c2', Numeric, nullable=True),
    Column('c3', Numeric, nullable=True),
    Column('c4', Numeric, nullable=True),
    Column('c5', Numeric, nullable=True),
    Column('c6', Numeric, nullable=True),
    Column('c7', Numeric, nullable=True),
    Column('c8', Numeric, nullable=True),
    Column('c9', Numeric, nullable=True),
    Column('c10', Numeric, nullable=True),
    Column('c11', Numeric, nullable=True),
    Column('c12', Numeric, nullable=True),
    Column('c13', Numeric, nullable=True),
    Column('c14', Numeric, nullable=True),
    Column('c15', Numeric, nullable=True),
    Column('c16', Numeric, nullable=True),
    Column('c17', Numeric, nullable=True),
    Column('c18', Numeric, nullable=True),
    Column('c19', Numeric, nullable=True),
    Column('c20', Numeric, nullable=True),
    Column('l1', Numeric, nullable=True),
    Column('l2', Numeric, nullable=True),
    Column('l3', Numeric, nullable=True),
    Column('l4', Numeric, nullable=True),
    Column('l5', Numeric, nullable=True),
    Column('l6', Numeric, nullable=True),
    Column('l7', Numeric, nullable=True),
    Column('l8', Numeric, nullable=True),
    Column('l9', Numeric, nullable=True),
    Column('l10', Numeric, nullable=True),
    Column('l11', Numeric, nullable=True),
    Column('l12', Numeric, nullable=True),
    Column('l13', Numeric, nullable=True),
    Column('l14', Numeric, nullable=True),
    Column('l15', Numeric, nullable=True),
    Column('l16', Numeric, nullable=True),
    Column('l17', Numeric, nullable=True),
    Column('l18', Numeric, nullable=True),
    Column('l19', Numeric, nullable=True),
    Column('l20', Numeric, nullable=True),
    Column('l21', Numeric, nullable=True),
    Column('l22', Numeric, nullable=True),
    Column('l23', Numeric, nullable=True),
    Column('l24', Numeric, nullable=True),
    Column('l25', Numeric, nullable=True),
    Column('l26', Numeric, nullable=True),
    Column('l27', Numeric, nullable=True),
    Column('l28', Numeric, nullable=True),
    Column('l29', Numeric, nullable=True),
    Column('l30', Numeric, nullable=True),
    Column('l31', Numeric, nullable=True),
    Column('l32', Numeric, nullable=True),
    Column('l33', Numeric, nullable=True),
    Column('l34', Numeric, nullable=True),
    Column('l35', Numeric, nullable=True),
    Column('l36', Numeric, nullable=True),
    Column('l37', Numeric, nullable=True),
    Column('l38', Numeric, nullable=True),
    Column('l39', Numeric, nullable=True),
    Column('l40', Numeric, nullable=True),
    Column('l41', Numeric, nullable=True),
    Column('source', String(200), nullable=True),
    Column('notes', String(1000), nullable=True),
    Column('added_at', DateTime, server_default=func.now()),
)


collection_codes = Table(
    'collection_codes', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('column_name', String(100), nullable=False),  # Removed unique constraint to allow per-church codes
    Column('code', String(400), nullable=True),
    Column('custom_collection_name', String(400), nullable=True),
    Column('church', Integer, ForeignKey('church.id'), nullable=True),  # NULL = global code, value = church-specific code
)


header_mappings = Table(
    'header_mappings', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('header_name', String(400), nullable=False, unique=True),
    Column('mapped_column', String(100), nullable=False),
)


uploaders = Table(
    'uploaders', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(200), nullable=False),
    Column('api_key', String(128), nullable=False, unique=True),
    Column('church', Integer, ForeignKey('church.id'), nullable=True),
    Column('created_at', DateTime, server_default=func.now()),
)


role_policies = Table(
    'role_policies', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('role', String(80), nullable=False, unique=True),
    Column('display_name', String(160), nullable=True),
    Column('can_view_dashboard', Integer, nullable=False, server_default='1'),
    Column('can_manage_users', Integer, nullable=False, server_default='0'),
    Column('can_manage_members', Integer, nullable=False, server_default='0'),
    Column('can_manage_collections', Integer, nullable=False, server_default='0'),
    Column('can_manage_members_collections', Integer, nullable=False, server_default='0'),
    Column('can_view_reports', Integer, nullable=False, server_default='0'),
    Column('can_manage_settings', Integer, nullable=False, server_default='0'),
    Column('can_manage_roles', Integer, nullable=False, server_default='0'),
    Column('can_view_collection_codes', Integer, nullable=False, server_default='0'),
    Column('system_protected', Integer, nullable=False, server_default='0'),
)


users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('username', String(150), nullable=False, unique=True),
    Column('first_name', String(120), nullable=True),
    Column('middle_name', String(120), nullable=True),
    Column('last_name', String(120), nullable=True),
    Column('email', String(320), nullable=True),
    Column('phone', String(60), nullable=True),
    Column('password_hash', String(128), nullable=False),
    Column('salt', String(64), nullable=False),
    Column('church', Integer, ForeignKey('church.id'), nullable=True),
    Column('role', String(20), nullable=False, server_default='uploader'),
    Column('created_at', DateTime, server_default=func.now()),
)

tokens = Table(
    'tokens', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('token', String(64), nullable=False, unique=True),
    Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
    Column('created_at', DateTime, server_default=func.now()),
)

global_config = Table(
    'global_config', metadata,
    Column('key', String(100), primary_key=True),
    Column('value', String(500), nullable=True),
)

def ensure_global_config_schema():
    """Create global_config table if absent and seed the default system_name."""
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS global_config (key VARCHAR(100) PRIMARY KEY, value VARCHAR(500))"
        ))
        conn.execute(text(
            "INSERT INTO global_config (key, value) SELECT 'system_name', 'Church Offerings' "
            "WHERE NOT EXISTS (SELECT 1 FROM global_config WHERE key = 'system_name')"
        ))


def create_tables():
    """Create `members` and `members_collection` tables if they do not exist."""
    ensure_db_exists()
    # Create base tables first.
    metadata.create_all(engine, tables=[church, members, collection_codes, header_mappings, uploaders, role_policies, users, tokens, global_config])

    # Ensure referenced members columns are uniquely constrained before creating dependent FKs.
    try:
        with engine.begin() as conn:
            conn.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ix_members_id_unique ON members(id)'))
            conn.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ix_members_sno_unique ON members(sno)'))
    except Exception:
        pass

    # Create dependent table after uniqueness constraints exist.
    metadata.create_all(engine, tables=[members_collection])
    try:
        ensure_users_schema()
    except Exception:
        pass
    try:
        ensure_role_policies_schema()
    except Exception:
        pass
    try:
        ensure_collection_codes_schema()
    except Exception:
        pass
    # Ensure any new columns are present on existing tables (simple ALTER TABLE add column migration)
    try:
        ensure_members_collection_schema()
    except Exception:
        # swallow errors here; in production you'd log
        pass
    try:
        # Before adding a unique constraint, remove any duplicate sno values
        deduplicate_members_by_sno()
    except Exception:
        pass

    try:
        # Remove duplicate s1 values in members_collection before adding unique index
        deduplicate_members_collection_s1()
    except Exception:
        pass

    try:
        backfill_members_collection_identifiers()
    except Exception:
        pass

    try:
        deduplicate_members_collection_s1()
    except Exception:
        pass

    try:
        with engine.begin() as conn:
            conn.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ix_members_collection_s1_unique ON members_collection(s1)'))
    except Exception:
        pass

    try:
        seed_role_policies()
    except Exception:
        pass
    try:
        seed_churches()
    except Exception:
        pass
    try:
        ensure_global_config_schema()
    except Exception:
        pass
    try:
        import_collection_codes_from_workbook()
    except Exception:
        pass


def _xlsx_read_collection_codes_rows(xlsx_path: str) -> List[dict]:
    """Read Collection_Codes.xlsx rows as dicts using stdlib XML parsing.

    Expected headers in first row: COLUMN_NAME, CODE, ID, CHURCH.
    Business mapping used by the app:
    - CODE -> db column_name (field key such as s1/c1/l31)
    - COLUMN_NAME -> db code (display label)
    """
    if not os.path.exists(xlsx_path):
        return []

    def _col_letters(ref: str) -> str:
        out = []
        for ch in str(ref):
            if ch.isalpha():
                out.append(ch)
            else:
                break
        return ''.join(out)

    rows_out: List[dict] = []
    ns_main = {'x': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}

    with zipfile.ZipFile(xlsx_path, 'r') as zf:
        shared_strings = []
        if 'xl/sharedStrings.xml' in zf.namelist():
            ss_root = ET.fromstring(zf.read('xl/sharedStrings.xml'))
            for si in ss_root.findall('.//x:si', ns_main):
                parts = [t.text or '' for t in si.findall('.//x:t', ns_main)]
                shared_strings.append(''.join(parts))

        sheet_xml = 'xl/worksheets/sheet1.xml'
        if sheet_xml not in zf.namelist():
            return []

        root = ET.fromstring(zf.read(sheet_xml))
        sheet_data = root.find('x:sheetData', ns_main)
        if sheet_data is None:
            return []

        header_map = {}
        for row in sheet_data.findall('x:row', ns_main):
            r_idx = int(row.attrib.get('r', '0') or '0')
            cells = {}
            for c in row.findall('x:c', ns_main):
                ref = c.attrib.get('r', '')
                col = _col_letters(ref)
                c_type = c.attrib.get('t', '')
                v = c.find('x:v', ns_main)
                if c_type == 's' and v is not None and v.text is not None:
                    try:
                        idx = int(v.text)
                        value = shared_strings[idx] if 0 <= idx < len(shared_strings) else ''
                    except Exception:
                        value = ''
                elif c_type == 'inlineStr':
                    t = c.find('x:is/x:t', ns_main)
                    value = t.text if t is not None and t.text is not None else ''
                else:
                    value = v.text if v is not None and v.text is not None else ''
                cells[col] = str(value).strip()

            if r_idx == 1:
                for col, val in cells.items():
                    hv = str(val or '').strip().upper()
                    if hv:
                        header_map[col] = hv
                continue

            if not cells:
                continue

            row_dict = {}
            for col, hv in header_map.items():
                row_dict[hv] = cells.get(col, '').strip()

            code_key = str(row_dict.get('CODE') or '').strip()
            label_value = str(row_dict.get('COLLECTION_NAME') or row_dict.get('COLUMN_NAME') or '').strip()
            church_raw = str(row_dict.get('CHURCH') or '').strip()
            if not code_key or not label_value:
                continue
            church_id = None
            if church_raw:
                try:
                    church_id = int(float(church_raw))
                except Exception:
                    church_id = None
            rows_out.append({
                'column_name': code_key,
                'code': label_value,
                'church': church_id,
                'custom_collection_name': None,
            })

    return rows_out


def import_collection_codes_from_workbook(workbook_path: Optional[str] = None) -> dict:
    """Import default collection codes from Collection_Codes.xlsx for every church.

    Workbook is treated as template rows. Defaults are materialized per church id.
    Upserts by (church, column_name) so repeated startup runs are idempotent.
    """
    ensure_db_exists()
    inspector = inspect(engine)
    if 'collection_codes' not in inspector.get_table_names():
        return {'ok': False, 'reason': 'collection_codes table missing', 'inserted': 0, 'updated': 0}

    xlsx_path = workbook_path or os.path.abspath(os.path.join(BASE_DIR, '..', 'Collection_Codes.xlsx'))
    template_rows = _xlsx_read_collection_codes_rows(xlsx_path)
    if not template_rows:
        return {'ok': False, 'reason': 'workbook missing or empty', 'path': xlsx_path, 'inserted': 0, 'updated': 0}

    with engine.connect() as conn:
        church_rows = conn.execute(text('SELECT id FROM church ORDER BY id')).fetchall()
    church_ids = [int(r[0]) for r in church_rows if r and r[0] is not None]
    if not church_ids:
        return {'ok': False, 'reason': 'no churches found', 'path': xlsx_path, 'inserted': 0, 'updated': 0}

    # Use workbook rows as defaults regardless of the source church id in workbook.
    rows = []
    for ch_id in church_ids:
        for item in template_rows:
            rows.append({
                'column_name': item['column_name'],
                'code': item['code'],
                'custom_collection_name': item.get('custom_collection_name'),
                'church': ch_id,
            })

    inserted = 0
    updated = 0
    with engine.connect() as conn:
        for item in rows:
            existing = conn.execute(
                text('SELECT id, code, custom_collection_name FROM collection_codes WHERE church=:ch AND LOWER(column_name)=LOWER(:cn) LIMIT 1'),
                {'ch': item['church'], 'cn': item['column_name']}
            ).fetchone()
            if existing:
                current_code = str(existing[1] or '').strip()
                new_code = str(item['code'] or '').strip()
                current_custom = str(existing[2] or '').strip()
                new_custom = str(item.get('custom_collection_name') or '').strip()
                if current_code != new_code or current_custom != new_custom:
                    conn.execute(
                        text('UPDATE collection_codes SET code=:code, custom_collection_name=:ccn WHERE id=:id'),
                        {'code': new_code, 'ccn': (new_custom or None), 'id': int(existing[0])}
                    )
                    updated += 1
            else:
                conn.execute(sql_insert(collection_codes).values(**item))
                inserted += 1
        try:
            conn.commit()
        except Exception:
            pass

    return {'ok': True, 'path': xlsx_path, 'rows': len(rows), 'churches': len(church_ids), 'inserted': inserted, 'updated': updated}


def ensure_collection_codes_schema():
    """Ensure collection_codes has church and custom_collection_name columns."""
    inspector = inspect(engine)
    if 'collection_codes' not in inspector.get_table_names():
        return
    existing = {c['name'] for c in inspector.get_columns('collection_codes')}
    expected = {
        'church': 'INTEGER',
        'custom_collection_name': 'TEXT',
    }
    for col, typ in expected.items():
        if col in existing:
            continue
        stmt = f'ALTER TABLE collection_codes ADD COLUMN {col} {typ}'
        with engine.connect() as conn:
            conn.execute(text(stmt))
            try:
                conn.commit()
            except Exception:
                pass

    # Legacy databases may still have a unique constraint/index on column_name
    # from the old global-only model. Drop it so per-church rows can coexist.
    # Postgres path: constraint name commonly auto-created as *_key.
    if DB_ENGINE not in ("sqlite", "sqlite3"):
        with engine.connect() as conn:
            try:
                conn.execute(text('ALTER TABLE collection_codes DROP CONSTRAINT IF EXISTS collection_codes_column_name_key'))
            except Exception:
                pass
            try:
                conn.execute(text('DROP INDEX IF EXISTS collection_codes_column_name_key'))
            except Exception:
                pass
            try:
                conn.execute(text('DROP INDEX IF EXISTS ix_collection_codes_column_name'))
            except Exception:
                pass
            try:
                conn.commit()
            except Exception:
                pass


def seed_role_policies():
    """Seed built-in role policies if table is empty."""
    ensure_db_exists()
    inspector = inspect(engine)
    if 'role_policies' not in inspector.get_table_names():
        return
    with engine.connect() as conn:
        res = conn.execute(text('SELECT COUNT(*) FROM role_policies'))
        try:
            cnt = res.scalar()
        except Exception:
            row = res.fetchone()
            cnt = row[0] if row else 0
        if cnt and int(cnt) > 0:
            return

        defaults = [
            {
                'role': 'system_admin',
                'display_name': 'System Admin',
                'can_view_dashboard': 1,
                'can_manage_users': 1,
                'can_manage_members': 1,
                'can_manage_collections': 1,
                'can_manage_members_collections': 1,
                'can_view_reports': 1,
                'can_manage_settings': 1,
                'can_manage_roles': 1,
                'can_view_collection_codes': 1,
                'system_protected': 1,
            },
            {
                'role': 'admin',
                'display_name': 'Church Admin',
                'can_view_dashboard': 1,
                'can_manage_users': 1,
                'can_manage_members': 1,
                'can_manage_collections': 1,
                'can_manage_members_collections': 1,
                'can_view_reports': 1,
                'can_manage_settings': 1,
                'can_manage_roles': 0,
                'can_view_collection_codes': 1,
                'system_protected': 1,
            },
            {
                'role': 'treasurer',
                'display_name': 'Treasurer',
                'can_view_dashboard': 1,
                'can_manage_users': 0,
                'can_manage_members': 0,
                'can_manage_collections': 1,
                'can_manage_members_collections': 1,
                'can_view_reports': 1,
                'can_manage_settings': 0,
                'can_manage_roles': 0,
                'can_view_collection_codes': 1,
                'system_protected': 1,
            },
            {
                'role': 'data_steward',
                'display_name': 'Data Steward',
                'can_view_dashboard': 1,
                'can_manage_users': 0,
                'can_manage_members': 1,
                'can_manage_collections': 1,
                'can_manage_members_collections': 1,
                'can_view_reports': 1,
                'can_manage_settings': 0,
                'can_manage_roles': 0,
                'can_view_collection_codes': 1,
                'system_protected': 1,
            },
            {
                'role': 'uploader',
                'display_name': 'Uploader',
                'can_view_dashboard': 1,
                'can_manage_users': 0,
                'can_manage_members': 0,
                'can_manage_collections': 1,
                'can_manage_members_collections': 1,
                'can_view_reports': 0,
                'can_manage_settings': 0,
                'can_manage_roles': 0,
                'can_view_collection_codes': 1,
                'system_protected': 1,
            },
            {
                'role': 'viewer',
                'display_name': 'Viewer',
                'can_view_dashboard': 1,
                'can_manage_users': 0,
                'can_manage_members': 0,
                'can_manage_collections': 0,
                'can_manage_members_collections': 1,
                'can_view_reports': 1,
                'can_manage_settings': 0,
                'can_manage_roles': 0,
                'can_view_collection_codes': 1,
                'system_protected': 1,
            },
        ]
        for item in defaults:
            try:
                conn.execute(sql_insert(role_policies).values(**item))
            except Exception:
                pass
        try:
            conn.commit()
        except Exception:
            pass


def list_role_policies() -> List[dict]:
    ensure_db_exists()
    out = []
    with engine.connect() as conn:
        try:
            res = conn.execute(text('''
                SELECT id, role, display_name,
                       can_view_dashboard, can_manage_users, can_manage_members,
                       can_manage_collections, can_manage_members_collections, can_view_reports,
                       can_manage_settings, can_manage_roles, can_view_collection_codes, system_protected
                FROM role_policies
                ORDER BY role
            '''))
            for r in res.fetchall():
                out.append({
                    'id': r[0],
                    'role': r[1],
                    'display_name': r[2],
                    'can_view_dashboard': bool(r[3]),
                    'can_manage_users': bool(r[4]),
                    'can_manage_members': bool(r[5]),
                    'can_manage_collections': bool(r[6]),
                    'can_manage_members_collections': bool(r[7]),
                    'can_view_reports': bool(r[8]),
                    'can_manage_settings': bool(r[9]),
                    'can_manage_roles': bool(r[10]),
                    'can_view_collection_codes': bool(r[11]),
                    'system_protected': bool(r[12]),
                })
        except Exception:
            pass
    return out


def role_exists(role_name: str) -> bool:
    rn = str(role_name or '').strip().lower()
    if rn == '':
        return False
    with engine.connect() as conn:
        try:
            res = conn.execute(text('SELECT COUNT(*) FROM role_policies WHERE LOWER(role)=:r'), {'r': rn})
            try:
                cnt = res.scalar() or 0
            except Exception:
                row = res.fetchone()
                cnt = row[0] if row else 0
            return int(cnt) > 0
        except Exception:
            return False


def upsert_role_policy(role_name: str, display_name: Optional[str], rights: dict) -> dict:
    rn = str(role_name or '').strip().lower()
    if rn == '':
        raise ValueError('role is required')
    payload = {
        'role': rn,
        'display_name': (display_name or '').strip() or rn,
        'can_view_dashboard': 1 if rights.get('can_view_dashboard') else 0,
        'can_manage_users': 1 if rights.get('can_manage_users') else 0,
        'can_manage_members': 1 if rights.get('can_manage_members') else 0,
        'can_manage_collections': 1 if rights.get('can_manage_collections') else 0,
        'can_manage_members_collections': 1 if rights.get('can_manage_members_collections') else 0,
        'can_view_collection_codes': 1 if rights.get('can_view_collection_codes') else 0,
        'can_view_reports': 1 if rights.get('can_view_reports') else 0,
        'can_manage_settings': 1 if rights.get('can_manage_settings') else 0,
        'can_manage_roles': 1 if rights.get('can_manage_roles') else 0,
    }
    with engine.connect() as conn:
        existing = conn.execute(text('SELECT id, system_protected FROM role_policies WHERE LOWER(role)=:r'), {'r': rn}).fetchone()
        if existing:
            conn.execute(text('''
                UPDATE role_policies
                SET display_name=:display_name,
                    can_view_dashboard=:can_view_dashboard,
                    can_manage_users=:can_manage_users,
                    can_manage_members=:can_manage_members,
                    can_manage_collections=:can_manage_collections,
                    can_manage_members_collections=:can_manage_members_collections,
                    can_view_collection_codes=:can_view_collection_codes,
                    can_view_reports=:can_view_reports,
                    can_manage_settings=:can_manage_settings,
                    can_manage_roles=:can_manage_roles
                WHERE id=:id
            '''), {**payload, 'id': existing[0]})
        else:
            conn.execute(sql_insert(role_policies).values(**payload, system_protected=0))
        try:
            conn.commit()
        except Exception:
            pass
    return payload


def delete_role_policy(role_name: str) -> bool:
    rn = str(role_name or '').strip().lower()
    if rn == '':
        return False
    with engine.connect() as conn:
        row = conn.execute(text('SELECT id, system_protected FROM role_policies WHERE LOWER(role)=:r'), {'r': rn}).fetchone()
        if not row:
            return False
        if int(row[1] or 0) == 1:
            raise ValueError('Cannot delete system-protected role')
        conn.execute(text('DELETE FROM role_policies WHERE id=:id'), {'id': int(row[0])})
        try:
            conn.commit()
        except Exception:
            pass
        return True


def migrate_members_collection_only() -> dict:
    """Apply only members_collection schema/data migrations.

    This function avoids touching other tables so production updates can be scoped.
    """
    ensure_db_exists()
    inspector = inspect(engine)
    if 'members_collection' not in inspector.get_table_names():
        return {'ok': False, 'message': 'members_collection table not found', 'updated': 0}

    try:
        ensure_members_collection_schema()
    except Exception:
        pass

    updated = 0
    try:
        updated = backfill_members_collection_identifiers()
    except Exception:
        updated = 0

    try:
        deduplicate_members_collection_s1()
    except Exception:
        pass

    try:
        with engine.begin() as conn:
            conn.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ix_members_collection_s1_unique ON members_collection(s1)'))
    except Exception:
        pass

    return {'ok': True, 'table': 'members_collection', 'updated': int(updated)}


def ensure_users_schema():
    """Ensure users table has expected profile columns."""
    inspector = inspect(engine)
    if 'users' not in inspector.get_table_names():
        return
    existing = {c['name'] for c in inspector.get_columns('users')}
    expected = {
        'first_name': 'TEXT',
        'middle_name': 'TEXT',
        'last_name': 'TEXT',
        'email': 'TEXT',
        'phone': 'TEXT',
    }
    missing = [k for k in expected.keys() if k not in existing]
    for col in missing:
        stmt = f'ALTER TABLE users ADD COLUMN {col} {expected[col]}'
        with engine.connect() as conn:
            conn.execute(text(stmt))
            try:
                conn.commit()
            except Exception:
                pass


def ensure_role_policies_schema():
    """Ensure role_policies table has expected rights columns."""
    inspector = inspect(engine)
    if 'role_policies' not in inspector.get_table_names():
        return
    existing = {c['name'] for c in inspector.get_columns('role_policies')}
    expected = {
        'can_manage_members_collections': 'INTEGER',
        'can_view_collection_codes': 'INTEGER',
    }
    added_cols = []
    for col, typ in expected.items():
        if col in existing:
            continue
        stmt = f'ALTER TABLE role_policies ADD COLUMN {col} {typ} DEFAULT 0'
        with engine.connect() as conn:
            conn.execute(text(stmt))
            try:
                conn.commit()
            except Exception:
                pass
        added_cols.append(col)

    # Backfill intended defaults for built-in roles only when the column is introduced
    # on an existing installation. This avoids overriding later manual role edits.
    if 'can_manage_members_collections' in added_cols:
        with engine.connect() as conn:
            conn.execute(text('''
                UPDATE role_policies
                SET can_manage_members_collections = 1
                WHERE LOWER(role) IN ('system_admin', 'admin', 'treasurer', 'data_steward', 'uploader', 'viewer')
            '''))
            try:
                conn.commit()
            except Exception:
                pass
    
    if 'can_view_collection_codes' in added_cols:
        with engine.connect() as conn:
            conn.execute(text('''
                UPDATE role_policies
                SET can_view_collection_codes = 1
                WHERE LOWER(role) IN ('system_admin', 'admin', 'treasurer', 'data_steward', 'uploader', 'viewer')
            '''))
            try:
                conn.commit()
            except Exception:
                pass


def create_uploader(name: str, church_id: Optional[int] = None) -> str:
    """Create an uploader record and return an api_key."""
    ensure_db_exists()
    import uuid
    api_key = uuid.uuid4().hex
    with engine.connect() as conn:
        tx = conn.begin()
        try:
            conn.execute(sql_insert(uploaders).values(name=name, api_key=api_key, church=church_id))
            tx.commit()
        except Exception:
            # Ensure failed transaction is cleared before retrying on the same connection.
            try:
                tx.rollback()
            except Exception:
                pass
            api_key = uuid.uuid4().hex
            tx2 = conn.begin()
            try:
                conn.execute(sql_insert(uploaders).values(name=name, api_key=api_key, church=church_id))
                tx2.commit()
            except Exception:
                try:
                    tx2.rollback()
                except Exception:
                    pass
                raise
    return api_key


def get_uploader_by_key(api_key: str) -> Optional[dict]:
    ensure_db_exists()
    with engine.connect() as conn:
        try:
            res = conn.execute(text('SELECT id, name, api_key, church FROM uploaders WHERE api_key=:k'), {'k': api_key})
            row = res.fetchone()
            if not row:
                return None
            return {'id': row[0], 'name': row[1], 'api_key': row[2], 'church': row[3]}
        except Exception:
            return None


def list_uploaders() -> List[dict]:
    ensure_db_exists()
    out = []
    with engine.connect() as conn:
        try:
            res = conn.execute(text('SELECT id, name, api_key, church FROM uploaders'))
            for r in res.fetchall():
                out.append({'id': r[0], 'name': r[1], 'api_key': r[2], 'church': r[3]})
        except Exception:
            pass
    return out


def _hash_password(password: str, salt: bytes) -> str:
    dk = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100_000)
    return binascii.hexlify(dk).decode('ascii')


def password_policy_error(password: str) -> Optional[str]:
    p = str(password or '')
    if len(p) < 8:
        return 'Password must be at least 8 characters long'
    if not any(ch.isalpha() for ch in p):
        return 'Password must include at least one letter'
    if not any(ch.isdigit() for ch in p):
        return 'Password must include at least one number'
    if not any(not ch.isalnum() for ch in p):
        return 'Password must include at least one special character'
    return None


def create_user(
    username: str,
    password: str,
    church_id: Optional[int] = None,
    role: str = 'uploader',
    first_name: Optional[str] = None,
    middle_name: Optional[str] = None,
    last_name: Optional[str] = None,
    email: Optional[str] = None,
    phone: Optional[str] = None,
) -> dict:
    ensure_db_exists()
    uname = str(username or '').strip().lower()
    if uname == 'saypy_admin':
        church_id = None
    elif church_id is None:
        raise ValueError('User church is required for all users except saypy_admin')
    policy_msg = password_policy_error(password)
    if policy_msg:
        raise ValueError(policy_msg)
    salt = os.urandom(16)
    ph = _hash_password(password, salt)
    salt_hex = binascii.hexlify(salt).decode('ascii')
    # Atomic write: commit on success, rollback on failure.
    with engine.begin() as conn:
        conn.execute(sql_insert(users).values(
            username=username,
            first_name=first_name,
            middle_name=middle_name,
            last_name=last_name,
            email=email,
            phone=phone,
            password_hash=ph,
            salt=salt_hex,
            church=church_id,
            role=role,
        ))

    with engine.connect() as conn:
        res = conn.execute(text('SELECT id, username, first_name, middle_name, last_name, email, phone, church, role FROM users WHERE username=:u'), {'u': username})
        row = res.fetchone()
        if not row:
            raise RuntimeError('Failed to create user')
        return {
            'id': row[0],
            'username': row[1],
            'first_name': row[2],
            'middle_name': row[3],
            'last_name': row[4],
            'email': row[5],
            'phone': row[6],
            'church': row[7],
            'role': row[8],
        }


def verify_user(username: str, password: str) -> Optional[dict]:
    ensure_db_exists()
    with engine.connect() as conn:
        res = conn.execute(text('SELECT id, username, password_hash, salt, church, role, first_name, last_name FROM users WHERE username=:u'), {'u': username})
        row = res.fetchone()
        if not row:
            return None
        salt = binascii.unhexlify(row[3])
        ph = _hash_password(password, salt)
        if ph == row[2]:
            return {
                'id': row[0],
                'username': row[1],
                'church': row[4],
                'role': row[5],
                'first_name': row[6],
                'last_name': row[7],
            }
        return None


def create_token_for_user(user_id: int) -> str:
    ensure_db_exists()
    tok = uuid.uuid4().hex
    with engine.begin() as conn:
        conn.execute(sql_insert(tokens).values(token=tok, user_id=user_id))
    return tok


def get_user_by_token(token: str) -> Optional[dict]:
    ensure_db_exists()
    with engine.connect() as conn:
        res = conn.execute(text('SELECT t.created_at, u.id, u.username, u.church, u.role, u.first_name, u.last_name FROM tokens t JOIN users u ON t.user_id = u.id WHERE t.token = :tok'), {'tok': token})
        row = res.fetchone()
        if not row:
            return None
        created_at = row[0]
        try:
            # created_at may already be a datetime; if string, parse it
            if isinstance(created_at, str):
                created_dt = datetime.fromisoformat(created_at)
            else:
                created_dt = created_at
        except Exception:
            created_dt = None

        # Enforce 1-hour expiry for tokens
        if created_dt is not None:
            if datetime.utcnow() - created_dt > timedelta(hours=1):
                # token expired
                return None

        return {
            'id': row[1],
            'username': row[2],
            'church': row[3],
            'role': row[4],
            'first_name': row[5],
            'last_name': row[6],
        }


def seed_collection_codes():
    """Seed `collection_codes` with default mappings if table is empty."""
    ensure_db_exists()
    inspector = inspect(engine)
    if 'collection_codes' not in inspector.get_table_names():
        return
    # Check if already seeded
    with engine.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM collection_codes"))
        try:
            count = res.scalar()
        except Exception:
            row = res.fetchone()
            count = row[0] if row else 0
        if count and int(count) > 0:
            return

    # Mapping based on provided spec
    mapping = [
        ('s1','Sno'),
        ('s2','TAREHE'),
        ('s3','NA.'),
        ('s4','JINA'),
        ('s5','NAMBARI YA RISITI'),
        ('s6','SADAKA'),
        ('c1','ZAKA'),
        ('c2','SADAKA FIELD'),
        ('c3','SHUKRANI FIELD'),
        ('c4','M/STAR'),
        ('c5','SADAKA YA KAMBI'),
        ('c6','SADAKA YA 13'),
        ('c7','RWANDA HOSPITAL'),
        ('c8','BAJETI YA KANISA'),
        ('c9','ADRA'),
    ]
    # c10..c20 marked UNUSED
    for i in range(10,21):
        mapping.append((f'c{i}','UNUSED'))

    # s7..s13 and s8/s9 mapping
    mapping += [
        ('s7','JUMLA'),
        ('s8','JUMLA KANISANI'),
        ('s9','JUMLA MATOLEO'),
        ('s10','DETAILS2'),
        ('s11','MODE'),
        ('s12','DETAILS'),
        ('s13','SHUKRANI 100%'),
        ('s14','KANISA MAHALIA'),
    ]

    # l1..l41 mapping (labels as provided)
    l_labels = [
        'SADAKA KANISANI','MAJENGO','MAREJESHO','SHUKRANI KANISANI','AMO','VIJANA','MAKAMBI',
        'BAJETI YA KANISA','SS WATOTO','VITABU','VITI','WAHITAJI','GOLI LA KANISA','KODI YA NYUMBA',
        'UNUSED','KING\'AMUZI','MCHANGO WA KANISA','KIBIDULA','MATENDO YA HURUMA','KWAYA','WAJANE',
        'SS WAKUBWA','MKUTANO WA UFUNUO WA MATUMAINI','MAHUBIRI','10 DAYS OF PRAYERS','MEZA YA PAMOJA',
        'MSAMARIA','RAMBIRAMBI','KIWANJA CHEKECHEA','MAJENGO MWERA','UWAKILI','FAMILIA','MAWASILIANO',
        'DORCAS','UINJILISTI','SHEMASI','IBADA CHEKECHEA','MAENDELEO CHEKECHEA','UINJILIST CHEKECHEA',
        'SABATO YA WAGENI','ELIMU'
    ]
    # Ensure length 41 by padding if necessary
    for idx, label in enumerate(l_labels, start=1):
        mapping.append((f'l{idx}', label))

    # Insert mapping (all global codes with church=NULL)
    with engine.connect() as conn:
        for idx, (col, code) in enumerate(mapping, start=1):
            try:
                conn.execute(sql_insert(collection_codes).values(column_name=col, code=code, church=None))
            except Exception:
                pass
        try:
            conn.commit()
        except Exception:
            pass


def ensure_members_collection_schema():
    """Ensure the `members_collection` table has the expected columns; add missing ones via ALTER TABLE."""
    inspector = inspect(engine)
    if 'members_collection' not in inspector.get_table_names():
        return
    existing = {c['name'] for c in inspector.get_columns('members_collection')}
    expected = {}
    # s fields
    expected['s1'] = 'TEXT'
    expected['s2'] = 'DATETIME'
    expected['s3'] = 'INTEGER'
    expected['s4'] = 'TEXT'
    expected['s5'] = 'NUMERIC'
    expected['s6'] = 'NUMERIC'
    expected['s7'] = 'NUMERIC'
    expected['s8'] = 'NUMERIC'
    expected['s9'] = 'NUMERIC'
    expected['s10'] = 'TEXT'
    expected['s11'] = 'TEXT'
    expected['s12'] = 'TEXT'
    expected['s13'] = 'NUMERIC'
    # c1..c20
    for i in range(1,21):
        expected[f'c{i}'] = 'NUMERIC'
    # l1..l41
    for i in range(1,42):
        expected[f'l{i}'] = 'NUMERIC'
    # other text fields
    expected['source'] = 'TEXT'
    expected['notes'] = 'TEXT'
    expected['church'] = 'INTEGER'
    missing = [k for k in expected.keys() if k not in existing]

    # Add each missing column
    for col in missing:
        col_type = expected[col]
        # Use SQLite-friendly types if DB_ENGINE indicates sqlite
        if DB_ENGINE in ("sqlite", "sqlite3"):
            if col_type.startswith('VARCHAR'):
                sql_type = 'TEXT'
            else:
                sql_type = col_type
        else:
            sql_type = col_type

        stmt = f'ALTER TABLE members_collection ADD COLUMN {col} {sql_type}'
        with engine.connect() as conn:
            conn.execute(text(stmt))
            try:
                conn.commit()
            except Exception:
                # some dialects/engines auto-commit
                pass

    # Ensure existing columns have compatible types for current business rules.
    # SQLAlchemy metadata updates do not alter existing DB column types automatically.
    if DB_ENGINE not in ("sqlite", "sqlite3"):
        with engine.begin() as conn:
            try:
                conn.execute(text("ALTER TABLE members_collection ALTER COLUMN s1 TYPE TEXT USING CASE WHEN s1 IS NULL THEN NULL ELSE TRIM(s1::TEXT) END"))
            except Exception:
                pass
            try:
                conn.execute(text('ALTER TABLE members_collection ALTER COLUMN s3 TYPE INTEGER USING CASE WHEN s3 IS NULL THEN NULL ELSE ROUND(s3)::INTEGER END'))
            except Exception:
                pass


def insert_member(
    sno: Optional[int] = None,
    MEMBER_NAME: Optional[str] = None,
    MEMBER_ID: Optional[int] = None,
    FAMILY_ID: Optional[int] = None,
    DEFAULT_FAMILY_ID: Optional[int] = None,
    OFFICIAL_MEMBER_ID: Optional[int] = None,
    pledge: Optional[float] = None,
    GROUP_NAME: Optional[str] = None,
    GROUP_ALIAS: Optional[str] = None,
    DEFAULT_GROUP_ALIAS: Optional[str] = None,
    GROUP_LEADER_ID: Optional[int] = None,
    DEFAULT_GROUP_LEADER_ID: Optional[int] = None,
    STATUS: Optional[str] = None,
    PHONE: Optional[str] = None,
    PHONE2: Optional[str] = None,
    EMAIL: Optional[str] = None,
    RESIDENCE: Optional[str] = None,
    church: Optional[int] = None,
) -> int:
    """Insert a member and return the new `id`."""
    ensure_db_exists()
    # Ensure `sno` is unique: if missing or already present, assign next available sequence
    try:
        with engine.begin() as conn:
            if sno is None:
                res = conn.execute(text('SELECT MAX(sno) FROM members'))
                try:
                    mx = res.scalar() or 0
                except Exception:
                    row = res.fetchone()
                    mx = row[0] if row and row[0] is not None else 0
                sno = int(mx) + 1
            else:
                # check existence
                res = conn.execute(text('SELECT COUNT(*) FROM members WHERE sno = :s'), {'s': sno})
                try:
                    cnt = res.scalar() or 0
                except Exception:
                    row = res.fetchone()
                    cnt = row[0] if row else 0
                if int(cnt) > 0:
                    # assign next available sno
                    res2 = conn.execute(text('SELECT MAX(sno) FROM members'))
                    try:
                        mx2 = res2.scalar() or 0
                    except Exception:
                        row2 = res2.fetchone()
                        mx2 = row2[0] if row2 and row2[0] is not None else 0
                    sno = int(mx2) + 1

            stmt = sql_insert(members).values(
                sno=sno,
        MEMBER_NAME=MEMBER_NAME,
        MEMBER_ID=MEMBER_ID,
        FAMILY_ID=FAMILY_ID,
        DEFAULT_FAMILY_ID=DEFAULT_FAMILY_ID,
        OFFICIAL_MEMBER_ID=OFFICIAL_MEMBER_ID,
        pledge=pledge,
        GROUP_NAME=GROUP_NAME,
        GROUP_ALIAS=GROUP_ALIAS,
        DEFAULT_GROUP_ALIAS=DEFAULT_GROUP_ALIAS,
        GROUP_LEADER_ID=GROUP_LEADER_ID,
        DEFAULT_GROUP_LEADER_ID=DEFAULT_GROUP_LEADER_ID,
        STATUS=STATUS,
        PHONE=PHONE,
        PHONE2=PHONE2,
        EMAIL=EMAIL,
        RESIDENCE=RESIDENCE,
        church=church,
    )
            res = conn.execute(stmt)
            try:
                pk = res.inserted_primary_key[0]
            except Exception:
                pk = None
            return pk
    except SQLAlchemyError:
        raise


def deduplicate_members_by_sno():
    """Remove duplicate rows in `members` that share the same `sno`.
    Keeps the row with the lowest `id` for each `sno` and deletes others.
    Only considers non-null sno values.
    """
    ensure_db_exists()
    inspector = inspect(engine)
    if 'members' not in inspector.get_table_names():
        return
    with engine.connect() as conn:
        # find sno values with duplicates
        res = conn.execute(text("SELECT sno FROM members WHERE sno IS NOT NULL GROUP BY sno HAVING COUNT(*) > 1"))
        duplicates = [r[0] for r in res.fetchall()]
        deleted = 0
        for s in duplicates:
            # keep the lowest id for this sno
            keep_res = conn.execute(text('SELECT id FROM members WHERE sno = :s ORDER BY id LIMIT 1'), {'s': s})
            try:
                keep_row = keep_res.fetchone()
                keep_id = keep_row[0] if keep_row else None
            except Exception:
                keep_id = None
            if keep_id is None:
                continue
            # delete others
            conn.execute(text('DELETE FROM members WHERE sno = :s AND id != :keep'), {'s': s, 'keep': keep_id})
            try:
                conn.commit()
            except Exception:
                pass
            deleted += 1
    return deleted


def deduplicate_members_collection_s1():
    """Remove duplicate rows in `members_collection` that share the same `s1`.
    Keeps the row with the lowest `id` for each `s1` and deletes others. Only considers non-null s1 values.
    Returns number of s1 values deduplicated.
    """
    ensure_db_exists()
    inspector = inspect(engine)
    if 'members_collection' not in inspector.get_table_names():
        return 0
    with engine.connect() as conn:
        # find s1 values with duplicates
        res = conn.execute(text("SELECT s1 FROM members_collection WHERE s1 IS NOT NULL GROUP BY s1 HAVING COUNT(*) > 1"))
        duplicates = [r[0] for r in res.fetchall()]
        deleted = 0
        for s in duplicates:
            # keep the lowest id for this s1
            keep_res = conn.execute(text('SELECT id FROM members_collection WHERE s1 = :s ORDER BY id LIMIT 1'), {'s': s})
            try:
                keep_row = keep_res.fetchone()
                keep_id = keep_row[0] if keep_row else None
            except Exception:
                keep_id = None
            if keep_id is None:
                continue
            # delete others
            conn.execute(text('DELETE FROM members_collection WHERE s1 = :s AND id != :keep'), {'s': s, 'keep': keep_id})
            try:
                conn.commit()
            except Exception:
                pass
            deleted += 1
    return deleted


def insert_members_collection(collection_code: str, member_id: Optional[int] = None, church: Optional[int] = None) -> int:
    """Insert into members_collection and return the new `id`."""
    ensure_db_exists()
    stmt = sql_insert(members_collection).values(collection_code=collection_code, member_id=member_id, church=church)
    try:
        with engine.begin() as conn:
            res = conn.execute(stmt)
            try:
                pk = res.inserted_primary_key[0]
            except Exception:
                pk = None
            return pk
    except SQLAlchemyError:
        raise


def seed_churches():
    """Seed `church` table with initial values if empty."""
    ensure_db_exists()
    inspector = inspect(engine)
    if 'church' not in inspector.get_table_names():
        return
    with engine.connect() as conn:
        res = conn.execute(text('SELECT COUNT(*) FROM church'))
        try:
            cnt = res.scalar()
        except Exception:
            row = res.fetchone()
            cnt = row[0] if row else 0
        if cnt and int(cnt) > 0:
            return
        # seed values with default app_name = church name
        vals = [
            {'name': 'Kibada', 'app_name': 'Church Offerings — Admin Console'},
            {'name': 'KCC', 'app_name': 'Church Offerings — Admin Console'},
            {'name': 'Mwera', 'app_name': 'Church Offerings — Admin Console'},
            {'name': 'Goroka', 'app_name': 'Church Offerings — Admin Console'},
        ]
        for v in vals:
            try:
                conn.execute(sql_insert(church).values(**v))
            except Exception:
                pass
        try:
            conn.commit()
        except Exception:
            pass


def get_header_mappings(headers: List[str]) -> dict:
    """Return a dict header -> mapped_column for headers found in header_mappings."""
    ensure_db_exists()
    if not headers:
        return {}
    out = {}
    with engine.connect() as conn:
        for h in headers:
            try:
                res = conn.execute(text('SELECT mapped_column FROM header_mappings WHERE header_name=:h'), {'h': h})
                try:
                    mc = res.scalar()
                except Exception:
                    row = res.fetchone()
                    mc = row[0] if row else None
                if mc:
                    out[h] = mc
            except Exception:
                pass
    return out


def upsert_header_mappings(mappings: List[dict]):
    """Accept list of {header_name, mapped_column} and upsert into header_mappings."""
    ensure_db_exists()
    with engine.connect() as conn:
        for m in mappings:
            hn = m.get('header_name')
            mc = m.get('mapped_column')
            if not hn or not mc:
                continue
            # Try update first
            try:
                res = conn.execute(text('UPDATE header_mappings SET mapped_column=:mc WHERE header_name=:hn'), {'mc': mc, 'hn': hn})
                try:
                    if getattr(res, 'rowcount', 0) == 0:
                        conn.execute(sql_insert(header_mappings).values(header_name=hn, mapped_column=mc))
                except Exception:
                    # fallback insert
                    try:
                        conn.execute(sql_insert(header_mappings).values(header_name=hn, mapped_column=mc))
                    except Exception:
                        pass
            except Exception:
                try:
                    conn.execute(sql_insert(header_mappings).values(header_name=hn, mapped_column=mc))
                except Exception:
                    pass
        try:
            conn.commit()
        except Exception:
            pass

