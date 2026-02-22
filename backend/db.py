import os
import hashlib
import binascii
import uuid
from datetime import datetime, timedelta
from typing import List
import pandas as pd
from dotenv import load_dotenv

# Use SQLAlchemy to support both SQLite and Postgres via a single API
from sqlalchemy import create_engine, inspect
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime, func, text, Numeric
from sqlalchemy import insert as sql_insert
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional

load_dotenv()

BASE_DIR = os.path.dirname(__file__)

# Config: DB_ENGINE can be 'sqlite' or 'postgres' (or 'postgresql')
DB_ENGINE = os.getenv("DB_ENGINE", "sqlite").lower()

# If DATABASE_URL is set (recommended for Postgres), use it. Otherwise for sqlite build file path.
_sqlite_default_path = os.path.join(BASE_DIR, "members.db")
if DB_ENGINE in ("sqlite", "sqlite3"):
    SQLITE_PATH = os.getenv("SQLITE_PATH", _sqlite_default_path)
    DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{SQLITE_PATH}")
else:
    # For postgres: expect DATABASE_URL like 'postgresql://user:pass@host:port/dbname'
    DATABASE_URL = os.getenv("DATABASE_URL", "")
    if not DATABASE_URL:
        raise RuntimeError("DB_ENGINE set to postgres but DATABASE_URL is not provided in environment")

# Create SQLAlchemy engine. For sqlite we need connect_args to allow multithreaded access
engine_kwargs = {}
if DB_ENGINE in ("sqlite", "sqlite3"):
    engine_kwargs = {"connect_args": {"check_same_thread": False}}

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
    # Use pandas to_sql which works with SQLAlchemy engines for both sqlite and postgres
    df.to_sql(table_name, engine, if_exists="append", index=False)


# --- Table definitions and helpers ---
metadata = MetaData()

members = Table(
    'members', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('sno', Integer, primary_key=True, nullable=False),
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
)

members_collection = Table(
    'members_collection', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('collection_code', String(200), nullable=False),
    Column('member_id', Integer, ForeignKey('members.id'), nullable=True),
    Column('church', Integer, ForeignKey('church.id'), nullable=True),
    Column('s1', Integer, nullable=True),
    Column('s2', DateTime, nullable=True),
    Column('s3', Numeric, nullable=True),
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
    Column('column_name', String(100), nullable=False, unique=True),
    Column('code', String(400), nullable=True),
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


users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('username', String(150), nullable=False, unique=True),
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

def create_tables():
    """Create `members` and `members_collection` tables if they do not exist."""
    ensure_db_exists()
    metadata.create_all(engine, tables=[church, members, members_collection, collection_codes, header_mappings, uploaders, users, tokens])
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

    # Ensure a unique index on `sno` to prevent future duplicates
    try:
        with engine.connect() as conn:
            conn.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ix_members_sno_unique ON members(sno)'))
            try:
                conn.commit()
            except Exception:
                pass
    except Exception:
        pass

    try:
        seed_collection_codes()
    except Exception:
        pass
    try:
        seed_churches()
    except Exception:
        pass


def create_uploader(name: str, church_id: Optional[int] = None) -> str:
    """Create an uploader record and return an api_key."""
    ensure_db_exists()
    import uuid
    api_key = uuid.uuid4().hex
    with engine.connect() as conn:
        try:
            conn.execute(sql_insert(uploaders).values(name=name, api_key=api_key, church=church_id))
            try:
                conn.commit()
            except Exception:
                pass
        except Exception:
            # try to generate another key on collision
            api_key = uuid.uuid4().hex
            try:
                conn.execute(sql_insert(uploaders).values(name=name, api_key=api_key, church=church_id))
                try:
                    conn.commit()
                except Exception:
                    pass
            except Exception:
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


def create_user(username: str, password: str, church_id: Optional[int] = None, role: str = 'uploader') -> dict:
    ensure_db_exists()
    salt = os.urandom(16)
    ph = _hash_password(password, salt)
    salt_hex = binascii.hexlify(salt).decode('ascii')
    with engine.connect() as conn:
        conn.execute(sql_insert(users).values(username=username, password_hash=ph, salt=salt_hex, church=church_id, role=role))
        try:
            conn.commit()
        except Exception:
            pass
        res = conn.execute(text('SELECT id, username, church, role FROM users WHERE username=:u'), {'u': username})
        row = res.fetchone()
        if not row:
            raise RuntimeError('Failed to create user')
        return {'id': row[0], 'username': row[1], 'church': row[2], 'role': row[3]}


def verify_user(username: str, password: str) -> Optional[dict]:
    ensure_db_exists()
    with engine.connect() as conn:
        res = conn.execute(text('SELECT id, username, password_hash, salt, church, role FROM users WHERE username=:u'), {'u': username})
        row = res.fetchone()
        if not row:
            return None
        salt = binascii.unhexlify(row[3])
        ph = _hash_password(password, salt)
        if ph == row[2]:
            return {'id': row[0], 'username': row[1], 'church': row[4], 'role': row[5]}
        return None


def create_token_for_user(user_id: int) -> str:
    ensure_db_exists()
    tok = uuid.uuid4().hex
    with engine.connect() as conn:
        conn.execute(sql_insert(tokens).values(token=tok, user_id=user_id))
        try:
            conn.commit()
        except Exception:
            pass
    return tok


def get_user_by_token(token: str) -> Optional[dict]:
    ensure_db_exists()
    with engine.connect() as conn:
        res = conn.execute(text('SELECT t.created_at, u.id, u.username, u.church, u.role FROM tokens t JOIN users u ON t.user_id = u.id WHERE t.token = :tok'), {'tok': token})
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

        return {'id': row[1], 'username': row[2], 'church': row[3], 'role': row[4]}


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

    # Insert mapping
    with engine.connect() as conn:
        for idx, (col, code) in enumerate(mapping, start=1):
            try:
                conn.execute(sql_insert(collection_codes).values(column_name=col, code=code))
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
    expected['s1'] = 'INTEGER'
    expected['s2'] = 'DATETIME'
    expected['s3'] = 'NUMERIC'
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
    if not missing:
        return

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


def insert_members_collection(collection_code: str, member_id: Optional[int] = None, church: Optional[int] = None) -> int:
    """Insert into members_collection and return the new `id`."""
    ensure_db_exists()
    stmt = sql_insert(members_collection).values(collection_code=collection_code, member_id=member_id, church=church)
    try:
        with engine.connect() as conn:
            res = conn.execute(stmt)
            conn.commit()
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
        # seed values
        vals = [
            {'name': 'Kibada'},
            {'name': 'KCC'},
            {'name': 'Mwera'},
            {'name': 'Goroka'},
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

