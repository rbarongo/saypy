from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Depends
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import os
import binascii
import pandas as pd
from .reports import compute_period_summary
from .db import (
    get_target_columns,
    insert_dataframe,
    get_sqlite_path,
    engine,
    create_tables,
    insert_member,
    insert_members_collection,
    get_header_mappings,
    upsert_header_mappings,
    create_uploader,
    get_uploader_by_key,
    list_uploaders,
    create_user,
    verify_user,
    _hash_password,
    password_policy_error,
    create_token_for_user,
    get_user_by_token,
    normalize_members_collection_rows,
    compute_members_collection_s1,
    list_role_policies,
    role_exists,
    upsert_role_policy,
    delete_role_policy,
    import_collection_codes_from_workbook,
    ensure_global_config_schema,
    ensure_members_collection_schema,
    ensure_members_schema,
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import inspect, text
from typing import List, Optional
from pydantic import BaseModel, ValidationError
from decimal import Decimal
from datetime import datetime
import numpy as np
import math
import logging
import io
import json

app = FastAPI(title="KSC Migration API")
logger = logging.getLogger(__name__)

bearer_scheme = HTTPBearer(auto_error=False)

ROLE_SYSTEM_ADMIN = 'system_admin'
ROLE_ADMIN = 'admin'
ROLE_TREASURER = 'treasurer'
ROLE_HEAD_TREASURER = 'head_treasurer'
ROLE_PASTOR = 'pastor'
ROLE_ELDER = 'elder'
ROLE_DATA_STEWARD = 'data_steward'
ROLE_UPLOADER = 'uploader'
ROLE_VIEWER = 'viewer'
ALL_ROLES = {ROLE_SYSTEM_ADMIN, ROLE_ADMIN, ROLE_TREASURER, ROLE_HEAD_TREASURER, ROLE_PASTOR, ROLE_ELDER, ROLE_DATA_STEWARD, ROLE_UPLOADER, ROLE_VIEWER}

MEMBER_STATUS_ACTIVE = 'active'
MEMBER_STATUS_INACTIVE_TRANSFER = 'inactive by transfer'
MEMBER_STATUS_INACTIVE_REMOVAL = 'inactive by removal'
MEMBER_STATUS_INACTIVE_DEATH = 'inactive by death'
MEMBER_STATUS_INACTIVE_UNKNOWN = 'inactive unknown reason'
MEMBER_STATUSES = {
    MEMBER_STATUS_ACTIVE,
    MEMBER_STATUS_INACTIVE_TRANSFER,
    MEMBER_STATUS_INACTIVE_REMOVAL,
    MEMBER_STATUS_INACTIVE_DEATH,
    MEMBER_STATUS_INACTIVE_UNKNOWN,
}


def _normalize_member_status(value: Optional[str]) -> Optional[str]:
    raw = str(value or '').strip().lower()
    if raw == '':
        return None
    aliases = {
        'active': MEMBER_STATUS_ACTIVE,
        'inactive by transfer': MEMBER_STATUS_INACTIVE_TRANSFER,
        'inactive transfer': MEMBER_STATUS_INACTIVE_TRANSFER,
        'transferred': MEMBER_STATUS_INACTIVE_TRANSFER,
        'inactive by removal': MEMBER_STATUS_INACTIVE_REMOVAL,
        'removed': MEMBER_STATUS_INACTIVE_REMOVAL,
        'inactive by death': MEMBER_STATUS_INACTIVE_DEATH,
        'dead': MEMBER_STATUS_INACTIVE_DEATH,
        'deceased': MEMBER_STATUS_INACTIVE_DEATH,
        'inactive unknown reason': MEMBER_STATUS_INACTIVE_UNKNOWN,
        'inactive unknown': MEMBER_STATUS_INACTIVE_UNKNOWN,
        'unknown': MEMBER_STATUS_INACTIVE_UNKNOWN,
    }
    normalized = aliases.get(raw, raw)
    if normalized not in MEMBER_STATUSES:
        allowed = ', '.join(sorted(MEMBER_STATUSES))
        raise HTTPException(status_code=422, detail=f'Invalid member status. Allowed values: {allowed}')
    return normalized


def _normalize_phone_text(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if s == '' or s.lower() in {'nan', 'none', 'null'}:
        return None
    if s.endswith('.0'):
        s = s[:-2]
    return s


def _normalize_member_transfer_fields(status_value: Optional[str], transfer_to_church: Optional[int], transfer_date: Optional[datetime]):
    """Apply status-specific transfer rules.

    - transfer_to_church must be provided when status is 'inactive by transfer'
    - transfer_to_church/transfer_date are cleared for other statuses
    """
    if status_value == MEMBER_STATUS_INACTIVE_TRANSFER:
        if transfer_to_church is None:
            raise HTTPException(
                status_code=422,
                detail='TRANSFER_TO_CHURCH is required when member status is "inactive by transfer"',
            )
        return transfer_to_church, transfer_date
    return None, None


def _normalize_user_context(user: Optional[dict]) -> Optional[dict]:
    if not isinstance(user, dict):
        return user
    u = dict(user)
    uname = str(u.get('username') or '').strip().lower()
    role = str(u.get('role') or ROLE_UPLOADER).strip().lower()
    if uname == 'saypy_admin':
        u['role'] = ROLE_SYSTEM_ADMIN
        u['church'] = None
        return u
    if not role_exists(role):
        u['role'] = ROLE_UPLOADER
    else:
        u['role'] = role
    return u


def _can_manage_roles(user: Optional[dict]) -> bool:
    return _is_system_admin_user(user)


def _is_system_admin_user(user: Optional[dict]) -> bool:
    if not isinstance(user, dict):
        return False
    if str(user.get('role') or '').lower() == ROLE_SYSTEM_ADMIN:
        return True
    return str(user.get('username') or '').strip().lower() == 'saypy_admin'


def _is_admin_user(user: Optional[dict]) -> bool:
    if not isinstance(user, dict):
        return False
    role = str(user.get('role') or '').lower()
    if role in {ROLE_SYSTEM_ADMIN, ROLE_ADMIN}:
        return True
    return str(user.get('username') or '').strip().lower() == 'saypy_admin'


def _auth_role(auth: dict) -> Optional[str]:
    if not isinstance(auth, dict):
        return None
    u = auth.get('user') if isinstance(auth.get('user'), dict) else None
    if u is not None:
        return str(u.get('role') or '').lower()
    up = auth.get('uploader') if isinstance(auth.get('uploader'), dict) else None
    if up is not None:
        return ROLE_UPLOADER
    return None


def _require_auth_roles(auth: dict, allowed_roles: set, context: str = 'this operation'):
    role = _auth_role(auth)
    if role in allowed_roles:
        return
    raise HTTPException(status_code=403, detail=f'Not authorized for {context}')


def _role_has_right(role: Optional[str], right_flag: str) -> bool:
    r = str(role or '').strip().lower()
    if r == ROLE_SYSTEM_ADMIN:
        return True
    # Backward-compatible fallback if policies are unavailable.
    fallback = {
        'can_manage_members': {ROLE_ADMIN, ROLE_DATA_STEWARD},
        'can_manage_collections': {ROLE_ADMIN, ROLE_TREASURER, ROLE_HEAD_TREASURER, ROLE_UPLOADER},
        'can_manage_members_collections': {ROLE_ADMIN, ROLE_TREASURER, ROLE_HEAD_TREASURER, ROLE_DATA_STEWARD, ROLE_UPLOADER, ROLE_VIEWER},
        'can_view_collection_codes': {ROLE_ADMIN, ROLE_TREASURER, ROLE_HEAD_TREASURER, ROLE_UPLOADER, ROLE_VIEWER},
        'can_view_reports': {ROLE_ADMIN, ROLE_TREASURER, ROLE_HEAD_TREASURER, ROLE_PASTOR, ROLE_ELDER, ROLE_DATA_STEWARD, ROLE_VIEWER},
        'can_delete_members_collections': {ROLE_ADMIN, ROLE_HEAD_TREASURER},
        'can_unlock_members_collections': {ROLE_ADMIN, ROLE_HEAD_TREASURER},
    }
    try:
        for rp in list_role_policies():
            if str(rp.get('role') or '').strip().lower() == r:
                if right_flag in rp:
                    if bool(rp.get(right_flag)):
                        return True
                    # Compatibility guard: if a built-in protected role row was migrated
                    # with an incorrect 0 for a known default right, keep expected access.
                    if bool(rp.get('system_protected')) and r in fallback.get(right_flag, set()):
                        return True
                    return False
                break
    except Exception:
        pass
    return r in fallback.get(right_flag, set())


def _require_auth_right(auth: dict, right_flag: str, context: str = 'this operation'):
    role = _auth_role(auth)
    if _role_has_right(role, right_flag):
        return
    raise HTTPException(status_code=403, detail=f'Not authorized for {context}')


def _require_auth_any_right(auth: dict, right_flags: set, context: str = 'this operation'):
    role = _auth_role(auth)
    for flag in right_flags:
        if _role_has_right(role, flag):
            return
    raise HTTPException(status_code=403, detail=f'Not authorized for {context}')


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing or invalid authorization token")
    token = credentials.credentials
    user = get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")
    return _normalize_user_context(user)


def require_api_key_or_user(request: Request, credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    """Allow either an X-API-KEY uploader key or a Bearer user token. Return a dict with keys: api_key, uploader, user."""
    api_key = None
    uploader = None
    user = None
    try:
        api_key = request.headers.get('x-api-key') or request.headers.get('X-API-KEY')
    except Exception:
        api_key = None

    if api_key:
        uploader = get_uploader_by_key(api_key)
        if not uploader:
            raise HTTPException(status_code=401, detail='Invalid API key')
        return {'api_key': api_key, 'uploader': uploader, 'user': None}

    # try bearer token
    if credentials and credentials.credentials:
        token = credentials.credentials
        user = get_user_by_token(token)
        if not user:
            raise HTTPException(status_code=401, detail='Invalid token')
        return {'api_key': None, 'uploader': None, 'user': _normalize_user_context(user)}

    raise HTTPException(status_code=401, detail='Missing authentication (API key or Bearer token)')


def _resolve_church_id(value) -> Optional[int]:
    """Resolve church input (id or name) into a church id."""
    if value is None or value == '':
        return None
    if isinstance(value, int):
        return int(value)
    try:
        s = str(value).strip()
        if s == '':
            return None
        if s.isdigit():
            return int(s)
    except Exception:
        pass
    try:
        with engine.connect() as conn:
            res = conn.execute(text('SELECT id FROM church WHERE name=:n'), {'n': str(value)})
            try:
                cid = res.scalar()
            except Exception:
                row = res.fetchone()
                cid = row[0] if row else None
        return int(cid) if cid is not None else None
    except Exception:
        return None


def _auth_context_church(auth: dict) -> Optional[int]:
    if not isinstance(auth, dict):
        return None
    u = auth.get('user') if isinstance(auth.get('user'), dict) else None
    up = auth.get('uploader') if isinstance(auth.get('uploader'), dict) else None
    if _is_system_admin_user(u):
        return None
    if u and u.get('church') is not None:
        return _resolve_church_id(u.get('church'))
    if up and up.get('church') is not None:
        return _resolve_church_id(up.get('church'))
    return None


def _enforce_church_scope(target_church, actor_church: Optional[int], context: str = 'requested data') -> Optional[int]:
    """Return the effective church id, or raise 403 if cross-church access is attempted."""
    t = _resolve_church_id(target_church)
    if actor_church is None:
        return t
    if t is None:
        return int(actor_church)
    if int(t) != int(actor_church):
        raise HTTPException(status_code=403, detail=f"Restricted information: your account cannot access {context} for another church")
    return int(actor_church)


def _ensure_collection_workflow_tables():
    """Create workflow/audit tables used for delete approvals and collection action logs."""
    with engine.begin() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS collection_action_logs (
                id INTEGER PRIMARY KEY,
                action VARCHAR(80) NOT NULL,
                row_id INTEGER NULL,
                church INTEGER NULL,
                actor_user_id INTEGER NULL,
                actor_username VARCHAR(200) NULL,
                actor_role VARCHAR(80) NULL,
                details TEXT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS members_collection_delete_requests (
                id INTEGER PRIMARY KEY,
                row_id INTEGER NOT NULL,
                church INTEGER NULL,
                requested_by_user_id INTEGER NULL,
                requested_by_username VARCHAR(200) NULL,
                requested_by_role VARCHAR(80) NULL,
                reason TEXT NULL,
                status VARCHAR(40) NOT NULL DEFAULT 'pending',
                approved_by_user_id INTEGER NULL,
                approved_by_username VARCHAR(200) NULL,
                approved_by_role VARCHAR(80) NULL,
                decision_note TEXT NULL,
                row_snapshot TEXT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                decided_at DATETIME NULL
            )
        '''))


def _safe_json_text(value) -> str:
    try:
        return json.dumps(value, ensure_ascii=True, default=str)
    except Exception:
        return '{}'


def _auth_actor_identity(auth: dict) -> dict:
    role = _auth_role(auth)
    user_obj = auth.get('user') if isinstance(auth, dict) and isinstance(auth.get('user'), dict) else None
    up_obj = auth.get('uploader') if isinstance(auth, dict) and isinstance(auth.get('uploader'), dict) else None
    if user_obj:
        return {
            'user_id': user_obj.get('id'),
            'username': user_obj.get('username'),
            'role': role,
        }
    if up_obj:
        return {
            'user_id': None,
            'username': up_obj.get('name'),
            'role': role,
        }
    return {'user_id': None, 'username': None, 'role': role}


def _log_collection_action(action: str, row_id: Optional[int], church: Optional[int], auth: dict, details: Optional[dict] = None):
    """Write auditable action entries for collection workflow operations."""
    actor = _auth_actor_identity(auth)
    try:
        with engine.begin() as conn:
            conn.execute(
                text('''
                    INSERT INTO collection_action_logs
                    (action, row_id, church, actor_user_id, actor_username, actor_role, details)
                    VALUES (:a, :rid, :ch, :uid, :un, :role, :d)
                '''),
                {
                    'a': action,
                    'rid': row_id,
                    'ch': church,
                    'uid': actor.get('user_id'),
                    'un': actor.get('username'),
                    'role': actor.get('role'),
                    'd': _safe_json_text(details or {}),
                }
            )
    except Exception:
        logger.exception('Failed to write collection action log')

app.add_middleware(
    CORSMiddleware,
    # Allow all origins for local development to avoid CORS issues from different localhost variants
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _read_upload_file(file: UploadFile):
    name = file.filename or "upload"
    try:
        try:
            file.file.seek(0)
        except Exception:
            pass

        raw = file.file.read()
        if raw is None:
            raise ValueError("Upload was empty")
        if isinstance(raw, str):
            raw = raw.encode("utf-8")

        if name.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(io.BytesIO(raw))
        elif name.lower().endswith('.csv') or file.content_type == 'text/csv':
            df = pd.read_csv(io.BytesIO(raw))
        else:
            # try excel first
            try:
                df = pd.read_excel(io.BytesIO(raw))
            except Exception:
                df = pd.read_csv(io.BytesIO(raw))
        return df
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse upload: {e}")


def _serializable_value(v):
    """Convert pandas/numpy/decimal/datetime values to JSON-serializable Python types."""
    try:
        if v is None:
            return None
        # normalize NaN/Inf floats
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                return None
        # pandas Timestamp
        if hasattr(v, 'to_pydatetime'):
            try:
                dt = v.to_pydatetime()
                return dt.isoformat()
            except Exception:
                pass
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, Decimal):
            try:
                iv = int(v)
                if iv == v:
                    return iv
            except Exception:
                pass
            return float(v)
        if isinstance(v, np.generic):
            try:
                return v.item()
            except Exception:
                return float(v)
        # basic python types are fine
        if isinstance(v, (str, int, float, bool)):
            # guard again for float NaN/Inf
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            return v
        # fallback to string
        return str(v)
    except Exception:
        return str(v)


def _guess_s1_column(df):
    """Guess which dataframe column holds the serial number (s1/sno).
    Heuristics: look for header names containing 's1', 'sno', 'serial', 's.no', or 'sno.' (case-insensitive).
    Fallback: use the first column name.
    """
    if df is None or df.shape[1] == 0:
        return None
    for c in df.columns:
        lc = str(c).lower()
        if 's1' in lc or 'sno' in lc or 'serial' in lc or 's.no' in lc or 'sr#' in lc or 's no' in lc:
            return c
    # fallback to first column
    return df.columns[0]


@app.post('/upload')
async def upload(batch: UploadFile = File(...), auth: dict = Depends(require_api_key_or_user), request: Request = None):
    _require_auth_right(auth, 'can_manage_collections', context='collection upload')
    df = _read_upload_file(batch)
    if df is None:
        raise HTTPException(status_code=400, detail="No data parsed from file")

    target_cols = get_target_columns()
    if not target_cols:
        raise HTTPException(status_code=500, detail="members_collection table not found in SQLite. Run migration first.")

    # Case-insensitive column mapping: map df columns to target columns by lowercase matching
    df_cols_map = {c.lower(): c for c in df.columns}
    mapped = {}
    for tc in target_cols:
        lc = tc.lower()
        if lc in df_cols_map:
            mapped[tc] = df[df_cols_map[lc]]
        else:
            mapped[tc] = pd.Series([None] * len(df))

    out_df = pd.DataFrame(mapped)
    # If an uploader API key is provided, use uploader's church and source
    uploader = auth.get('uploader') if isinstance(auth, dict) else None
    actor_church = _auth_context_church(auth)
    # if uploader provided, apply defaults
    if uploader:
        try:
            if uploader.get('church'):
                out_df['church'] = out_df.get('church', pd.Series([uploader.get('church')] * len(out_df)))
            if uploader.get('name'):
                out_df['source'] = out_df.get('source', pd.Series([uploader.get('name')] * len(out_df)))
        except Exception:
            pass
    if actor_church is not None:
        out_df['church'] = actor_church
    # Filter rows: only keep rows where the guessed S1/serial column has a non-empty value
    s1_col = _guess_s1_column(out_df)
    if s1_col is not None:
        try:
            mask = out_df[s1_col].notna() & (out_df[s1_col].astype(str).str.strip() != '')
            out_df = out_df[mask]
        except Exception:
            # if anything goes wrong with filtering, fall back to original df
            pass

    insert_dataframe(out_df)
    return {"inserted": len(out_df), "table": "members_collection"}


@app.post('/upload/headers')
async def upload_headers(batch: UploadFile = File(...), auth: dict = Depends(require_api_key_or_user), request: Request = None):
    """Receive an uploaded Excel/CSV and return headers and first 5 rows for preview without inserting."""
    _require_auth_right(auth, 'can_manage_collections', context='collection upload preview')
    df = _read_upload_file(batch)
    if df is None:
        raise HTTPException(status_code=400, detail="No data parsed from file")
    headers = list(df.columns)
    # Provide both the full dataset and a filtered preview (rows with guessed S1 non-empty)
    s1_col = _guess_s1_column(df)
    df_filtered = df
    if s1_col is not None:
        try:
            mask = df[s1_col].notna() & (df[s1_col].astype(str).str.strip() != '')
            df_filtered = df[mask]
        except Exception:
            df_filtered = df

    full_preview = df.fillna('').to_dict(orient='records')
    # Return the filtered dataset (filled) so the client can preview and edit all rows by default
    preview = df_filtered.fillna('').to_dict(orient='records')
    # fetch previous mappings for these headers and suggest mapped columns
    try:
        suggestions = get_header_mappings(headers)
    except Exception:
        suggestions = {}
    # If API key present, return uploader info so frontend can preselect church/uploader
    uploader = auth.get('uploader') if isinstance(auth, dict) else None

    return {"headers": headers, "full_preview": full_preview, "preview": preview, "suggestions": suggestions, "s1_column": s1_col, "preview_count": len(preview), "uploader": uploader}


@app.post('/submit/{table_name}')
async def submit_form(table_name: str, request: Request, auth: dict = Depends(require_api_key_or_user)):
    """Accept form-encoded or JSON submissions and insert into the named table.

    Allowed tables: `collection_codes`, `members_collections`, `members`.
    The endpoint will create the table if it does not exist and append the submitted row.
    """
    # accept both singular and plural table names for backward compatibility
    allowed = {"collection_codes", "members_collection", "members_collections", "members"}
    if table_name not in allowed:
        raise HTTPException(status_code=400, detail=f"Table not allowed: {table_name}")

    # Accept JSON or form data
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/json"):
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    if not isinstance(payload, dict) or not payload:
        raise HTTPException(status_code=400, detail="No data provided in request body or form")

    # Enforce table-specific rights to keep menu domains independent.
    if table_name in {"members_collection", "members_collections"}:
        _require_auth_right(auth, 'can_manage_members_collections', context='members collections submission')
    elif table_name == "members":
        _require_auth_right(auth, 'can_manage_members', context='member submission')
    elif table_name == "collection_codes":
        _require_auth_right(auth, 'can_manage_collections', context='collection code submission')

    # Normalize: convert single-value lists to values
    row = {k: (v[0] if isinstance(v, (list, tuple)) and len(v) == 1 else v) for k, v in payload.items()}
    actor_church = _auth_context_church(auth)
    if table_name in {"members_collection", "members_collections", "members"}:
        row['church'] = _enforce_church_scope(row.get('church'), actor_church, context=f'{table_name} submission')
    df = pd.DataFrame([row])
    insert_dataframe(df, table_name=table_name)
    return {"inserted": 1, "table": table_name}


class MemberIn(BaseModel):
    sno: Optional[int] = None
    MEMBER_NAME: Optional[str] = None
    MEMBER_ID: Optional[int] = None
    FAMILY_ID: Optional[int] = None
    DEFAULT_FAMILY_ID: Optional[int] = None
    OFFICIAL_MEMBER_ID: Optional[int] = None
    pledge: Optional[float] = None
    GROUP_NAME: Optional[str] = None
    GROUP_ALIAS: Optional[str] = None
    DEFAULT_GROUP_ALIAS: Optional[str] = None
    GROUP_LEADER_ID: Optional[int] = None
    DEFAULT_GROUP_LEADER_ID: Optional[int] = None
    STATUS: Optional[str] = None
    TRANSFER_TO_CHURCH: Optional[int] = None
    TRANSFER_DATE: Optional[datetime] = None
    PHONE: Optional[str] = None
    PHONE2: Optional[str] = None
    EMAIL: Optional[str] = None
    RESIDENCE: Optional[str] = None
    church: Optional[int] = None


class MemberCollectionIn(BaseModel):
    collection_code: str
    member_id: int | None = None


@app.post('/members')
def create_member(payload: MemberIn, auth: dict = Depends(require_api_key_or_user)):
    """Create a member record and return its id."""
    _require_auth_right(auth, 'can_manage_members', context='member creation')
    ensure_members_schema()
    actor_church = _auth_context_church(auth)
    effective_church = _enforce_church_scope(payload.church, actor_church, context='member creation')
    normalized_status = _normalize_member_status(payload.STATUS)
    transfer_to_church, transfer_date = _normalize_member_transfer_fields(
        normalized_status,
        payload.TRANSFER_TO_CHURCH,
        payload.TRANSFER_DATE,
    )
    pk = insert_member(
        sno=payload.sno,
        MEMBER_NAME=payload.MEMBER_NAME,
        MEMBER_ID=payload.MEMBER_ID,
        FAMILY_ID=payload.FAMILY_ID,
        DEFAULT_FAMILY_ID=payload.DEFAULT_FAMILY_ID,
        OFFICIAL_MEMBER_ID=payload.OFFICIAL_MEMBER_ID,
        pledge=payload.pledge,
        GROUP_NAME=payload.GROUP_NAME,
        GROUP_ALIAS=payload.GROUP_ALIAS,
        DEFAULT_GROUP_ALIAS=payload.DEFAULT_GROUP_ALIAS,
        GROUP_LEADER_ID=payload.GROUP_LEADER_ID,
        DEFAULT_GROUP_LEADER_ID=payload.DEFAULT_GROUP_LEADER_ID,
        STATUS=normalized_status,
        TRANSFER_TO_CHURCH=transfer_to_church,
        TRANSFER_DATE=transfer_date,
        STATUS_UPDATED_AT=datetime.utcnow() if normalized_status else None,
        PHONE=_normalize_phone_text(payload.PHONE),
        PHONE2=_normalize_phone_text(payload.PHONE2),
        EMAIL=payload.EMAIL,
        RESIDENCE=payload.RESIDENCE,
        church=effective_church,
    )
    return {"id": pk}


@app.post('/members_collection')
def create_members_collection(payload: MemberCollectionIn, auth: dict = Depends(require_api_key_or_user)):
    """Create a members_collection row. `member_id` may be omitted if you will link later."""
    _require_auth_right(auth, 'can_manage_members_collections', context='members collections creation')
    actor_church = _auth_context_church(auth)
    if actor_church is not None:
        raise HTTPException(status_code=400, detail='Use /members_collections/bulk for church-scoped inserts')
    pk = insert_members_collection(collection_code=payload.collection_code, member_id=payload.member_id)
    return {"id": pk}

@app.put('/members_collection/{row_id}')
def update_members_collection(row_id: int, payload: dict, auth: dict = Depends(require_api_key_or_user)):
    """Update a members_collection row by id. Accepts any subset of columns present in the table."""
    try:
        _require_auth_right(auth, 'can_manage_members_collections', context='collection update')
        # limit updates to known columns to avoid SQL injection
        cols = get_target_columns('members_collection')
        if not cols:
            raise HTTPException(status_code=500, detail='members_collection table not found')
        update_cols = {k: v for k, v in payload.items() if k in cols and k != 'id'}
        if not update_cols:
            raise HTTPException(status_code=400, detail='No updatable columns provided')

        actor_church = _auth_context_church(auth)
        actor_role = _auth_role(auth)
        can_unlock = _role_has_right(actor_role, 'can_unlock_members_collections')
        existing_row = None
        if actor_church is not None:
            with engine.connect() as conn:
                existing_row = conn.execute(text('SELECT church, is_locked FROM members_collection WHERE id=:id'), {'id': row_id}).fetchone()
            if not existing_row:
                raise HTTPException(status_code=404, detail='members_collection row not found')
            _enforce_church_scope(existing_row[0], actor_church, context='this members_collection record')
            if int(existing_row[1] or 0) == 1 and not can_unlock:
                raise HTTPException(status_code=403, detail='This record is locked. Request unlock from head treasurer.')
            update_cols['church'] = _enforce_church_scope(update_cols.get('church'), actor_church, context='members_collection update')
        else:
            with engine.connect() as conn:
                existing_row = conn.execute(text('SELECT church, is_locked FROM members_collection WHERE id=:id'), {'id': row_id}).fetchone()
            if not existing_row:
                raise HTTPException(status_code=404, detail='members_collection row not found')
            if int(existing_row[1] or 0) == 1 and not can_unlock:
                raise HTTPException(status_code=403, detail='This record is locked. Unlock is required before editing.')

        if any(k in update_cols for k in ('s2', 's3', 'church', 's1')):
            with engine.connect() as conn:
                cur = conn.execute(text('SELECT s2, s3, church FROM members_collection WHERE id=:id'), {'id': row_id}).fetchone()
            if cur:
                s2_val = update_cols.get('s2', cur[0])
                s3_val = update_cols.get('s3', cur[1])
                church_val = update_cols.get('church', cur[2])
                s1_val = compute_members_collection_s1(s2_val, church_val, s3_val)
                if s1_val is not None:
                    update_cols['s1'] = str(s1_val)

        set_parts = ', '.join([f"{c}=:{c}" for c in update_cols.keys()])
        params = dict(update_cols)
        params['id'] = row_id
        with engine.connect() as conn:
            conn.execute(text(f"UPDATE members_collection SET {set_parts} WHERE id=:id"), params)
            try:
                conn.commit()
            except Exception:
                pass
        _log_collection_action('members_collection_update', row_id, update_cols.get('church') or (existing_row[0] if existing_row else None), auth, {'updated_fields': sorted(list(update_cols.keys()))})
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/members_collection/{row_id}/unlock')
def unlock_members_collection(row_id: int, auth: dict = Depends(require_api_key_or_user)):
    """Unlock a submitted members_collection row for editing (head treasurer/admin roles)."""
    _require_auth_right(auth, 'can_unlock_members_collections', context='members collection unlock')
    actor_church = _auth_context_church(auth)
    actor = _auth_actor_identity(auth)
    try:
        with engine.begin() as conn:
            row = conn.execute(text('SELECT id, church, is_locked FROM members_collection WHERE id=:id'), {'id': row_id}).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail='members_collection row not found')
            church_id = row[1]
            _enforce_church_scope(church_id, actor_church, context='members_collection unlock')
            if int(row[2] or 0) == 0:
                return {'ok': True, 'already_unlocked': True}
            conn.execute(
                text('''
                    UPDATE members_collection
                    SET is_locked=0, unlocked_at=:ua, unlocked_by_user_id=:uid
                    WHERE id=:id
                '''),
                {'ua': datetime.utcnow(), 'uid': actor.get('user_id'), 'id': row_id}
            )
        _log_collection_action('members_collection_unlock', row_id, church_id, auth, {'unlocked': True})
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/members_collection/{row_id}/delete_request')
async def request_members_collection_delete(row_id: int, request: Request, auth: dict = Depends(require_api_key_or_user)):
    """Create a deletion request; uploader requests require head treasurer approval."""
    _require_auth_right(auth, 'can_manage_members_collections', context='members collection delete request')
    actor_church = _auth_context_church(auth)
    actor = _auth_actor_identity(auth)
    body = {}
    try:
        body = await request.json()
    except Exception:
        body = {}
    reason = str((body or {}).get('reason') or '').strip() or 'No reason provided'
    try:
        with engine.begin() as conn:
            row = conn.execute(text('SELECT * FROM members_collection WHERE id=:id'), {'id': row_id}).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail='members_collection row not found')
            record = dict(row._mapping)
            church_id = record.get('church')
            _enforce_church_scope(church_id, actor_church, context='members_collection delete request')
            existing_pending = conn.execute(
                text("SELECT id FROM members_collection_delete_requests WHERE row_id=:rid AND status='pending' ORDER BY id DESC LIMIT 1"),
                {'rid': row_id}
            ).fetchone()
            if existing_pending:
                return {'ok': True, 'request_id': int(existing_pending[0]), 'already_pending': True}
            ins = conn.execute(
                text('''
                    INSERT INTO members_collection_delete_requests
                    (row_id, church, requested_by_user_id, requested_by_username, requested_by_role, reason, status, row_snapshot)
                    VALUES (:rid, :ch, :uid, :un, :role, :reason, 'pending', :snap)
                '''),
                {
                    'rid': row_id,
                    'ch': church_id,
                    'uid': actor.get('user_id'),
                    'un': actor.get('username'),
                    'role': actor.get('role'),
                    'reason': reason,
                    'snap': _safe_json_text(record),
                }
            )
            request_id = None
            try:
                request_id = int(ins.lastrowid)
            except Exception:
                created = conn.execute(text("SELECT id FROM members_collection_delete_requests WHERE row_id=:rid AND status='pending' ORDER BY id DESC LIMIT 1"), {'rid': row_id}).fetchone()
                request_id = int(created[0]) if created else None
        _log_collection_action('members_collection_delete_requested', row_id, church_id, auth, {'request_id': request_id, 'reason': reason})
        return {'ok': True, 'request_id': request_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/members_collection/delete_requests')
def list_members_collection_delete_requests(status: Optional[str] = 'pending', auth: dict = Depends(require_api_key_or_user)):
    """List delete requests for review/auditing."""
    _require_auth_any_right(auth, {'can_delete_members_collections', 'can_unlock_members_collections'}, context='delete request review')
    actor_church = _auth_context_church(auth)
    try:
        sql = 'SELECT * FROM members_collection_delete_requests WHERE 1=1'
        params = {}
        if status:
            sql += ' AND LOWER(status)=:st'
            params['st'] = str(status).strip().lower()
        if actor_church is not None:
            sql += ' AND church=:ch'
            params['ch'] = actor_church
        sql += ' ORDER BY id DESC'
        with engine.connect() as conn:
            rows = [dict(r._mapping) for r in conn.execute(text(sql), params).fetchall()]
        return [{k: _serializable_value(v) for k, v in r.items()} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/members_collection/delete_requests/{request_id}/approve')
async def approve_members_collection_delete_request(request_id: int, request: Request, auth: dict = Depends(require_api_key_or_user)):
    """Approve and execute a pending deletion request (head treasurer/admin roles)."""
    _require_auth_right(auth, 'can_delete_members_collections', context='delete approval')
    actor_church = _auth_context_church(auth)
    actor = _auth_actor_identity(auth)
    body = {}
    try:
        body = await request.json()
    except Exception:
        body = {}
    decision_note = str((body or {}).get('decision_note') or '').strip()
    try:
        with engine.begin() as conn:
            req = conn.execute(text('SELECT * FROM members_collection_delete_requests WHERE id=:id'), {'id': request_id}).fetchone()
            if not req:
                raise HTTPException(status_code=404, detail='Delete request not found')
            req_map = dict(req._mapping)
            if str(req_map.get('status') or '').lower() != 'pending':
                raise HTTPException(status_code=400, detail='Delete request is not pending')
            row_id = int(req_map.get('row_id'))
            church_id = req_map.get('church')
            _enforce_church_scope(church_id, actor_church, context='delete approval')

            current = conn.execute(text('SELECT * FROM members_collection WHERE id=:id'), {'id': row_id}).fetchone()
            if not current:
                raise HTTPException(status_code=404, detail='Target members_collection row no longer exists')

            conn.execute(text('DELETE FROM members_collection WHERE id=:id'), {'id': row_id})
            conn.execute(
                text('''
                    UPDATE members_collection_delete_requests
                    SET status='approved', approved_by_user_id=:uid, approved_by_username=:un,
                        approved_by_role=:role, decision_note=:note, decided_at=:decided
                    WHERE id=:id
                '''),
                {
                    'uid': actor.get('user_id'),
                    'un': actor.get('username'),
                    'role': actor.get('role'),
                    'note': decision_note,
                    'decided': datetime.utcnow(),
                    'id': request_id,
                }
            )
        _log_collection_action('members_collection_delete_approved', row_id, church_id, auth, {'request_id': request_id, 'decision_note': decision_note})
        return {'ok': True, 'deleted_row_id': row_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/members_collection/delete_requests/{request_id}/reject')
async def reject_members_collection_delete_request(request_id: int, request: Request, auth: dict = Depends(require_api_key_or_user)):
    """Reject a pending deletion request."""
    _require_auth_right(auth, 'can_delete_members_collections', context='delete approval rejection')
    actor_church = _auth_context_church(auth)
    actor = _auth_actor_identity(auth)
    body = {}
    try:
        body = await request.json()
    except Exception:
        body = {}
    decision_note = str((body or {}).get('decision_note') or '').strip()
    try:
        with engine.begin() as conn:
            req = conn.execute(text('SELECT * FROM members_collection_delete_requests WHERE id=:id'), {'id': request_id}).fetchone()
            if not req:
                raise HTTPException(status_code=404, detail='Delete request not found')
            req_map = dict(req._mapping)
            if str(req_map.get('status') or '').lower() != 'pending':
                raise HTTPException(status_code=400, detail='Delete request is not pending')
            _enforce_church_scope(req_map.get('church'), actor_church, context='delete approval rejection')
            conn.execute(
                text('''
                    UPDATE members_collection_delete_requests
                    SET status='rejected', approved_by_user_id=:uid, approved_by_username=:un,
                        approved_by_role=:role, decision_note=:note, decided_at=:decided
                    WHERE id=:id
                '''),
                {
                    'uid': actor.get('user_id'),
                    'un': actor.get('username'),
                    'role': actor.get('role'),
                    'note': decision_note,
                    'decided': datetime.utcnow(),
                    'id': request_id,
                }
            )
        _log_collection_action('members_collection_delete_rejected', int(req_map.get('row_id') or 0), req_map.get('church'), auth, {'request_id': request_id, 'decision_note': decision_note})
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete('/members_collection/{row_id}')
def delete_members_collection(row_id: int, auth: dict = Depends(require_api_key_or_user)):
    """Direct delete for authorized roles (head treasurer/admin/system_admin)."""
    _require_auth_right(auth, 'can_delete_members_collections', context='members collection delete')
    actor_church = _auth_context_church(auth)
    try:
        with engine.begin() as conn:
            row = conn.execute(text('SELECT church FROM members_collection WHERE id=:id'), {'id': row_id}).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail='members_collection row not found')
            church_id = row[0]
            _enforce_church_scope(church_id, actor_church, context='members collection delete')
            conn.execute(text('DELETE FROM members_collection WHERE id=:id'), {'id': row_id})
        _log_collection_action('members_collection_deleted_direct', row_id, church_id, auth, {'mode': 'direct'})
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/members_collections/bulk')
def bulk_insert_members_collections(rows: List[dict], auth: dict = Depends(require_api_key_or_user), request: Request = None):
    """Accept a list of dicts and insert into `members_collection` in bulk."""
    _require_auth_right(auth, 'can_manage_collections', context='bulk collection insert')
    received_count = len(rows) if rows is not None else 0
    if not rows:
        raise HTTPException(status_code=400, detail={"message": "No rows provided", "received": received_count})
    # Pre-process rows: resolve church names/defaults and normalize identifiers.
    uploader = auth.get('uploader') if isinstance(auth, dict) else None
    actor_church = _auth_context_church(auth)
    pre_rows = []
    for i, r in enumerate(rows):
        row = dict(r)

        # resolve church name -> id if necessary
        church_val = row.get('church')
        if (church_val is None or church_val == '') and uploader and uploader.get('church'):
            row['church'] = uploader.get('church')
        row['church'] = _enforce_church_scope(row.get('church'), actor_church, context='bulk members_collection insert')

        for sk in ('s4', 's10', 's11', 's12', 'source', 'collection_code', 'notes'):
            if row.get(sk) is not None and not isinstance(row.get(sk), str):
                row[sk] = str(row.get(sk))

        # set source from uploader if not present
        try:
            if uploader and not row.get('source'):
                row['source'] = uploader.get('name')
        except Exception:
            pass

        pre_rows.append(row)

    normalized_rows = normalize_members_collection_rows(pre_rows, fill_missing_s3=True, recompute_s1=True)
    errors = []
    valid_rows = []
    for i, row in enumerate(normalized_rows):
        try:
            validated = MembersCollectionRow(**row)
            valid_rows.append(validated.dict())
        except ValidationError as ve:
            errors.append({"index": i, "errors": ve.errors()})

    if errors:
        # echo received count and rows for debugging
        raise HTTPException(status_code=422, detail={"received": received_count, "validation_errors": errors, "rows": normalized_rows})

    try:
        # Normalize Decimal -> int/float for DB driver compatibility
        def _norm(v):
            if isinstance(v, Decimal):
                try:
                    iv = int(v)
                    if iv == v:
                        return iv
                except Exception:
                    pass
                return float(v)
            return v

        norm_rows = []
        for r in valid_rows:
            nr = {k: _norm(v) for k, v in r.items()}
            norm_rows.append(nr)

        df = pd.DataFrame(norm_rows)
        if df.empty:
            return {"received": received_count, "valid": len(norm_rows), "inserted": 0, "message": "No rows to insert after normalization"}
        insert_dataframe(df, table_name='members_collection')
        return {"received": received_count, "valid": len(norm_rows), "inserted": len(df)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/collection_codes')
def list_collection_codes(current_user: dict = Depends(get_current_user)):
    """List collection codes scoped by church.

    - System admin: sees all codes.
    - Non-system users: see their church codes plus global codes.
    """
    try:
        church_id = current_user.get('church')
        role = str(current_user.get('role') or '').lower()
        params = {}
        if role == ROLE_SYSTEM_ADMIN:
            query = 'SELECT * FROM collection_codes ORDER BY CASE WHEN church IS NULL THEN 0 ELSE 1 END, church, id'
        else:
            query = 'SELECT * FROM collection_codes WHERE church = :church_id OR church IS NULL ORDER BY CASE WHEN church = :church_id THEN 0 WHEN church IS NULL THEN 1 ELSE 2 END, id'
            params['church_id'] = church_id
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()
            out = [dict(row._mapping) for row in rows]
        
        return [{k: _serializable_value(v) for k, v in r.items()} for r in out]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class MembersCollectionRow(BaseModel):
    collection_code: Optional[str] = None
    member_id: Optional[int] = None
    # s1/s3 can be auto-generated from s2 + church daily sequence.
    s1: Optional[str] = None
    s2: datetime
    s3: Optional[int] = None
    s4: str
    # optional monetary/other fields
    s5: Optional[float] = None
    s6: Optional[float] = None
    s7: Optional[float] = None
    s8: Optional[float] = None
    s9: Optional[float] = None
    s10: Optional[str] = None
    s11: Optional[str] = None
    s12: Optional[str] = None
    s13: Optional[float] = None
    source: Optional[str] = None
    # c1..c20 optional
    c1: Optional[float] = None
    c2: Optional[float] = None
    c3: Optional[float] = None
    c4: Optional[float] = None
    c5: Optional[float] = None
    c6: Optional[float] = None
    c7: Optional[float] = None
    c8: Optional[float] = None
    c9: Optional[float] = None
    c10: Optional[float] = None
    c11: Optional[float] = None
    c12: Optional[float] = None
    c13: Optional[float] = None
    c14: Optional[float] = None
    c15: Optional[float] = None
    c16: Optional[float] = None
    c17: Optional[float] = None
    c18: Optional[float] = None
    c19: Optional[float] = None
    c20: Optional[float] = None
    # l1..l41 optional
    l1: Optional[float] = None
    l2: Optional[float] = None
    l3: Optional[float] = None
    l4: Optional[float] = None
    l5: Optional[float] = None
    l6: Optional[float] = None
    l7: Optional[float] = None
    l8: Optional[float] = None
    l9: Optional[float] = None
    l10: Optional[float] = None
    l11: Optional[float] = None
    l12: Optional[float] = None
    l13: Optional[float] = None
    l14: Optional[float] = None
    l15: Optional[float] = None
    l16: Optional[float] = None
    l17: Optional[float] = None
    l18: Optional[float] = None
    l19: Optional[float] = None
    l20: Optional[float] = None
    l21: Optional[float] = None
    l22: Optional[float] = None
    l23: Optional[float] = None
    l24: Optional[float] = None
    l25: Optional[float] = None
    l26: Optional[float] = None
    l27: Optional[float] = None
    l28: Optional[float] = None
    l29: Optional[float] = None
    l30: Optional[float] = None
    l31: Optional[float] = None
    l32: Optional[float] = None
    l33: Optional[float] = None
    l34: Optional[float] = None
    l35: Optional[float] = None
    l36: Optional[float] = None
    l37: Optional[float] = None
    l38: Optional[float] = None
    l39: Optional[float] = None
    l40: Optional[float] = None
    l41: Optional[float] = None


@app.post('/members_collections/validate')
def validate_members_collections(rows: List[dict], auth: dict = Depends(require_api_key_or_user), request: Request = None):
    """Validate rows and return per-row validation errors (if any)."""
    _require_auth_right(auth, 'can_manage_collections', context='collection validation')
    errors = []
    pre_rows = []
    # If API key present, use uploader to default church/source
    uploader = auth.get('uploader') if isinstance(auth, dict) else None
    actor_church = _auth_context_church(auth)

    for i, r in enumerate(rows):
        row = dict(r)
        # Normalize church: allow church name or id
        church_val = row.get('church')
        if (church_val is None or church_val == '') and uploader and uploader.get('church'):
            row['church'] = uploader.get('church')
        row['church'] = _enforce_church_scope(row.get('church'), actor_church, context='validation request')

        for sk in ('s4', 's10', 's11', 's12', 'source', 'collection_code', 'notes'):
            if row.get(sk) is not None and not isinstance(row.get(sk), str):
                row[sk] = str(row.get(sk))

        pre_rows.append(row)

    out_rows = normalize_members_collection_rows(pre_rows, fill_missing_s3=True, recompute_s1=True)
    for i, row in enumerate(out_rows):
        try:
            MembersCollectionRow(**row)
        except ValidationError as ve:
            errors.append({"index": i, "errors": ve.errors()})
    return {"validation_errors": errors, "rows": out_rows}


class CollectionCodeIn(BaseModel):
    column_name: str
    code: str | None = None
    custom_collection_name: str | None = None
    scope: str | None = None  # 'local' | 'conference' | 'split'
    conference_split_pct: float | None = None  # 0-100, used when scope='split'


class AppNameIn(BaseModel):
    app_name: str


class ChurchCreateIn(BaseModel):
    name: str
    app_name: Optional[str] = None


@app.post('/collection_codes')
def create_collection_code(payload: CollectionCodeIn, auth: dict = Depends(require_api_key_or_user)):
    try:
        _require_auth_right(auth, 'can_manage_collections', context='collection code creation')
        with engine.connect() as conn:
            actor_church = _auth_context_church(auth)
            if actor_church is None:
                raise HTTPException(status_code=400, detail='church is required for collection code creation')
            conn.execute(
                text("INSERT INTO collection_codes (column_name, code, custom_collection_name, church) VALUES (:cn, :c, :ccn, :ch)"),
                {"cn": payload.column_name, "c": payload.code, "ccn": payload.custom_collection_name, "ch": actor_church}
            )
            try:
                conn.commit()
            except Exception:
                pass
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/header_mappings')
def save_header_mappings(arr: List[dict], auth: dict = Depends(require_api_key_or_user)):
    """Save provided header->mapped_column mappings. Expects list of {header_name, mapped_column}."""
    try:
        _require_auth_roles(auth, {ROLE_SYSTEM_ADMIN, ROLE_ADMIN, ROLE_TREASURER, ROLE_UPLOADER}, context='header mapping save')
        upsert_header_mappings(arr)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class UploaderIn(BaseModel):
    name: str
    church: Optional[int] = None


@app.post('/uploaders')
def create_uploader_endpoint(payload: UploaderIn, current_user: dict = Depends(get_current_user)):
    try:
        if not _is_admin_user(current_user):
            raise HTTPException(status_code=403, detail='Not authorized')
        current_church = None if _is_system_admin_user(current_user) else _resolve_church_id(current_user.get('church'))
        church_id = _enforce_church_scope(payload.church, current_church, context='uploader creation')
        key = create_uploader(payload.name, church_id)
        return {"api_key": key}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/uploaders')
def list_uploaders_endpoint(current_user: dict = Depends(get_current_user)):
    try:
        if not _is_admin_user(current_user):
            raise HTTPException(status_code=403, detail='Not authorized')
        out = list_uploaders()
        user_church = None if _is_system_admin_user(current_user) else (_resolve_church_id(current_user.get('church')) if isinstance(current_user, dict) else None)
        if user_church is not None:
            out = [u for u in out if _resolve_church_id(u.get('church')) == user_church]
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/uploaders/{api_key}')
def get_uploader_by_key_endpoint(api_key: str, current_user: dict = Depends(get_current_user)):
    try:
        if not _is_admin_user(current_user):
            raise HTTPException(status_code=403, detail='Not authorized')
        u = get_uploader_by_key(api_key)
        if not u:
            raise HTTPException(status_code=404, detail="Uploader not found")
        user_church = None if _is_system_admin_user(current_user) else (_resolve_church_id(current_user.get('church')) if isinstance(current_user, dict) else None)
        _enforce_church_scope(u.get('church'), user_church, context='uploader details')
        return u
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class UserIn(BaseModel):
    username: str
    password: str
    church: Optional[int] = None


class UserRegister(BaseModel):
    username: str
    password: str
    church: Optional[int] = None
    role: Optional[str] = None
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None


class SelfPasswordResetIn(BaseModel):
    current_password: str
    new_password: str


class RolePolicyIn(BaseModel):
    role: str
    display_name: Optional[str] = None
    can_view_dashboard: bool = True
    can_manage_users: bool = False
    can_manage_members: bool = False
    can_manage_collections: bool = False
    can_manage_members_collections: bool = False
    can_delete_members_collections: bool = False
    can_unlock_members_collections: bool = False
    can_view_collection_codes: bool = False
    can_view_reports: bool = False
    can_manage_settings: bool = False
    can_manage_roles: bool = False


@app.get('/roles')
def get_roles(current_user: dict = Depends(get_current_user)):
    # Allow all authenticated users to fetch role policies for UI rights checks.
    # Non-system users should not see the system_admin role.
    try:
        rows = list_role_policies()
        if _is_system_admin_user(current_user):
            return rows
        # Non-system users cannot assign or see system_admin.
        return [r for r in rows if str(r.get('role') or '').lower() != ROLE_SYSTEM_ADMIN]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/roles')
def create_role(payload: RolePolicyIn, current_user: dict = Depends(get_current_user)):
    if not _can_manage_roles(current_user):
        raise HTTPException(status_code=403, detail='Only system admin can define roles')
    try:
        role_name = str(payload.role or '').strip().lower()
        if role_name == ROLE_SYSTEM_ADMIN:
            raise HTTPException(status_code=400, detail='system_admin role is reserved')
        rights = {
            'can_view_dashboard': payload.can_view_dashboard,
            'can_manage_users': payload.can_manage_users,
            'can_manage_members': payload.can_manage_members,
            'can_manage_collections': payload.can_manage_collections,
            'can_manage_members_collections': payload.can_manage_members_collections,
            'can_delete_members_collections': payload.can_delete_members_collections,
            'can_unlock_members_collections': payload.can_unlock_members_collections,
            'can_view_collection_codes': payload.can_view_collection_codes,
            'can_view_reports': payload.can_view_reports,
            'can_manage_settings': payload.can_manage_settings,
            'can_manage_roles': payload.can_manage_roles,
        }
        return upsert_role_policy(role_name, payload.display_name, rights)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put('/roles/{role_name}')
def update_role(role_name: str, payload: RolePolicyIn, current_user: dict = Depends(get_current_user)):
    if not _can_manage_roles(current_user):
        raise HTTPException(status_code=403, detail='Only system admin can update roles')
    try:
        target = str(role_name or '').strip().lower()
        if not target:
            raise HTTPException(status_code=400, detail='role_name is required')
        if target == ROLE_SYSTEM_ADMIN:
            raise HTTPException(status_code=400, detail='system_admin role rights cannot be changed')
        rights = {
            'can_view_dashboard': payload.can_view_dashboard,
            'can_manage_users': payload.can_manage_users,
            'can_manage_members': payload.can_manage_members,
            'can_manage_collections': payload.can_manage_collections,
            'can_manage_members_collections': payload.can_manage_members_collections,
            'can_delete_members_collections': payload.can_delete_members_collections,
            'can_unlock_members_collections': payload.can_unlock_members_collections,
            'can_view_collection_codes': payload.can_view_collection_codes,
            'can_view_reports': payload.can_view_reports,
            'can_manage_settings': payload.can_manage_settings,
            'can_manage_roles': payload.can_manage_roles,
        }
        return upsert_role_policy(target, payload.display_name, rights)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete('/roles/{role_name}')
def remove_role(role_name: str, current_user: dict = Depends(get_current_user)):
    if not _can_manage_roles(current_user):
        raise HTTPException(status_code=403, detail='Only system admin can delete roles')
    try:
        ok = delete_role_policy(role_name)
        if not ok:
            raise HTTPException(status_code=404, detail='Role not found')
        return {'ok': True}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/users/register')
def register_user(payload: UserRegister, credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    try:
        if not credentials or not credentials.credentials:
            raise HTTPException(status_code=403, detail='Only admins can create users')
        creator = _normalize_user_context(get_user_by_token(credentials.credentials))
        if not creator:
            raise HTTPException(status_code=401, detail='Invalid token')

        creator_role = str(creator.get('role') or '').lower()
        if creator_role not in {ROLE_SYSTEM_ADMIN, ROLE_ADMIN}:
            raise HTTPException(status_code=403, detail='Only admins can create users')

        role = str(payload.role or ROLE_UPLOADER).strip().lower()
        if not role_exists(role):
            raise HTTPException(status_code=400, detail='Invalid role')
        if role == ROLE_SYSTEM_ADMIN and creator_role != ROLE_SYSTEM_ADMIN:
            raise HTTPException(status_code=403, detail='Only system admin can assign system_admin role')

        creator_church = None if creator_role == ROLE_SYSTEM_ADMIN else _resolve_church_id(creator.get('church'))
        requested_church = _resolve_church_id(payload.church)
        effective_church = _enforce_church_scope(requested_church, creator_church, context='user registration')
        requested_username = str(payload.username or '').strip().lower()
        if requested_username == 'saypy_admin' and effective_church is not None:
            raise HTTPException(status_code=400, detail='saypy_admin must have church set to null')
        if requested_username != 'saypy_admin' and effective_church is None:
            raise HTTPException(status_code=400, detail='Church is required for all users except saypy_admin')
        out = create_user(
            payload.username,
            payload.password,
            effective_church,
            role=role,
            first_name=payload.first_name,
            middle_name=payload.middle_name,
            last_name=payload.last_name,
            email=payload.email,
            phone=payload.phone,
        )
        return out
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/users/login')
def login_user(payload: UserIn):
    u = verify_user(payload.username, payload.password)
    if not u:
        raise HTTPException(status_code=401, detail='Invalid username or password')
    u = _normalize_user_context(u)
    token = create_token_for_user(u['id'])
    return {"token": token, "user": u}


@app.post('/users/me/reset-password')
def reset_own_password(payload: SelfPasswordResetIn, current_user: dict = Depends(get_current_user)):
    try:
        username = str(current_user.get('username') or '').strip()
        user_id = current_user.get('id')
        if not username or user_id is None:
            raise HTTPException(status_code=401, detail='Invalid user session')

        current_password = str(payload.current_password or '')
        new_password = str(payload.new_password or '')
        policy_msg = password_policy_error(new_password)
        if policy_msg:
            raise HTTPException(status_code=400, detail=policy_msg)
        if current_password == new_password:
            raise HTTPException(status_code=400, detail='New password must be different from current password')

        verified = verify_user(username, current_password)
        if not verified:
            raise HTTPException(status_code=401, detail='Current password is incorrect')

        salt = os.urandom(16)
        ph = _hash_password(new_password, salt)
        salt_hex = binascii.hexlify(salt).decode('ascii')

        with engine.begin() as conn:
            conn.execute(
                text('UPDATE users SET password_hash=:ph, salt=:s WHERE id=:id'),
                {'ph': ph, 's': salt_hex, 'id': int(user_id)}
            )

        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/users')
def list_users(current_user: dict = Depends(get_current_user)):
    # only admins may list users
    if not _is_admin_user(current_user):
        raise HTTPException(status_code=403, detail='Not authorized')
    try:
        current_church = None if _is_system_admin_user(current_user) else _resolve_church_id(current_user.get('church'))
        with engine.connect() as conn:
            if current_church is None:
                res = conn.execute(text('SELECT id, username, first_name, middle_name, last_name, email, phone, church, role, created_at FROM users'))
            else:
                res = conn.execute(text('SELECT id, username, first_name, middle_name, last_name, email, phone, church, role, created_at FROM users WHERE church=:c'), {'c': current_church})
            rows = [dict(r._mapping) for r in res.fetchall()]
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put('/users/{user_id}')
def update_user(user_id: int, payload: UserRegister, current_user: dict = Depends(get_current_user)):
    # only admins may update users
    role_self = str(current_user.get('role') or '').lower()
    if not _is_admin_user(current_user):
        raise HTTPException(status_code=403, detail='Not authorized')
    try:
        current_church = None if _is_system_admin_user(current_user) else _resolve_church_id(current_user.get('church'))
        requested_church = _resolve_church_id(payload.church)
        effective_church = _enforce_church_scope(requested_church, current_church, context='user update')
        requested_username = str(payload.username or '').strip().lower()
        if requested_username == 'saypy_admin' and effective_church is not None:
            raise HTTPException(status_code=400, detail='saypy_admin must have church set to null')
        if requested_username != 'saypy_admin' and effective_church is None:
            raise HTTPException(status_code=400, detail='Church is required for all users except saypy_admin')
        next_role = str(payload.role or ROLE_UPLOADER).strip().lower()
        if not role_exists(next_role):
            raise HTTPException(status_code=400, detail='Invalid role')
        if next_role == ROLE_SYSTEM_ADMIN and role_self != ROLE_SYSTEM_ADMIN:
            raise HTTPException(status_code=403, detail='Only system admin can assign system_admin role')
        # update username, church, role, and optionally password
        with engine.connect() as conn:
            if current_church is not None:
                existing = conn.execute(text('SELECT church FROM users WHERE id=:id'), {'id': user_id}).fetchone()
                if not existing:
                    raise HTTPException(status_code=404, detail='user not found')
                _enforce_church_scope(existing[0], current_church, context='this user record')
            if payload.password:
                policy_msg = password_policy_error(payload.password)
                if policy_msg:
                    raise HTTPException(status_code=400, detail=policy_msg)
                # create new salt/hash
                salt = os.urandom(16)
                ph = _hash_password(payload.password, salt)
                salt_hex = binascii.hexlify(salt).decode('ascii')
                conn.execute(text('UPDATE users SET username=:u, first_name=:fn, middle_name=:mn, last_name=:ln, email=:em, phone=:phn, password_hash=:ph, salt=:s, church=:c, role=:r WHERE id=:id'),
                             {'u': payload.username, 'fn': payload.first_name, 'mn': payload.middle_name, 'ln': payload.last_name, 'em': payload.email, 'phn': payload.phone, 'ph': ph, 's': salt_hex, 'c': effective_church, 'r': next_role, 'id': user_id})
            else:
                conn.execute(text('UPDATE users SET username=:u, first_name=:fn, middle_name=:mn, last_name=:ln, email=:em, phone=:phn, church=:c, role=:r WHERE id=:id'),
                             {'u': payload.username, 'fn': payload.first_name, 'mn': payload.middle_name, 'ln': payload.last_name, 'em': payload.email, 'phn': payload.phone, 'c': effective_church, 'r': next_role, 'id': user_id})
            try:
                conn.commit()
            except Exception:
                pass
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/churches')
def list_churches():
    try:
        df = pd.read_sql_table('church', con=engine)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/churches')
def create_church(payload: ChurchCreateIn, current_user: dict = Depends(get_current_user)):
    """Create a new church and apply default collection codes from Collection_Codes.xlsx."""
    try:
        if not _is_system_admin_user(current_user):
            raise HTTPException(status_code=403, detail='Only system admin can create churches')

        name = str(payload.name or '').strip()
        if not name:
            raise HTTPException(status_code=400, detail='Church name is required')
        app_name = str(payload.app_name or '').strip() or 'Church Offerings \u2014 Admin Console'

        with engine.connect() as conn:
            existing = conn.execute(text('SELECT id FROM church WHERE LOWER(name)=LOWER(:n)'), {'n': name}).fetchone()
            if existing:
                raise HTTPException(status_code=400, detail='Church already exists')
            conn.execute(text('INSERT INTO church (name, app_name) VALUES (:n, :a)'), {'n': name, 'a': app_name})
            try:
                conn.commit()
            except Exception:
                pass

            created = conn.execute(text('SELECT id, name, app_name FROM church WHERE LOWER(name)=LOWER(:n)'), {'n': name}).fetchone()

        # Ensure default church-scoped codes are present for the newly created church.
        import_collection_codes_from_workbook()

        if not created:
            raise HTTPException(status_code=500, detail='Failed to create church')
        return {'ok': True, 'church': {'id': created[0], 'name': created[1], 'app_name': created[2]}}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/churches/{church_id}')
def get_church(church_id: int, current_user: dict = Depends(get_current_user)):
    """Get church details including app_name."""
    try:
        # Check if user can access this church
        user_church = current_user.get('church')
        user_role = current_user.get('role')
        if user_role != ROLE_SYSTEM_ADMIN and user_church != church_id:
            raise HTTPException(status_code=403, detail='You do not have access to this church')
        
        with engine.connect() as conn:
            result = conn.execute(text('SELECT id, name, app_name FROM church WHERE id = :id'), {'id': church_id})
            row = result.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail='Church not found')
            return dict(row._mapping)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/churches/{church_id}/app_name')
def get_church_app_name(church_id: int, current_user: dict = Depends(get_current_user)):
    """Get the application name for a church."""
    try:
        # Check if user can access this church
        user_church = current_user.get('church')
        user_role = current_user.get('role')
        if user_role != ROLE_SYSTEM_ADMIN and user_church != church_id:
            raise HTTPException(status_code=403, detail='You do not have access to this church')
        
        with engine.connect() as conn:
            result = conn.execute(text('SELECT app_name FROM church WHERE id = :id'), {'id': church_id})
            row = result.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail='Church not found')
            app_name = row[0] if row[0] else 'Church Offerings \u2014 Admin Console'
            return {'app_name': app_name}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put('/churches/{church_id}/app_name')
def update_church_app_name(church_id: int, payload: AppNameIn, current_user: dict = Depends(get_current_user)):
    """Update the application name for a church (admin only)."""
    try:
        # Check if user is admin for this church
        user_church = current_user.get('church')
        user_role = current_user.get('role')
        if user_role not in {ROLE_SYSTEM_ADMIN, ROLE_ADMIN}:
            raise HTTPException(status_code=403, detail='Only admins can update church settings')
        if user_role == ROLE_ADMIN and user_church != church_id:
            raise HTTPException(status_code=403, detail='You can only update your own church')
        
        app_name = str(payload.app_name or '').strip()
        if not app_name:
            raise HTTPException(status_code=400, detail='app_name is required')
        
        with engine.connect() as conn:
            conn.execute(text('UPDATE church SET app_name = :name WHERE id = :id'), {'name': app_name, 'id': church_id})
            try:
                conn.commit()
            except Exception:
                pass
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/config')
def get_config():
    """Return public system-wide configuration (no auth required)."""
    try:
        ensure_global_config_schema()
        with engine.connect() as conn:
            row = conn.execute(text("SELECT value FROM global_config WHERE key = 'system_name'")).fetchone()
        system_name = row[0] if row and row[0] else 'Church Offerings'
        return {'system_name': system_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SystemNameIn(BaseModel):
    system_name: str


@app.put('/config/system_name')
def update_system_name(payload: SystemNameIn, current_user: dict = Depends(get_current_user)):
    """Update the system-wide login screen name. Only system_admin."""
    try:
        if not _is_system_admin_user(current_user):
            raise HTTPException(status_code=403, detail='Only system admin can change the system name')
        name = str(payload.system_name or '').strip()
        if not name:
            raise HTTPException(status_code=400, detail='system_name is required')
        ensure_global_config_schema()
        with engine.connect() as conn:
            conn.execute(
                text("INSERT INTO global_config (key, value) VALUES ('system_name', :v) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"),
                {'v': name}
            )
            try:
                conn.commit()
            except Exception:
                pass
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/churches/{church_id}/collection_codes')
def create_church_collection_code(church_id: int, payload: CollectionCodeIn, current_user: dict = Depends(get_current_user)):
    """Create a church-specific collection code for users with collection rights."""
    try:
        user_church = current_user.get('church')
        if not (_role_has_right(current_user.get('role'), 'can_manage_collections') or _role_has_right(current_user.get('role'), 'can_view_collection_codes')):
            raise HTTPException(status_code=403, detail='Not authorized for collection code creation')
        if not _is_system_admin_user(current_user) and user_church != church_id:
            raise HTTPException(status_code=403, detail='You can only manage codes for your own church')
        
        with engine.connect() as conn:
            conn.execute(text("INSERT INTO collection_codes (column_name, code, custom_collection_name, church, scope, conference_split_pct) VALUES (:cn, :c, :ccn, :ch, :sc, :csp)"), 
                        {"cn": payload.column_name, "c": payload.code, "ccn": payload.custom_collection_name, "ch": church_id, "sc": payload.scope, "csp": payload.conference_split_pct})
            try:
                conn.commit()
            except Exception:
                pass
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/churches/{church_id}/collection_codes')
def list_church_collection_codes(church_id: int, current_user: dict = Depends(get_current_user)):
    """List collection codes for a specific church.

    Returns both local church codes and global shared codes, with local first.
    """
    try:
        user_church = current_user.get('church')
        if not (_role_has_right(current_user.get('role'), 'can_manage_collections') or _role_has_right(current_user.get('role'), 'can_view_collection_codes')):
            raise HTTPException(status_code=403, detail='Not authorized for collection code viewing')
        if not _is_system_admin_user(current_user) and user_church != church_id:
            raise HTTPException(status_code=403, detail='You can only view codes for your own church')

        with engine.connect() as conn:
            result = conn.execute(
                text('''
                    SELECT *
                    FROM collection_codes
                    WHERE church = :church_id OR church IS NULL
                    ORDER BY CASE WHEN church = :church_id THEN 0 WHEN church IS NULL THEN 1 ELSE 2 END, id
                '''),
                {'church_id': church_id}
            )
            rows = result.fetchall()
            out = [dict(row._mapping) for row in rows]

        return [{k: _serializable_value(v) for k, v in r.items()} for r in out]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put('/churches/{church_id}/collection_codes/{code_id}')
def update_church_collection_code(church_id: int, code_id: int, payload: CollectionCodeIn, current_user: dict = Depends(get_current_user)):
    """Update a church-specific collection code for users with collection rights."""
    try:
        user_church = current_user.get('church')
        if not (_role_has_right(current_user.get('role'), 'can_manage_collections') or _role_has_right(current_user.get('role'), 'can_view_collection_codes')):
            raise HTTPException(status_code=403, detail='Not authorized for collection code update')
        if not _is_system_admin_user(current_user) and user_church != church_id:
            raise HTTPException(status_code=403, detail='You can only manage codes for your own church')
        
        # Verify code belongs to this church
        with engine.connect() as conn:
            result = conn.execute(text("SELECT church FROM collection_codes WHERE id = :id"), {"id": code_id})
            row = result.fetchone()
            if not row or row[0] != church_id:
                raise HTTPException(status_code=404, detail='Code not found or does not belong to this church')
            
            conn.execute(text("UPDATE collection_codes SET column_name=:cn, code=:c, custom_collection_name=:ccn, scope=:sc, conference_split_pct=:csp WHERE id=:id"), 
                        {"cn": payload.column_name, "c": payload.code, "ccn": payload.custom_collection_name, "sc": payload.scope, "csp": payload.conference_split_pct, "id": code_id})
            try:
                conn.commit()
            except Exception:
                pass
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete('/churches/{church_id}/collection_codes/{code_id}')
def delete_church_collection_code(church_id: int, code_id: int, current_user: dict = Depends(get_current_user)):
    """Delete a church-specific collection code for users with collection rights."""
    try:
        user_church = current_user.get('church')
        if not _role_has_right(current_user.get('role'), 'can_manage_collections'):
            raise HTTPException(status_code=403, detail='Not authorized for collection code deletion')
        if not _is_system_admin_user(current_user) and user_church != church_id:
            raise HTTPException(status_code=403, detail='You can only manage codes for your own church')
        
        # Verify code belongs to this church
        with engine.connect() as conn:
            result = conn.execute(text("SELECT church FROM collection_codes WHERE id = :id"), {"id": code_id})
            row = result.fetchone()
            if not row or row[0] != church_id:
                raise HTTPException(status_code=404, detail='Code not found or does not belong to this church')
            
            conn.execute(text("DELETE FROM collection_codes WHERE id = :id"), {"id": code_id})
            try:
                conn.commit()
            except Exception:
                pass
        return {'ok': True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.put('/collection_codes/{code_id}')
def update_collection_code(code_id: int, payload: CollectionCodeIn, auth: dict = Depends(require_api_key_or_user)):
    try:
        _require_auth_right(auth, 'can_manage_collections', context='collection code update')
        actor_church = _auth_context_church(auth)
        with engine.connect() as conn:
            if actor_church is not None:
                row = conn.execute(text('SELECT church FROM collection_codes WHERE id=:id'), {'id': code_id}).fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail='Code not found')
                _enforce_church_scope(row[0], actor_church, context='this collection code')
            conn.execute(text("UPDATE collection_codes SET column_name=:cn, code=:c, custom_collection_name=:ccn, scope=:sc, conference_split_pct=:csp WHERE id=:id"), {"cn": payload.column_name, "c": payload.code, "ccn": payload.custom_collection_name, "sc": payload.scope, "csp": payload.conference_split_pct, "id": code_id})
            try:
                conn.commit()
            except Exception:
                pass
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/members')
def list_members(q: Optional[str] = None, auth: dict = Depends(require_api_key_or_user)):
    try:
        _require_auth_any_right(
            auth,
            {'can_manage_members', 'can_manage_collections', 'can_manage_members_collections'},
            context='members listing'
        )
        actor_church = _auth_context_church(auth)
        sql = 'SELECT * FROM members WHERE 1=1'
        params = {}

        if actor_church is not None:
            sql += ' AND church = :church'
            params['church'] = actor_church

        if q:
            q_text = str(q).strip()
            q_like = f"%{q_text.lower()}%"
            try:
                qnum = int(q_text)
            except Exception:
                qnum = None
            if qnum is not None:
                sql += " AND (LOWER(COALESCE(\"MEMBER_NAME\", '')) LIKE :q_like OR CAST(\"MEMBER_ID\" AS TEXT) = :q_exact)"
                params['q_exact'] = str(qnum)
            else:
                sql += " AND LOWER(COALESCE(\"MEMBER_NAME\", '')) LIKE :q_like"
            params['q_like'] = q_like

        sql += ' ORDER BY sno ASC, id ASC'

        with engine.connect() as conn:
            res = conn.execute(text(sql), params)
            rows = [dict(r._mapping) for r in res.fetchall()]

        for r in rows:
            r['PHONE'] = _normalize_phone_text(r.get('PHONE'))
            r['PHONE2'] = _normalize_phone_text(r.get('PHONE2'))

        out = [{k: _serializable_value(v) for k, v in r.items()} for r in rows]
        return out
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put('/members/{member_id}')
def update_member(member_id: int, payload: MemberIn, auth: dict = Depends(require_api_key_or_user)):
    try:
        _require_auth_right(auth, 'can_manage_members', context='member update')
        ensure_members_schema()
        actor_church = _auth_context_church(auth)
        if actor_church is not None:
            with engine.connect() as conn:
                existing = conn.execute(text('SELECT church FROM members WHERE id=:id'), {'id': member_id}).fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail='member not found')
            _enforce_church_scope(existing[0], actor_church, context='this member record')
        effective_church = _enforce_church_scope(payload.church, actor_church, context='member update')
        normalized_status = _normalize_member_status(payload.STATUS)
        transfer_to_church, transfer_date = _normalize_member_transfer_fields(
            normalized_status,
            payload.TRANSFER_TO_CHURCH,
            payload.TRANSFER_DATE,
        )

        inspector = inspect(engine)
        member_cols = {str(c.get('name') or '').strip() for c in inspector.get_columns('members')}
        member_cols_lc = {c.lower() for c in member_cols}

        transfer_to_col = 'transfer_to_church' if 'transfer_to_church' in member_cols_lc else ('"TRANSFER_TO_CHURCH"' if 'TRANSFER_TO_CHURCH' in member_cols else None)
        transfer_date_col = 'transfer_date' if 'transfer_date' in member_cols_lc else ('"TRANSFER_DATE"' if 'TRANSFER_DATE' in member_cols else None)
        status_updated_col = 'status_updated_at' if 'status_updated_at' in member_cols_lc else ('"STATUS_UPDATED_AT"' if 'STATUS_UPDATED_AT' in member_cols else None)

        set_parts = [
            'sno=:sno',
            '"MEMBER_NAME"=:mn',
            '"MEMBER_ID"=:mid',
            '"FAMILY_ID"=:fid',
            '"DEFAULT_FAMILY_ID"=:dfid',
            '"OFFICIAL_MEMBER_ID"=:omid',
            'pledge=:pledge',
            '"GROUP_NAME"=:gname',
            '"GROUP_ALIAS"=:galias',
            '"DEFAULT_GROUP_ALIAS"=:dgalias',
            '"GROUP_LEADER_ID"=:gleader',
            '"DEFAULT_GROUP_LEADER_ID"=:dgleader',
            '"STATUS"=:status',
            '"PHONE"=:phone',
            '"PHONE2"=:phone2',
            '"EMAIL"=:email',
            '"RESIDENCE"=:res',
            'church=:church',
        ]

        if transfer_to_col:
            set_parts.append(f'{transfer_to_col}=:transfer_to_church')
        if transfer_date_col:
            set_parts.append(f'{transfer_date_col}=:transfer_date')
        if status_updated_col:
            set_parts.append(f'{status_updated_col}=:status_updated_at')

        update_sql = f"UPDATE members SET {', '.join(set_parts)} WHERE id=:id"

        with engine.connect() as conn:
            conn.execute(
                text(update_sql),
                {
                    "sno": payload.sno,
                    "mn": payload.MEMBER_NAME,
                    "mid": payload.MEMBER_ID,
                    "fid": payload.FAMILY_ID,
                    "dfid": payload.DEFAULT_FAMILY_ID,
                    "omid": payload.OFFICIAL_MEMBER_ID,
                    "pledge": payload.pledge,
                    "gname": payload.GROUP_NAME,
                    "galias": payload.GROUP_ALIAS,
                    "dgalias": payload.DEFAULT_GROUP_ALIAS,
                    "gleader": payload.GROUP_LEADER_ID,
                    "dgleader": payload.DEFAULT_GROUP_LEADER_ID,
                    "status": normalized_status,
                    "transfer_to_church": transfer_to_church,
                    "transfer_date": transfer_date,
                    "status_updated_at": datetime.utcnow() if normalized_status else None,
                    "phone": _normalize_phone_text(payload.PHONE),
                    "phone2": _normalize_phone_text(payload.PHONE2),
                    "email": payload.EMAIL,
                    "res": payload.RESIDENCE,
                    "church": effective_church,
                    "id": member_id,
                },
            )
            try:
                conn.commit()
            except Exception:
                pass
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/reports/members_collections')
def report_members_collections(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    search: Optional[str] = None,
    search_field: Optional[str] = 'all',
    collection_code: Optional[str] = None,
    member_id: Optional[str] = None,
    member_name: Optional[str] = None,
    s1: Optional[str] = None,
    amount_min: Optional[float] = None,
    amount_max: Optional[float] = None,
    limit: Optional[int] = None,
    auth: dict = Depends(require_api_key_or_user),
):
    """Return church-scoped uploaded members_collection rows with flexible optional filters."""
    try:
        _require_auth_any_right(auth, {'can_manage_members_collections', 'can_view_reports'}, context='members collections access')
        # Read full table into pandas then filter by s2 in Python to avoid SQL param dialect issues
        df = pd.read_sql_table('members_collection', con=engine)
        actor_church = _auth_context_church(auth)
        if actor_church is not None and 'church' in df.columns:
            try:
                actor_church_num = int(actor_church)
            except Exception:
                actor_church_num = None
            if actor_church_num is not None:
                # Coerce church values defensively for sqlite/postgres mixed type reads.
                church_series = pd.to_numeric(df['church'], errors='coerce')
                df = df[church_series == actor_church_num]
            else:
                df = df[df['church'].astype(str) == str(actor_church)]
        # Flexible filters for members collections page.
        search = str(search or '').strip()
        search_field = str(search_field or 'all').strip()
        collection_code = str(collection_code or '').strip()
        member_id = str(member_id or '').strip()
        member_name = str(member_name or '').strip()
        s1 = str(s1 or '').strip()
        has_active_filters = any([
            start_date,
            end_date,
            search,
            collection_code,
            member_id,
            member_name,
            s1,
            amount_min is not None,
            amount_max is not None,
        ])

        if collection_code and 'collection_code' in df.columns:
            df = df[df['collection_code'].astype(str).str.strip().str.lower() == collection_code.lower()]

        if member_id and 'member_id' in df.columns:
            df = df[df['member_id'].astype(str).str.lower().str.contains(member_id.lower(), na=False)]

        if member_name and 's4' in df.columns:
            df = df[df['s4'].astype(str).str.lower().str.contains(member_name.lower(), na=False)]

        if s1 and 's1' in df.columns:
            df = df[df['s1'].astype(str).str.lower().str.contains(s1.lower(), na=False)]

        if amount_min is not None and 's5' in df.columns:
            amt = pd.to_numeric(df['s5'], errors='coerce')
            df = df[amt >= float(amount_min)]

        if amount_max is not None and 's5' in df.columns:
            amt = pd.to_numeric(df['s5'], errors='coerce')
            df = df[amt <= float(amount_max)]

        if search:
            q = search.lower()
            if search_field and search_field != 'all' and search_field in df.columns:
                df = df[df[search_field].astype(str).str.lower().str.contains(q, na=False)]
            else:
                mask = pd.Series([False] * len(df), index=df.index)
                for col in df.columns:
                    try:
                        mask = mask | df[col].astype(str).str.lower().str.contains(q, na=False)
                    except Exception:
                        continue
                df = df[mask]
        # Parse provided dates defensively. Accept either full ISO datetimes or simple YYYY-MM-DD.
        try:
            start_dt = pd.to_datetime(start_date, errors='coerce') if start_date else None
            end_dt = pd.to_datetime(end_date, errors='coerce') if end_date else None
            # If user provided a date-only string like YYYY-MM-DD, extend end_dt to end of that day
            if end_date and end_dt is not pd.NaT and len(str(end_date)) == 10:
                end_dt = end_dt + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
            # Apply filter if s2 column exists and at least one bound is provided
            if 's2' in df.columns and (start_dt is not None or end_dt is not None):
                df['s2'] = pd.to_datetime(df['s2'], errors='coerce')
                mask = pd.Series([True] * len(df))
                if start_dt is not None and start_dt is not pd.NaT:
                    mask = mask & (df['s2'] >= start_dt)
                if end_dt is not None and end_dt is not pd.NaT:
                    mask = mask & (df['s2'] <= end_dt)
                df = df[mask.fillna(False)]
        except Exception as e:
            # Return a helpful error for invalid date input rather than failing silently
            raise HTTPException(status_code=400, detail=f"Invalid date filter: {e}")

        # Default view: latest 10 uploaded rows for this church.
        if 'added_at' in df.columns:
            df['added_at'] = pd.to_datetime(df['added_at'], errors='coerce')
            df = df.sort_values(by=['added_at', 'id'], ascending=[False, False], na_position='last')
        elif 'id' in df.columns:
            df = df.sort_values(by='id', ascending=False)

        effective_limit = limit
        if effective_limit is None and not has_active_filters:
            effective_limit = 10
        if effective_limit is not None:
            try:
                lim = max(1, int(effective_limit))
                df = df.head(lim)
            except Exception:
                pass
        rows = df.to_dict(orient='records')
        out = [{k: _serializable_value(v) for k, v in r.items()} for r in rows]
        return out
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/reports/period_summary')
def report_period_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    auth: dict = Depends(require_api_key_or_user),
):
    """Period summary report — aggregated conference / local / total per collection item.

    Business logic lives in ``backend/reports.py :: compute_period_summary``.
    Edit that module to change aggregation rules, label overrides, or split
    percentages without touching the API routing layer.
    """
    _require_auth_any_right(
        auth,
        {'can_manage_members_collections', 'can_view_reports'},
        context='period summary report',
    )
    actor_church = _auth_context_church(auth)
    try:
        return compute_period_summary(
            actor_church=actor_church,
            start_date=start_date,
            end_date=end_date,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _log_user_church_policy_violations():
    """Log users violating the church assignment policy.

    Policy: only `saypy_admin` can have church=NULL.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT id, username, role
                    FROM users
                    WHERE church IS NULL
                      AND LOWER(username) <> 'saypy_admin'
                    ORDER BY id
                """)
            ).fetchall()
        if not rows:
            logger.info("User church policy check passed: no violations found")
            return

        sample = [f"id={r[0]}, username={r[1]}, role={r[2]}" for r in rows[:10]]
        logger.warning(
            "User church policy violation: %s user(s) have church=NULL but are not saypy_admin. Sample: %s",
            len(rows),
            "; ".join(sample),
        )
    except Exception:
        logger.exception("Failed to run user church policy validation")


@app.on_event("startup")
def on_startup():
    try:
        create_tables()
    except Exception:
        logger.exception("Database initialization failed during startup")
        raise

    _log_user_church_policy_violations()

    try:
        _ensure_collection_workflow_tables()
    except Exception:
        logger.exception('Failed to initialize collection workflow tables')

    try:
        added_cols = ensure_members_collection_schema()
        if added_cols:
            logger.info('members_collection schema migration added columns: %s', ', '.join(sorted(added_cols)))
        else:
            logger.info('members_collection schema migration: no columns added')
    except Exception:
        logger.exception('Failed to verify/migrate members_collection schema columns')

    # If members table exists but is empty, attempt to initialize from Members.xlsx
    try:
        inspector = inspect(engine)
        if 'members' in inspector.get_table_names():
            with engine.connect() as conn:
                res = conn.execute(text('SELECT COUNT(*) FROM members'))
                try:
                    cnt = res.scalar()
                except Exception:
                    row = res.fetchone()
                    cnt = row[0] if row else 0
            if not cnt:
                try:
                    # import here to avoid circular imports at module load
                    from .init_members import run as init_members_run
                    init_members_run()
                except Exception:
                    # do not crash startup if initialization fails
                    pass
    except Exception:
        pass


@app.get('/members_view')
def get_members_view(auth: dict = Depends(require_api_key_or_user)):
    """Return rows from `members_view`. If the view doesn't exist, attempt to create it
    from the `members` table (if present).
    """
    inspector = inspect(engine)
    views = []
    try:
        views = inspector.get_view_names()
    except Exception:
        # fallback: inspector may not support get_view_names on all dialects
        views = []

    if 'members_view' not in views:
        # Try to create a simple view from `members` table
        tables = inspector.get_table_names()
        if 'members' in tables:
            with engine.connect() as conn:
                conn.execute(text('CREATE VIEW members_view AS SELECT * FROM members'))
        else:
            raise HTTPException(status_code=404, detail="members_view not found and `members` table does not exist")

    # Read the view and return JSON rows
    try:
        df = pd.read_sql_table('members_view', con=engine)
        rows = df.to_dict(orient='records')
        actor_church = _auth_context_church(auth)
        if actor_church is not None:
            filtered_rows = []
            for r in rows:
                if _resolve_church_id(r.get('church')) == actor_church:
                    filtered_rows.append(r)
            rows = filtered_rows
        out = [{k: _serializable_value(v) for k, v in r.items()} for r in rows]
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read members_view: {e}")
