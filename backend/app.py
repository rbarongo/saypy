from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Depends
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
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
    create_token_for_user,
    get_user_by_token,
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import inspect, text
from typing import List, Optional
from pydantic import BaseModel, ValidationError
from decimal import Decimal
from datetime import datetime
import numpy as np
import math

app = FastAPI(title="KSC Migration API")

bearer_scheme = HTTPBearer(auto_error=False)


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing or invalid authorization token")
    token = credentials.credentials
    user = get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")
    return user


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
        return {'api_key': None, 'uploader': None, 'user': user}

    raise HTTPException(status_code=401, detail='Missing authentication (API key or Bearer token)')

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
        if name.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file.file)
        elif name.lower().endswith('.csv') or file.content_type == 'text/csv':
            df = pd.read_csv(file.file)
        else:
            # try excel first
            try:
                df = pd.read_excel(file.file)
            except Exception:
                file.file.seek(0)
                df = pd.read_csv(file.file)
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
    # if uploader provided, apply defaults
    if uploader:
        try:
            if uploader.get('church'):
                out_df['church'] = out_df.get('church', pd.Series([uploader.get('church')] * len(out_df)))
            if uploader.get('name'):
                out_df['source'] = out_df.get('source', pd.Series([uploader.get('name')] * len(out_df)))
        except Exception:
            pass
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
async def submit_form(table_name: str, request: Request):
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

    # Normalize: convert single-value lists to values
    row = {k: (v[0] if isinstance(v, (list, tuple)) and len(v) == 1 else v) for k, v in payload.items()}
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
    PHONE: Optional[str] = None
    PHONE2: Optional[str] = None
    EMAIL: Optional[str] = None
    RESIDENCE: Optional[str] = None
    church: Optional[int] = None


class MemberCollectionIn(BaseModel):
    collection_code: str
    member_id: int | None = None


@app.post('/members')
def create_member(payload: MemberIn):
    """Create a member record and return its id."""
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
        STATUS=payload.STATUS,
        PHONE=payload.PHONE,
        PHONE2=payload.PHONE2,
        EMAIL=payload.EMAIL,
        RESIDENCE=payload.RESIDENCE,
        church=payload.church,
    )
    return {"id": pk}


@app.post('/members_collection')
def create_members_collection(payload: MemberCollectionIn):
    """Create a members_collection row. `member_id` may be omitted if you will link later."""
    pk = insert_members_collection(collection_code=payload.collection_code, member_id=payload.member_id)
    return {"id": pk}

@app.put('/members_collection/{row_id}')
def update_members_collection(row_id: int, payload: dict):
    """Update a members_collection row by id. Accepts any subset of columns present in the table."""
    try:
        # limit updates to known columns to avoid SQL injection
        cols = get_target_columns('members_collection')
        if not cols:
            raise HTTPException(status_code=500, detail='members_collection table not found')
        update_cols = {k: v for k, v in payload.items() if k in cols and k != 'id'}
        if not update_cols:
            raise HTTPException(status_code=400, detail='No updatable columns provided')
        set_parts = ', '.join([f"{c}=:{c}" for c in update_cols.keys()])
        params = dict(update_cols)
        params['id'] = row_id
        with engine.connect() as conn:
            conn.execute(text(f"UPDATE members_collection SET {set_parts} WHERE id=:id"), params)
            try:
                conn.commit()
            except Exception:
                pass
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/members_collections/bulk')
def bulk_insert_members_collections(rows: List[dict], auth: dict = Depends(require_api_key_or_user), request: Request = None):
    """Accept a list of dicts and insert into `members_collection` in bulk."""
    received_count = len(rows) if rows is not None else 0
    if not rows:
        raise HTTPException(status_code=400, detail={"message": "No rows provided", "received": received_count})
    # Pre-process rows: compute s1 when possible, resolve church names
    # If API key present, use uploader defaults
    uploader = auth.get('uploader') if isinstance(auth, dict) else None
    errors = []
    valid_rows = []
    for i, r in enumerate(rows):
        row = dict(r)
        # resolve church name -> id if necessary
        church_val = row.get('church')
        # if uploader present and no church provided, set uploader's church
        if (church_val is None or church_val == '') and uploader and uploader.get('church'):
            row['church'] = uploader.get('church')
        if church_val is not None and not isinstance(church_val, int):
            try:
                with engine.connect() as conn:
                    res = conn.execute(text('SELECT id FROM church WHERE name=:n'), {'n': str(church_val)})
                    try:
                        cid = res.scalar()
                    except Exception:
                        rr = res.fetchone()
                        cid = rr[0] if rr else None
                if cid:
                    row['church'] = int(cid)
            except Exception:
                pass

        # compute s1 if missing or placeholder (e.g. 1)
        try:
            s1_raw = row.get('s1')
        except Exception:
            s1_raw = None
        if not s1_raw or (isinstance(s1_raw, (int, str)) and str(s1_raw).strip() == '1'):
            try:
                s2val = row.get('s2')
                if isinstance(s2val, str):
                    s2dt = datetime.fromisoformat(s2val)
                elif isinstance(s2val, datetime):
                    s2dt = s2val
                else:
                    s2dt = None
                s3val = row.get('s3')
                s3int = int(s3val) if s3val is not None and str(s3val).strip() != '' else None
                church_id = row.get('church') or (uploader.get('church') if uploader else 1)
                if s2dt and s3int is not None:
                    ymd = s2dt.strftime('%Y%m%d')
                    row['s1'] = int(f"{ymd}{int(church_id):03d}{int(s3int):03d}")
            except Exception:
                pass
        # coerce s1 to int for numeric s1 requirement
        if row.get('s1') is not None and not isinstance(row.get('s1'), int):
            try:
                row['s1'] = int(row['s1'])
            except Exception:
                try:
                    import re
                    digs = re.sub(r'[^0-9]', '', str(row['s1']))
                    row['s1'] = int(digs) if digs else row['s1']
                except Exception:
                    pass
        # set source from uploader if not present
        try:
            if uploader and not row.get('source'):
                row['source'] = uploader.get('name')
        except Exception:
            pass

        try:
            validated = MembersCollectionRow(**row)
            valid_rows.append(validated.dict())
        except ValidationError as ve:
            errors.append({"index": i, "errors": ve.errors()})

    if errors:
        # echo received count and rows for debugging
        raise HTTPException(status_code=422, detail={"received": received_count, "validation_errors": errors, "rows": rows})

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
def list_collection_codes():
    try:
        df = pd.read_sql_table('collection_codes', con=engine)
        rows = df.to_dict(orient='records')
        out = [{k: _serializable_value(v) for k, v in r.items()} for r in rows]
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class MembersCollectionRow(BaseModel):
    collection_code: Optional[str] = None
    member_id: Optional[int] = None
    # Required: s1,s2,s3,s4. s1 may be computed if missing or placeholder before validation.
    s1: int
    s2: datetime
    s3: int
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
    errors = []
    out_rows = []
    # If API key present, use uploader to default church/source
    uploader = auth.get('uploader') if isinstance(auth, dict) else None

    for i, r in enumerate(rows):
        row = dict(r)
        # Normalize church: allow church name or id
        church_val = row.get('church')
        if (church_val is None or church_val == '') and uploader and uploader.get('church'):
            row['church'] = uploader.get('church')
        if church_val is not None and not isinstance(church_val, int):
            # try to resolve name -> id
            try:
                with engine.connect() as conn:
                    res = conn.execute(text('SELECT id FROM church WHERE name=:n'), {'n': str(church_val)})
                    try:
                        cid = res.scalar()
                    except Exception:
                        rr = res.fetchone()
                        cid = rr[0] if rr else None
                if cid:
                    row['church'] = int(cid)
            except Exception:
                # leave as-is; validation will catch missing church if required elsewhere
                pass

        # Compute s1 if missing or placeholder and s2/s3 present
        try:
            s1_raw = row.get('s1')
        except Exception:
            s1_raw = None
        if not s1_raw or (isinstance(s1_raw, (int, str)) and str(s1_raw).strip() == '1'):
            try:
                s2val = row.get('s2')
                if isinstance(s2val, str):
                    s2dt = datetime.fromisoformat(s2val)
                elif isinstance(s2val, datetime):
                    s2dt = s2val
                else:
                    s2dt = None
                s3val = row.get('s3')
                s3int = int(s3val) if s3val is not None and str(s3val).strip() != '' else None
                church_id = row.get('church') or 1
                if s2dt and s3int is not None:
                    ymd = s2dt.strftime('%Y%m%d')
                    row['s1'] = int(f"{ymd}{int(church_id):03d}{int(s3int):03d}")
            except Exception:
                pass

        # coerce s1 to int to satisfy numeric s1 requirement
        if row.get('s1') is not None and not isinstance(row.get('s1'), int):
            try:
                row['s1'] = int(row['s1'])
            except Exception:
                try:
                    # fallback: remove non-digits then parse
                    import re
                    digs = re.sub(r'[^0-9]', '', str(row['s1']))
                    row['s1'] = int(digs) if digs else row['s1']
                except Exception:
                    pass

        try:
            MembersCollectionRow(**row)
            out_rows.append(row)
        except ValidationError as ve:
            errors.append({"index": i, "errors": ve.errors()})
    return {"validation_errors": errors, "rows": out_rows}


class CollectionCodeIn(BaseModel):
    column_name: str
    code: str | None = None


@app.post('/collection_codes')
def create_collection_code(payload: CollectionCodeIn):
    try:
        with engine.connect() as conn:
            res = conn.execute(text("INSERT INTO collection_codes (column_name, code) VALUES (:cn, :c)"), {"cn": payload.column_name, "c": payload.code})
            try:
                conn.commit()
            except Exception:
                pass
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/header_mappings')
def save_header_mappings(arr: List[dict]):
    """Save provided header->mapped_column mappings. Expects list of {header_name, mapped_column}."""
    try:
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
        church_id = payload.church if payload.church is not None else current_user.get('church')
        key = create_uploader(payload.name, church_id)
        return {"api_key": key}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/uploaders')
def list_uploaders_endpoint():
    try:
        out = list_uploaders()
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/uploaders/{api_key}')
def get_uploader_by_key_endpoint(api_key: str):
    try:
        u = get_uploader_by_key(api_key)
        if not u:
            raise HTTPException(status_code=404, detail="Uploader not found")
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


@app.post('/users/register')
def register_user(payload: UserRegister, credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    try:
        # Default role to 'uploader' unless an admin is creating a different role
        role = payload.role or 'uploader'
        if role != 'uploader':
            # require bearer token of an admin user to create non-uploader roles
            if not credentials or not credentials.credentials:
                raise HTTPException(status_code=403, detail='Only admins can create users with elevated roles')
            creator = get_user_by_token(credentials.credentials)
            if not creator or creator.get('role') != 'admin':
                raise HTTPException(status_code=403, detail='Only admins can create users with elevated roles')

        out = create_user(payload.username, payload.password, payload.church, role=role)
        return out
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/users/login')
def login_user(payload: UserIn):
    u = verify_user(payload.username, payload.password)
    if not u:
        raise HTTPException(status_code=401, detail='Invalid username or password')
    token = create_token_for_user(u['id'])
    return {"token": token, "user": u}


@app.get('/users')
def list_users(current_user: dict = Depends(get_current_user)):
    # only admins may list users
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='Not authorized')
    try:
        with engine.connect() as conn:
            res = conn.execute(text('SELECT id, username, church, role, created_at FROM users'))
            rows = [dict(r) for r in res.fetchall()]
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put('/users/{user_id}')
def update_user(user_id: int, payload: UserRegister, current_user: dict = Depends(get_current_user)):
    # only admins may update users
    if current_user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='Not authorized')
    try:
        # update username, church, role, and optionally password
        with engine.connect() as conn:
            if payload.password:
                # create new salt/hash
                salt = os.urandom(16)
                ph = _hash_password(payload.password, salt)
                salt_hex = binascii.hexlify(salt).decode('ascii')
                conn.execute(text('UPDATE users SET username=:u, password_hash=:ph, salt=:s, church=:c, role=:r WHERE id=:id'),
                             {'u': payload.username, 'ph': ph, 's': salt_hex, 'c': payload.church, 'r': (payload.role or 'uploader'), 'id': user_id})
            else:
                conn.execute(text('UPDATE users SET username=:u, church=:c, role=:r WHERE id=:id'),
                             {'u': payload.username, 'c': payload.church, 'r': (payload.role or 'uploader'), 'id': user_id})
            try:
                conn.commit()
            except Exception:
                pass
        return {'ok': True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/churches')
def list_churches():
    try:
        df = pd.read_sql_table('church', con=engine)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put('/collection_codes/{code_id}')
def update_collection_code(code_id: int, payload: CollectionCodeIn):
    try:
        with engine.connect() as conn:
            conn.execute(text("UPDATE collection_codes SET column_name=:cn, code=:c WHERE id=:id"), {"cn": payload.column_name, "c": payload.code, "id": code_id})
            try:
                conn.commit()
            except Exception:
                pass
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/members')
def list_members(q: Optional[str] = None):
    try:
        df = pd.read_sql_table('members', con=engine)
        if q:
            # Search MEMBER_NAME text or exact MEMBER_ID when numeric
            try:
                qnum = int(q)
            except Exception:
                qnum = None
            mask = df['MEMBER_NAME'].astype(str).str.contains(q, case=False, na=False)
            if qnum is not None:
                mask = mask | (df['MEMBER_ID'] == qnum)
            df = df[mask]
        rows = df.to_dict(orient='records')
        out = [{k: _serializable_value(v) for k, v in r.items()} for r in rows]
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put('/members/{member_id}')
def update_member(member_id: int, payload: MemberIn):
    try:
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                    UPDATE members SET
                        sno=:sno,
                        MEMBER_NAME=:mn,
                        MEMBER_ID=:mid,
                        FAMILY_ID=:fid,
                        DEFAULT_FAMILY_ID=:dfid,
                        OFFICIAL_MEMBER_ID=:omid,
                        pledge=:pledge,
                        GROUP_NAME=:gname,
                        GROUP_ALIAS=:galias,
                        DEFAULT_GROUP_ALIAS=:dgalias,
                        GROUP_LEADER_ID=:gleader,
                        DEFAULT_GROUP_LEADER_ID=:dgleader,
                        STATUS=:status,
                        PHONE=:phone,
                        PHONE2=:phone2,
                        EMAIL=:email,
                        RESIDENCE=:res
                            ,
                            church=:church
                    WHERE id=:id
                    """
                ),
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
                    "status": payload.STATUS,
                    "phone": payload.PHONE,
                    "phone2": payload.PHONE2,
                    "email": payload.EMAIL,
                    "res": payload.RESIDENCE,
                    "church": payload.church,
                    "id": member_id,
                },
            )
            try:
                conn.commit()
            except Exception:
                pass
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/reports/members_collections')
def report_members_collections(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Return members_collection rows, optionally filtered by s2 (date) range. Dates in ISO format."""
    try:
        # Read full table into pandas then filter by s2 in Python to avoid SQL param dialect issues
        df = pd.read_sql_table('members_collection', con=engine)
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
        rows = df.to_dict(orient='records')
        out = [{k: _serializable_value(v) for k, v in r.items()} for r in rows]
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.on_event("startup")
def on_startup():
    try:
        create_tables()
    except Exception:
        # Do not crash the app on startup table creation errors; log would be better in production
        pass

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
def get_members_view():
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
        out = [{k: _serializable_value(v) for k, v in r.items()} for r in rows]
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read members_view: {e}")
