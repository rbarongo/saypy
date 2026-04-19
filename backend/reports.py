"""reports.py — Backend report view functions.

Each function computes a report from the database and returns a plain Python
dict that the API layer can return directly as JSON.  The functions are
intentionally free of FastAPI types (no HTTPException, no Request) so that
they can also be called offline (CLI, tests, scheduled jobs) without spinning
up the web server.

Separation of concerns
-----------------------
  reports.py  → pure query + aggregation logic (change business rules here)
  app.py      → auth checks, dependency injection, HTTP plumbing

Adding a new report
-------------------
  1. Write a function below following the ``compute_<report_name>`` convention.
  2. Add a thin ``@app.get('/reports/<name>')`` endpoint in ``app.py`` that
     calls it and returns the result.
"""

from __future__ import annotations

from typing import Optional
import math
import re

import pandas as pd
from sqlalchemy import text

from .db import engine


# ---------------------------------------------------------------------------
# Helpers shared across report functions
# ---------------------------------------------------------------------------

def _build_code_map(actor_church: Optional[int]) -> dict:
    """Return a column_name → info dict for collection codes visible to the church.

    Church-specific rows take precedence over global (church IS NULL) rows.
    Each entry has:
        label        – display name
        scope        – 'local' | 'conference' | 'split' | None  (None → local)
        conf_pct     – float 0-100 used when scope='split'
        church       – church id or None (for global codes)
    """
    ch = actor_church if actor_church is not None else -1
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT id, column_name, code, custom_collection_name, church,
                       scope, conference_split_pct
                FROM collection_codes
                WHERE church = :ch OR church IS NULL
                ORDER BY CASE WHEN church = :ch THEN 0 ELSE 1 END, id
            """),
            {'ch': ch},
        )
        cc_rows = result.fetchall()

    code_map: dict = {}
    for ccr in cc_rows:
        d = dict(ccr._mapping)
        col = str(d.get('column_name') or '').lower().strip()
        if not col:
            continue
        # If we already have a church-specific entry, skip incoming global row.
        if col in code_map and code_map[col]['church'] is not None and d.get('church') is None:
            continue
        label = str(
            d.get('custom_collection_name') or d.get('code') or d.get('column_name') or col
        )
        scope_raw = str(d.get('scope') or '').lower().strip()
        scope = scope_raw if scope_raw else None
        try:
            conf_pct: Optional[float] = float(d['conference_split_pct']) if d.get('conference_split_pct') is not None else None
        except Exception:
            conf_pct = None
        code_map[col] = {
            'label': label,
            'scope': scope,
            'conf_pct': conf_pct,
            'church': d.get('church'),
        }
    return code_map


def _build_code_alias_map(actor_church: Optional[int]) -> dict:
    """Return alias -> canonical column_name for visible collection codes.

    Aliases include: column_name, code, custom_collection_name.
    """
    ch = actor_church if actor_church is not None else -1
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT column_name, code, custom_collection_name, church
                FROM collection_codes
                WHERE church = :ch OR church IS NULL
                ORDER BY CASE WHEN church = :ch THEN 0 ELSE 1 END, id
                """
            ),
            {'ch': ch},
        )
        rows = result.fetchall()

    alias_map: dict = {}
    for rr in rows:
        d = dict(rr._mapping)
        col = str(d.get('column_name') or '').strip().lower()
        if not col:
            continue
        for alias_raw in (d.get('column_name'), d.get('code'), d.get('custom_collection_name')):
            alias = str(alias_raw or '').strip().lower()
            if not alias:
                continue
            if alias not in alias_map:
                alias_map[alias] = col
    return alias_map


# ---------------------------------------------------------------------------
# Hard-coded label overrides
# These apply when the operator has not yet set the scope field in the DB.
# Edit this dict to change the default split rules without touching other code.
# ---------------------------------------------------------------------------
LABEL_OVERRIDES: dict[str, dict] = {
    # keyword (matched case-insensitively inside the label string): scope rules
    'zaka':     {'scope': 'conference', 'conf_pct': 100.0},
    'sadaka':   {'scope': 'split',      'conf_pct': 58.0},
    'shukrani': {'scope': 'split',      'conf_pct': 58.0},
}


def _apply_label_overrides(code_map: dict) -> dict:
    """Apply LABEL_OVERRIDES to any code whose scope is still unset."""
    result = {}
    for col, info in code_map.items():
        if info['scope']:
            result[col] = info
            continue
        label_lower = info['label'].lower().strip()
        matched = None
        for keyword, ov in LABEL_OVERRIDES.items():
            if keyword in label_lower:
                matched = ov
                break
        if matched:
            result[col] = {**info, 'scope': matched['scope'], 'conf_pct': matched['conf_pct']}
        else:
            result[col] = info
    return result


def _split_amount(amount: float, scope: Optional[str], conf_pct: Optional[float]):
    """Return (conference_amount, local_amount) for a given scope rule."""
    amt = float(amount or 0)
    if scope == 'conference':
        return amt, 0.0
    if scope == 'split' and conf_pct is not None:
        conf = round(amt * float(conf_pct) / 100, 2)
        return conf, round(amt - conf, 2)
    # default: local
    return 0.0, amt


def _finite_amount(value) -> float:
    """Best-effort numeric coercion returning 0.0 for null/non-finite values."""
    try:
        amt = float(value)
        if not math.isfinite(amt):
            return 0.0
        return amt
    except Exception:
        return 0.0


def _parse_bound_date(value: Optional[str], *, end_of_day: bool = False):
    """Parse API date bound strings with fallback for day-first formats."""
    if not value:
        return None
    dt = pd.to_datetime(value, errors='coerce')
    if pd.isna(dt):
        dt = pd.to_datetime(value, errors='coerce', dayfirst=True)
    if pd.isna(dt):
        return None
    if end_of_day:
        txt = str(value).strip()
        if len(txt) == 10:
            dt = dt + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    return dt


def _coerce_collection_datetime_series(series: pd.Series) -> pd.Series:
    """Coerce collection date column from mixed string/numeric formats.

    Handles ISO strings, day-first strings, and Excel serial day numbers.
    """
    dt = pd.to_datetime(series, errors='coerce')
    still_bad = dt.isna()

    if still_bad.any():
        dt_dayfirst = pd.to_datetime(series, errors='coerce', dayfirst=True)
        dt = dt.where(~still_bad, dt_dayfirst)
        still_bad = dt.isna()

    if still_bad.any():
        numeric = pd.to_numeric(series, errors='coerce')
        excel_mask = still_bad & numeric.notna() & (numeric > 20000) & (numeric < 90000)
        if excel_mask.any():
            excel_dt = pd.to_datetime(numeric[excel_mask], unit='D', origin='1899-12-30', errors='coerce')
            dt.loc[excel_mask] = excel_dt

    return dt


def _is_contribution_code(col_name: str) -> bool:
    """Legacy compatibility wrapper (kept for external references)."""
    return _is_reportable_code(col_name, col_name)


def _is_total_like(text_value: str) -> bool:
    t = str(text_value or '').strip().lower()
    if not t:
        return False
    return ('jumla' in t) or ('total' in t)


def _is_reportable_code(column_name: str, label: str) -> bool:
    """Return True only for collection items that should appear in Period Summary."""
    c = str(column_name or '').strip().lower()
    if not c:
        return False

    # Technical/meta/total fields should never be report rows.
    non_item_cols = {
        's1', 's2', 's3', 's4', 's5',
        's7', 's8', 's9',
        's10', 's11', 's12', 's14',
    }
    if c in non_item_cols:
        return False

    if _is_total_like(c) or _is_total_like(label):
        return False

    return True


def _iter_members_collection_chunks(chunk_size: int = 5000):
    """Yield members_collection rows in manageable chunks."""
    query = text('SELECT * FROM members_collection')
    yield from pd.read_sql_query(query, con=engine, chunksize=chunk_size)


def _safe_numeric_series(df: pd.DataFrame, col_name: str) -> pd.Series:
    if col_name in df.columns:
        s = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0)
        return s.where(pd.Series([math.isfinite(float(v)) for v in s], index=s.index), 0.0)
    return pd.Series([0.0] * len(df), index=df.index, dtype='float64')


# ---------------------------------------------------------------------------
# Report: Period Summary
# ---------------------------------------------------------------------------

def compute_period_summary(
    actor_church: Optional[int],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """Aggregate collection entries into a period summary table.

    Returns a dict::

        {
          "start_date": str | None,
          "end_date":   str | None,
          "rows": [
              {"item": str, "conference": float, "local": float, "total": float},
              ...
          ],
          "totals": {"conference": float, "local": float, "total": float}
        }

    Rules applied per collection item
    ----------------------------------
    The ``scope`` and ``conference_split_pct`` fields on **collection_codes**
    control how each item's total is split:

    * ``scope='conference'``  → 100 % goes to conference column
    * ``scope='local'``       → 100 % goes to local column  (also the default)
    * ``scope='split'``       → conference = amount × conf_pct / 100,
                                local      = amount × (100 - conf_pct) / 100

    Hard-coded overrides (``LABEL_OVERRIDES`` dict at module level) apply when
    the operator has not yet explicitly set the scope in the DB:

    * Items whose label contains **"zaka"**     → conference 100 %
    * Items whose label contains **"sadaka"**   → split 58 % conf / 42 % local
    * Items whose label contains **"shukrani"** → split 58 % conf / 42 % local

    To permanently change these defaults, edit ``LABEL_OVERRIDES`` above.
    To override for a specific church, set the ``scope`` / ``conference_split_pct``
    columns on the relevant ``collection_codes`` row via the admin UI.
    """
    empty_result = {
        'start_date': start_date,
        'end_date': end_date,
        'rows': [],
        'totals': {'conference': 0.0, 'local': 0.0, 'total': 0.0},
    }

    # ---- build code map ---------------------------------------------------
    code_map = _build_code_map(actor_church)
    code_map = _apply_label_overrides(code_map)
    code_map = {
        k: v for k, v in code_map.items()
        if _is_reportable_code(k, v.get('label'))
    }
    code_alias_map = _build_code_alias_map(actor_church)
    code_alias_map = {k: v for k, v in code_alias_map.items() if v in code_map}

    if not code_map:
        return empty_result

    # ---- aggregation state -------------------------------------------------
    totals_by_code = {code: 0.0 for code in code_map.keys()}
    any_rows = False

    start_dt = _parse_bound_date(start_date, end_of_day=False)
    end_dt = _parse_bound_date(end_date, end_of_day=True)

    # ---- process transactions in chunks -----------------------------------
    for df in _iter_members_collection_chunks(chunk_size=5000):
        if df is None or df.empty:
            continue

        if actor_church is not None and 'church' in df.columns:
            try:
                actor_church_num = int(actor_church)
            except Exception:
                actor_church_num = None
            if actor_church_num is not None:
                church_series = pd.to_numeric(df['church'], errors='coerce')
                df = df[(church_series.isna()) | (church_series == actor_church_num)]

        if 's2' in df.columns and (start_dt is not None or end_dt is not None):
            date_series = _coerce_collection_datetime_series(df['s2'])
            # Uploaded files may miss canonical s2 mapping; fall back to added_at when present.
            if 'added_at' in df.columns:
                added_series = _coerce_collection_datetime_series(df['added_at'])
                date_series = date_series.where(date_series.notna(), added_series)
            df['s2'] = date_series
            if start_dt is not None:
                df = df[df['s2'] >= start_dt]
            if end_dt is not None:
                df = df[df['s2'] <= end_dt]

        if df.empty:
            continue

        any_rows = True

        # Resolve collection_code values to canonical code keys.
        if 'collection_code' in df.columns:
            resolved_code = df['collection_code'].apply(
                lambda v: code_alias_map.get(str(v or '').strip().lower(), str(v or '').strip().lower())
            )
        else:
            resolved_code = pd.Series([''] * len(df), index=df.index)

        s5_series = _safe_numeric_series(df, 's5')

        # Aggregate strictly by configured collection codes.
        for code in code_map.keys():
            col_series = _safe_numeric_series(df, code)
            chunk_total = float(col_series.sum())

            # Form submissions often keep amount in s5 while collection_code points to the item.
            if 's5' in df.columns:
                match_code = (resolved_code == code)
                # Avoid double count when the code column itself already carries amount.
                use_s5 = match_code & (col_series == 0)
                chunk_total += float(s5_series[use_s5].sum())

            totals_by_code[code] = totals_by_code.get(code, 0.0) + chunk_total

    if not any_rows:
        return empty_result

    # ---- format output ----------------------------------------------------
    out_rows = []
    for k in sorted(code_map, key=lambda x: str(code_map[x].get('label') or x).lower()):
        amount = float(totals_by_code.get(k, 0.0) or 0.0)
        if amount == 0.0:
            continue
        info = code_map[k]
        conf_amt, local_amt = _split_amount(amount, info.get('scope'), info.get('conf_pct'))
        out_rows.append({
            'item':       str(info.get('label') or k),
            'conference': round(conf_amt, 2),
            'local':      round(local_amt, 2),
            'total':      round(amount, 2),
        })

    totals = {
        'conference': round(sum(r['conference'] for r in out_rows), 2),
        'local':      round(sum(r['local']      for r in out_rows), 2),
        'total':      round(sum(r['total']       for r in out_rows), 2),
    }

    return {
        'start_date': start_date,
        'end_date':   end_date,
        'rows':       out_rows,
        'totals':     totals,
    }
