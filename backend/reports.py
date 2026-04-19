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
    # ---- load transactions ------------------------------------------------
    df = pd.read_sql_table('members_collection', con=engine)

    if actor_church is not None and 'church' in df.columns:
        try:
            actor_church_num = int(actor_church)
        except Exception:
            actor_church_num = None
        if actor_church_num is not None:
            church_series = pd.to_numeric(df['church'], errors='coerce')
            df = df[(church_series.isna()) | (church_series == actor_church_num)]

    if 's2' in df.columns and (start_date or end_date):
        df['s2'] = _coerce_collection_datetime_series(df['s2'])
        start_dt = _parse_bound_date(start_date, end_of_day=False)
        end_dt = _parse_bound_date(end_date, end_of_day=True)
        if start_dt is not None:
            df = df[df['s2'] >= start_dt]
        if end_dt is not None:
            df = df[df['s2'] <= end_dt]

    empty_result = {
        'start_date': start_date,
        'end_date': end_date,
        'rows': [],
        'totals': {'conference': 0.0, 'local': 0.0, 'total': 0.0},
    }
    if df.empty:
        return empty_result

    # ---- build code map ---------------------------------------------------
    code_map = _build_code_map(actor_church)
    code_map = _apply_label_overrides(code_map)

    def _get_info(col_name: str) -> dict:
        return code_map.get(
            str(col_name or '').lower().strip(),
            {'label': str(col_name), 'scope': None, 'conf_pct': None, 'church': None},
        )

    # ---- numeric columns for fallback amount lookup -----------------------
    _non_amount_cols = {
        'id', 'church', 'member_id', 's1', 's2', 's3', 's4',
        's10', 's11', 's12', 'source', 'notes', 'collection_code',
        'added_at', 'updated_at',
    }
    all_numeric_cols = [
        c for c in df.columns
        if c not in _non_amount_cols and pd.api.types.is_numeric_dtype(df[c])
    ]

    # ---- aggregate --------------------------------------------------------
    summary: dict = {}  # col_name_lower → {label, conference, local, total}

    mapped_amount_cols = [c for c in code_map.keys() if c in df.columns]

    for _, row in df.iterrows():
        col_name = str(row.get('collection_code') or '').strip()
        col_lower = col_name.lower() if col_name else ''

        info = _get_info(col_name) if col_name else None
        amount = 0.0

        # Preferred: read from the dynamic column whose name matches collection_code.
        if col_lower and col_lower in df.columns:
            amount = _finite_amount(row.get(col_lower))

        # If collection_code is missing (legacy rows), infer item from a mapped amount column.
        if (not col_lower or info is None) and amount == 0.0:
            for mc in mapped_amount_cols:
                mv = _finite_amount(row.get(mc))
                if mv != 0.0:
                    col_lower = mc
                    info = _get_info(mc)
                    amount = mv
                    break

        # Generic fallback for rows with sparse/partial data.
        if amount == 0.0:
            for nc in all_numeric_cols:
                nv = _finite_amount(row.get(nc))
                if nv != 0.0:
                    amount = nv
                    break

        # If still no code, keep row visible as uncategorized instead of dropping it.
        if info is None:
            hint = str(row.get('s10') or row.get('s4') or '').strip()
            label = hint if hint else 'Uncategorized'
            info = {'label': label, 'scope': None, 'conf_pct': None, 'church': None}
            col_lower = f'uncategorized::{label.lower()}'

        conf, local = _split_amount(amount, info['scope'], info['conf_pct'])

        key = col_lower
        if key not in summary:
            summary[key] = {'label': info['label'], 'conference': 0.0, 'local': 0.0, 'total': 0.0}
        summary[key]['conference'] += conf
        summary[key]['local'] += local
        summary[key]['total'] += float(amount)

    # ---- format output ----------------------------------------------------
    out_rows = []
    for k in sorted(summary, key=lambda x: summary[x]['label'].lower()):
        s = summary[k]
        out_rows.append({
            'item':       s['label'],
            'conference': round(s['conference'], 2),
            'local':      round(s['local'], 2),
            'total':      round(s['total'], 2),
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
