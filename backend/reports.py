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
        df = df[
            df['church'].apply(
                lambda v: v is None or pd.isna(v) or int(v) == int(actor_church)
            )
        ]

    if 's2' in df.columns and (start_date or end_date):
        df['s2'] = pd.to_datetime(df['s2'], errors='coerce')
        if start_date:
            start_dt = pd.to_datetime(start_date, errors='coerce')
            if start_dt is not pd.NaT:
                df = df[df['s2'] >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date, errors='coerce')
            if end_dt is not pd.NaT:
                end_dt = end_dt + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
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

    for _, row in df.iterrows():
        col_name = str(row.get('collection_code') or '').strip()
        if not col_name:
            continue

        info = _get_info(col_name)
        col_lower = col_name.lower()

        # Preferred: read from the dynamic column whose name matches the code
        amount = 0.0
        if col_lower in df.columns:
            v = row.get(col_lower)
            try:
                amount = float(v) if v is not None and not (isinstance(v, float) and pd.isna(v)) else 0.0
            except Exception:
                amount = 0.0

        # Fallback: first non-zero numeric column in the row
        if amount == 0.0:
            for nc in all_numeric_cols:
                try:
                    nv = float(row.get(nc) or 0)
                    if nv != 0:
                        amount = nv
                        break
                except Exception:
                    pass

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
