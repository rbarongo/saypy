import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from .db import insert_member, create_tables

BASE_DIR = os.path.dirname(__file__)

# Prefer the workspace-level Members.xlsx path, then backend/ variations
POSSIBLE_FILES = [
    r'C:\_rwey\python\Saypy\saypy\Members.xlsx',
    os.path.join(os.path.dirname(BASE_DIR), 'Members.xlsx'),
    os.path.join(BASE_DIR, 'Members.xlsx'),
    os.path.join(BASE_DIR, 'Members.xls'),
    os.path.join(BASE_DIR, 'members.xlsx'),
    os.path.join(BASE_DIR, 'members.xls'),
]

EXPECTED = [
    'sno', 'MEMBER_NAME', 'MEMBER_ID', 'FAMILY_ID', 'DEFAULT_FAMILY_ID', 'OFFICIAL_MEMBER_ID',
    'pledge', 'GROUP_NAME', 'GROUP_ALIAS', 'DEFAULT_GROUP_ALIAS', 'GROUP_LEADER_ID', 'DEFAULT_GROUP_LEADER_ID',
    'STATUS', 'PHONE', 'PHONE2', 'EMAIL', 'RESIDENCE', 'church'
]

SYNONYMS = {
    'name': 'MEMBER_NAME',
    'member name': 'MEMBER_NAME',
    'member_name': 'MEMBER_NAME',
    'memberid': 'MEMBER_ID',
    'member id': 'MEMBER_ID',
    'id': 'MEMBER_ID',
    'family': 'FAMILY_ID',
    'family id': 'FAMILY_ID',
    'official_member_id': 'OFFICIAL_MEMBER_ID',
    'official id': 'OFFICIAL_MEMBER_ID',
    'phone': 'PHONE',
    'phone2': 'PHONE2',
    'email': 'EMAIL',
    'residence': 'RESIDENCE',
    'group': 'GROUP_NAME',
    'group_name': 'GROUP_NAME',
    'pledge': 'pledge',
    'church': 'church',
}


def find_file():
    for p in POSSIBLE_FILES:
        if os.path.exists(p):
            return p
    return None


def map_columns(df_cols):
    mapping = {}
    lowcols = {c.lower(): c for c in df_cols}
    for exp in EXPECTED:
        el = exp.lower()
        # direct match
        if el in lowcols:
            mapping[lowcols[el]] = exp
            continue
        # try contains
        found = None
        for lc, orig in lowcols.items():
            if el in lc or lc in el:
                found = orig
                break
        if found:
            mapping[found] = exp
    # fallback: try synonyms mapping
    for lc, orig in lowcols.items():
        if orig in mapping:
            continue
        key = lc.replace(' ', '').replace('_', '')
        if key in SYNONYMS:
            mapping[orig] = SYNONYMS[key]
    return mapping


def to_py(v):
    if pd.isna(v):
        return None
    return v


def run():
    print('Ensuring tables exist...')
    create_tables()
    f = find_file()
    if not f:
        print('Members.xlsx not found in expected locations:')
        for p in POSSIBLE_FILES:
            print('  -', p)
        return

    print('Reading', f)
    df = pd.read_excel(f)
    if df.empty:
        print('No rows found in Excel file')
        return

    col_map = map_columns(list(df.columns))
    print('Detected columns mapping (source -> target):')
    for src, tgt in col_map.items():
        print(f'  {src} -> {tgt}')

    inserted = 0
    errors = 0
    for _, row in df.iterrows():
        try:
            kwargs = {}
            for src, tgt in col_map.items():
                val = to_py(row[src])
                if val is None:
                    kwargs[tgt] = None
                else:
                    # simple coercions
                    if tgt in ('sno','MEMBER_ID','FAMILY_ID','DEFAULT_FAMILY_ID','OFFICIAL_MEMBER_ID','GROUP_LEADER_ID','DEFAULT_GROUP_LEADER_ID'):
                        try:
                            kwargs[tgt] = int(val)
                        except Exception:
                            kwargs[tgt] = None
                    elif tgt == 'pledge':
                        try:
                            kwargs[tgt] = float(val)
                        except Exception:
                            kwargs[tgt] = None
                    else:
                        kwargs[tgt] = str(val).strip()

            # ensure required key MEMBER_NAME exists
            if 'MEMBER_NAME' not in kwargs or not kwargs.get('MEMBER_NAME'):
                # try to build from other columns
                possible = None
                for c in df.columns:
                    if 'name' in c.lower():
                        possible = to_py(row[c])
                        break
                if possible:
                    kwargs['MEMBER_NAME'] = str(possible)

            insert_member(**kwargs)
            inserted += 1
        except Exception as e:
            print('Row insert error:', e)
            errors += 1

    print(f'Inserted: {inserted}, errors: {errors}')


if __name__ == '__main__':
    run()
