from backend.db import deduplicate_members_by_sno, insert_member, engine

print('Running deduplication...')
del_count = deduplicate_members_by_sno()
print('Duplicate groups cleaned (count):', del_count)

print('Inserting test member without sno...')
new_id = insert_member(MEMBER_NAME='__TEST_NEW_MEMBER__')
print('Inserted id:', new_id)

# Show last 5 members
from sqlalchemy import text

with engine.connect() as conn:
    res = conn.execute(text('SELECT id, sno, MEMBER_NAME FROM members ORDER BY id DESC LIMIT 5'))
    rows = res.fetchall()
    print('Last 5 members:')
    for r in rows:
        print(r)
