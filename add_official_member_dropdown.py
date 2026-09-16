#!/usr/bin/env python3
"""Add OFFICIAL_MEMBER_ID dropdown handling to Edit Member form"""

# Read the file
with open('frontend/src/App.jsx', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the line with "TRANSFER_DATE' || key==='transfer_date'" and locate the closing brace
# Then find the next "FAMILY_ID" and insert the new code before it

insert_index = -1
for i, line in enumerate(lines):
    if ("TRANSFER_DATE' || key==='transfer_date'" in line and
        i < len(lines) - 30):
        # Found TRANSFER_DATE line, now look for FAMILY_ID after it
        for j in range(i, min(i+30, len(lines))):
            if "} else if(key==='FAMILY_ID'" in lines[j]:
                insert_index = j
                break
        break

if insert_index > 0:
    print(f"Found insertion point at line {insert_index}")
    
    # The code to insert
    new_code = '''                    } else if(key==='OFFICIAL_MEMBER_ID' || key==='official_member_id'){
                      const displayControl = (
                        <input placeholder={key} value={val||''} type='number' onChange={e=>setMemberForm(prev=>({...prev,[key]: e.target.value===''? null: Number(e.target.value) }))} style={{width:'100%'}} />
                      )
                      const selectedMemberId = val
                      mappedDisplay = (
                        <select value={selectedMemberId||''} onChange={e=>{
                          const selected = e.target.value
                          const memberId = selected === '' ? null : Number(selected)
                          setMemberForm(prev=>({...prev,[key]: memberId}))
                        }} style={{width:'100%'}}>
                          <option value=''>-- select unique member --</option>
                          {(members || []).map(m=> (
                            <option key={m.id} value={m.MEMBER_ID || ''}>{(m.MEMBER_NAME || '')} (ID: {m.MEMBER_ID || 'N/A'})</option>
                          ))}
                        </select>
                      )
                      control = displayControl
'''
    
    # Insert the new code before the FAMILY_ID line
    lines.insert(insert_index, new_code)
    
    # Write back to file
    with open('frontend/src/App.jsx', 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    print("✓ Successfully inserted OFFICIAL_MEMBER_ID dropdown code")
else:
    print("✗ Could not find insertion point for OFFICIAL_MEMBER_ID code")
