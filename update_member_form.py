#!/usr/bin/env python3
"""Update Edit Member form with group info toggle and unique member ID dropdown"""

import re

# Read the file
with open('frontend/src/App.jsx', 'r', encoding='utf-8') as f:
    content = f.read()

# Step 1: Add OFFICIAL_MEMBER_ID dropdown handling before FAMILY_ID handling
search_pattern = r"(\s*} else if\(key==='TRANSFER_DATE' \|\| key==='transfer_date'\)\{[\s\S]*?control = <input type='date'[\s\S]*?\/>)\s*} else if\(key==='FAMILY_ID'"

replacement = r"""\1
                    } else if(key==='OFFICIAL_MEMBER_ID' || key==='official_member_id'){
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
                    } else if(key==='FAMILY_ID'"""

content_new = re.sub(search_pattern, replacement, content)

# If regex didn't work, try simpler substring replacement
if content_new == content:
    print("Regex replacement didn't match, trying direct substring replacement...")
    search_str = """                    } else if(key==='TRANSFER_DATE' || key==='transfer_date'){
                      const v = val? new Date(val).toISOString().slice(0,10) : ''
                      control = <input type='date' value={v} onChange={e=> setMemberForm(prev=>({...prev, [key]: e.target.value || null}))} style={{width:'100%'}} />
                    } else if(key==='FAMILY_ID' || key==='family_id'"""
    
    replacement_str = """                    } else if(key==='TRANSFER_DATE' || key==='transfer_date'){
                      const v = val? new Date(val).toISOString().slice(0,10) : ''
                      control = <input type='date' value={v} onChange={e=> setMemberForm(prev=>({...prev, [key]: e.target.value || null}))} style={{width:'100%'}} />
                    } else if(key==='OFFICIAL_MEMBER_ID' || key==='official_member_id'){
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
                    } else if(key==='FAMILY_ID' || key==='family_id'"""
    
    if search_str in content:
        content_new = content.replace(search_str, replacement_str)
        print("✓ Direct substring replacement successful")
    else:
        print("✗ Could not find substring to replace")
        print("Available nearby content for manual inspection:")
        idx = content.find("TRANSFER_DATE' || key==='transfer_date'")
        if idx > 0:
            print(content[idx:idx+500])

# Write the file
with open('frontend/src/App.jsx', 'w', encoding='utf-8') as f:
    f.write(content_new)

print("✓ File updated successfully")
