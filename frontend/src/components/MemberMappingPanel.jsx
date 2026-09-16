import React from 'react'

const MODE_COPY = {
  member: {
    title: 'Map Member — Unique Identity for',
    description: <>Filter and transfer members to the right. Then click one name on the right to designate it as the <strong>Unique Identity</strong>. All selected members will be updated so both <strong>MEMBER_ID</strong> and <strong>OFFICIAL_MEMBER_ID</strong> use that designated member ID.</>,
    designateLabel: 'click to designate unique name',
    designatedPrefix: 'Designated unique name:',
    saveLabel: 'Save Unique Identity Mapping',
  },
  family: {
    title: 'Map Families — Family Group for',
    description: <>Filter and transfer members to the right. Then click one name on the right to designate as <strong>Family Name</strong>. All selected members will be updated so <strong>FAMILY_ID</strong> and <strong>DEFAULT_FAMILY_ID</strong> use that designated family member ID.</>,
    designateLabel: 'click to designate family name',
    designatedPrefix: 'Designated family name:',
    saveLabel: 'Save Family Mapping',
  },
}

export default function MemberMappingPanel({ mode, targetMember, members, filter, setFilter, right, setRight, designated, setDesignated, saveMessage, onSave, onCancel }){
  const copy = MODE_COPY[mode]
  const targetName = String(targetMember?.MEMBER_NAME || targetMember?.member_name || '').trim()
  const lowerFilter = filter.toLowerCase()
  const rightIds = new Set(right.map(r => r.id))

  let sourcePool = members || []
  let uniquePoolNote = ''
  if(mode === 'family'){
    const uniquePool = (members || []).filter(m => {
      const uid = m.OFFICIAL_MEMBER_ID || m.official_member_id
      return uid && Number(uid) > 0
    })
    sourcePool = uniquePool.length > 0 ? uniquePool : (members || [])
    if(uniquePool.length === 0) uniquePoolNote = ' (none found — showing all members)'
  }

  const lhsPool = sourcePool.filter(m => {
    if(rightIds.has(m.id)) return false
    if(!lowerFilter) return true
    return String(m.MEMBER_NAME || m.member_name || '').toLowerCase().includes(lowerFilter)
  })

  const designatedRow = right.find(r => r.id === designated)

  return (
    <div className={`mapping-panel mapping-panel--${mode}`}>
      <h4 className='mapping-panel-title'>{copy.title}: <strong>{targetName}</strong></h4>
      <p className='mapping-panel-desc'>{copy.description}{mode === 'family' ? ` LHS shows unique-identified members${uniquePoolNote}.` : ''}</p>

      <div className='form-row mb8'>
        <input placeholder='Filter members by name...' value={filter} onChange={e=>setFilter(e.target.value)} style={{flex:1}} />
        <button type='button' className='btn btn-sm' onClick={()=>setFilter('')}>Clear</button>
      </div>

      <div className='mapping-columns'>
        <div>
          <div className='mapping-col-label'>Available Members ({lhsPool.length})</div>
          <div className='mapping-pool'>
            {lhsPool.map(m=>{
              const nm = String(m.MEMBER_NAME||m.member_name||'').trim()
              const mid = m.MEMBER_ID||m.member_id||''
              const uid = m.OFFICIAL_MEMBER_ID||m.official_member_id||''
              return (
                <div key={m.id} className='mapping-pool-item' onDoubleClick={()=>setRight(prev=>[...prev, m])}>
                  <span>{nm}{mid ? ` (ID:${mid})` : ''}{mode === 'family' && uid ? ` [UID:${uid}]` : ''}</span>
                  <button type='button' className='btn btn-sm' onClick={()=>setRight(prev=>[...prev, m])}>&rarr;</button>
                </div>
              )
            })}
          </div>
          <div className='hint'>Double-click or click &rarr; to transfer</div>
        </div>

        <div className='mapping-middle'>
          <button type='button' className='btn btn-sm' onClick={()=>setRight(lhsPool)}>All &rarr;</button>
          <button type='button' className='btn btn-sm' onClick={()=>setRight([])}>&larr; All</button>
        </div>

        <div>
          <div className='mapping-col-label'>Selected ({right.length}) — {copy.designateLabel}</div>
          <div className='mapping-pool'>
            {right.map(m=>{
              const nm = String(m.MEMBER_NAME||m.member_name||'').trim()
              const mid = m.MEMBER_ID||m.member_id||''
              const isDesignated = designated === m.id
              return (
                <div
                  key={m.id}
                  onClick={()=>setDesignated(m.id)}
                  className={`mapping-pool-item${isDesignated ? ' mapping-pool-item--designated' : ''}`}
                >
                  <span>{isDesignated ? '★ ' : ''}{nm}{mid ? ` (ID:${mid})` : ''}</span>
                  <button type='button' className='btn btn-sm' onClick={e=>{e.stopPropagation(); setRight(prev=>prev.filter(r=>r.id!==m.id)); if(designated===m.id) setDesignated(null);}}>&times;</button>
                </div>
              )
            })}
          </div>
          {designatedRow && (
            <div className='mapping-designated-note'>&#9733; {copy.designatedPrefix} {String(designatedRow?.MEMBER_NAME||designatedRow?.member_name||'').trim()}</div>
          )}
        </div>
      </div>

      <div className='flex gap8 mt8'>
        <button type='button' className='btn btn-primary' disabled={!designated || right.length===0} onClick={onSave}>{copy.saveLabel}</button>
        <button type='button' className='btn btn-ghost' onClick={onCancel}>Cancel</button>
      </div>
      {saveMessage && <div className='mapping-save-message'>{saveMessage}</div>}
    </div>
  )
}
