import React from 'react'
import ColumnPicker from './ColumnPicker'
import MemberMappingPanel from './MemberMappingPanel'
import Card from './Card'
import Modal from './Modal'
import PageHeader from './members-core/PageHeader'

export default function MembersPage({
  membersQ, setMembersQ, fetchMembers,
  membersSearchField, setMembersSearchField,
  membersSearchText, setMembersSearchText,
  addMembersFilter, clearMembersFilters,
  setEditingMember, setMemberForm, setShowMemberGroupInfo, setShowMembershipInfo, setShowMemberForm,
  showMembersColumnPicker, setShowMembersColumnPicker,
  getMembersAllColumns, getMembersDisplayColumns, memberLabelForColumn, memberDisplayCellValue,
  setMembersVisibleCols, saveMembersColumnSelection,
  exportMembersExcel, exportMembersPdf,
  membersMaxRows, setMembersMaxRows,
  filteredMembers,
  membersExtraFilters, updateMembersFilter, removeMembersFilter,
  membersFilterableFields, membersFieldLabel,
  membersSortKey, setMembersSortKey, membersSortDir, setMembersSortDir,
  showMemberForm, editingMember, memberForm,
  membersFields,
  showMemberGroupInfo, showMembershipInfo,
  churches, canAccessChurch, denyRestrictedChurchAccess, user,
  showTransferChurchCreator, setShowTransferChurchCreator, newTransferChurchName, setNewTransferChurchName, createTransferChurchOption,
  members,
  authFetch, setStatus,
  nextMemberNumericValue,
  mapMode, setMapMode, mapTargetMember, setMapTargetMember, mapFilter, setMapFilter, mapRight, setMapRight, mapDesignated, setMapDesignated, mapSaveMessage, setMapSaveMessage,
  mainGridWrapStyle, mainGridTableStyle,
}) {
  const loadedCount = Array.isArray(members) ? members.length : 0
  const visibleCount = Array.isArray(filteredMembers) ? filteredMembers.length : 0
  const activeCount = (filteredMembers || []).filter(m => String(m?.STATUS || m?.status || '').toLowerCase() === 'active').length
  const inactiveCount = (filteredMembers || []).filter(m => String(m?.STATUS || m?.status || '').toLowerCase().startsWith('inactive')).length
  const churchesRepresented = new Set((filteredMembers || []).map(m => m?.church).filter(v => v !== null && v !== undefined && v !== '')).size

  function closeMemberForm() {
    setShowMemberForm(false); setEditingMember(null); setShowMemberGroupInfo(false); setShowMembershipInfo(false); setMemberForm({})
  }

  function startMapping(mode) {
    setMapTargetMember(editingMember); setMapMode(mode); setMapFilter(''); setMapRight([]); setMapDesignated(null); setMapSaveMessage(''); setShowMemberForm(false)
  }

  function cancelMapping() {
    setMapMode(null); setMapTargetMember(null); setMapRight([]); setMapDesignated(null); setMapSaveMessage('')
  }

  async function saveMemberMapping() {
    try {
      const des = mapRight.find(r => r.id === mapDesignated)
      const designatedMemberId = Number(des?.MEMBER_ID || des?.member_id || des?.id || 0)
      if (!designatedMemberId) { setStatus('Designated member has no MEMBER_ID'); return }
      let ok = 0
      for (const m of mapRight) {
        const payload = { ...m, MEMBER_ID: designatedMemberId, member_id: designatedMemberId, OFFICIAL_MEMBER_ID: designatedMemberId, official_member_id: designatedMemberId }
        const res = await authFetch(`http://localhost:8000/members/${m.id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
        if (res.ok) ok++
      }
      setStatus(`Map Member saved: ${ok}/${mapRight.length} updated with MEMBER_ID=${designatedMemberId} and OFFICIAL_MEMBER_ID=${designatedMemberId}`)
      setMapSaveMessage('Mapping was successful.')
      await fetchMembers('')
    } catch (e) { setStatus('Map Member save failed: ' + e.message) }
  }

  async function saveFamilyMapping() {
    try {
      const des = mapRight.find(r => r.id === mapDesignated)
      const desFamilyId = Number(des?.MEMBER_ID || des?.member_id || des?.id || 0)
      if (!desFamilyId) { setStatus('Designated member has no MEMBER_ID'); return }
      let ok = 0
      for (const m of mapRight) {
        const payload = { ...m, FAMILY_ID: desFamilyId, DEFAULT_FAMILY_ID: desFamilyId }
        const res = await authFetch(`http://localhost:8000/members/${m.id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
        if (res.ok) ok++
      }
      setStatus(`Map Families saved: ${ok}/${mapRight.length} updated with FAMILY_ID=${desFamilyId}`)
      setMapSaveMessage('Mapping was successful.')
      await fetchMembers('')
    } catch (e) { setStatus('Map Families save failed: ' + e.message) }
  }

  async function saveMember() {
    try {
      if (editingMember) {
        const res = await authFetch(`http://localhost:8000/members/${editingMember.id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(memberForm) })
        const data = await res.json(); if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
        setStatus('Member updated')
      } else {
        const createPayload = { ...memberForm }
        const memberIdRaw = createPayload.MEMBER_ID ?? createPayload.member_id
        if (memberIdRaw === undefined || memberIdRaw === null || String(memberIdRaw).trim() === '') {
          createPayload.MEMBER_ID = nextMemberNumericValue(['MEMBER_ID', 'member_id'], 1)
        }
        if (createPayload.sno === undefined || createPayload.sno === null || String(createPayload.sno).trim() === '') {
          createPayload.sno = nextMemberNumericValue(['sno'], 1)
        }
        const res = await authFetch('http://localhost:8000/members', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(createPayload) })
        const data = await res.json(); if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
        setStatus('Member created')
      }
      closeMemberForm()
      await fetchMembers('')
    } catch (e) { setStatus('Save failed: ' + e.message) }
  }

  function renderMemberField(key) {
    const val = memberForm[key] === undefined ? '' : memberForm[key]
    let control = null
    let mappedDisplay = null

    if (key === 'STATUS') {
      control = (
        <select value={val || ''} onChange={e => setMemberForm(prev => ({ ...prev, [key]: e.target.value || null }))}>
          <option value=''>-- status --</option>
          <option value='active'>active</option>
          <option value='inactive by transfer'>inactive by transfer</option>
          <option value='inactive by removal'>inactive by removal</option>
          <option value='inactive by death'>inactive by death</option>
          <option value='inactive unknown reason'>inactive unknown reason</option>
        </select>
      )
    } else if (key === 'church') {
      control = (
        <select value={val || ''} onChange={e => {
          const selected = e.target.value
          if (!canAccessChurch(selected)) { denyRestrictedChurchAccess('member data'); return }
          setMemberForm(prev => ({ ...prev, [key]: selected }))
        }}>
          <option value=''>-- church --</option>
          {churches.map(c => <option key={c.id} value={c.id} disabled={!canAccessChurch(c.id)}>{c.name}</option>)}
        </select>
      )
    } else if (key === 'TRANSFER_TO_CHURCH' || key === 'transfer_to_church') {
      const canCreateChurch = String(user?.role || '').toLowerCase() === 'system_admin' || String(user?.username || '').toLowerCase() === 'saypy_admin'
      const selectedTransferChurch = memberForm?.TRANSFER_TO_CHURCH ?? memberForm?.transfer_to_church ?? ''
      control = (
        <div className='kv' style={{ minWidth: 260 }}>
          <select value={selectedTransferChurch || ''} onChange={e => {
            const selected = e.target.value
            if (!canAccessChurch(selected)) { denyRestrictedChurchAccess('member transfer'); return }
            const mapped = selected === '' ? null : Number(selected)
            setMemberForm(prev => ({ ...prev, TRANSFER_TO_CHURCH: mapped, transfer_to_church: mapped }))
          }}>
            <option value=''>-- transfer to church --</option>
            {churches.map(c => <option key={c.id} value={c.id} disabled={!canAccessChurch(c.id)}>{c.name}</option>)}
          </select>
          <div className='flex gap8' style={{ alignItems: 'center', flexWrap: 'wrap' }}>
            <button type='button' className='btn btn-sm' onClick={() => setShowTransferChurchCreator(v => !v)}>
              {showTransferChurchCreator ? 'Cancel add church' : 'Add missing church'}
            </button>
            {!canCreateChurch && <span className='small'>System admin only</span>}
          </div>
          {showTransferChurchCreator && (
            <div className='flex gap8' style={{ alignItems: 'center', flexWrap: 'wrap' }}>
              <input
                placeholder='New church name'
                value={newTransferChurchName}
                onChange={e => setNewTransferChurchName(e.target.value)}
                disabled={!canCreateChurch}
                style={{ minWidth: 180 }}
              />
              <button type='button' className='btn btn-sm' onClick={createTransferChurchOption} disabled={!canCreateChurch}>Create</button>
            </div>
          )}
        </div>
      )
    } else if (key === 'TRANSFER_DATE' || key === 'transfer_date') {
      const v = val ? new Date(val).toISOString().slice(0, 10) : ''
      control = <input type='date' value={v} onChange={e => setMemberForm(prev => ({ ...prev, [key]: e.target.value || null }))} />
    } else if (key === 'sno') {
      control = (
        <input placeholder={key} value={val ?? ''} type='number' readOnly disabled style={{ fontWeight: 700 }} />
      )
    } else if (key === 'OFFICIAL_MEMBER_ID' || key === 'official_member_id') {
      control = (
        <input placeholder={key} value={val || ''} type='number' onChange={e => setMemberForm(prev => ({ ...prev, [key]: e.target.value === '' ? null : Number(e.target.value) }))} />
      )
      const selectedMemberId = val
      const selectedMemberIdentity = Number(selectedMemberId)
      const selectedMemberProfileIdRaw = memberForm?.MEMBER_ID ?? memberForm?.member_id ?? editingMember?.MEMBER_ID ?? editingMember?.member_id
      const selectedMemberProfileId = Number(selectedMemberProfileIdRaw)
      const clickedMemberRow = (members || []).find(m => Number(m?.id) === Number(editingMember?.id))
        || (members || []).find(m => Number(m?.MEMBER_ID ?? m?.member_id) === selectedMemberProfileId)
      const selectedMemberRealIdRaw = clickedMemberRow?.MEMBER_ID ?? clickedMemberRow?.member_id ?? selectedMemberProfileIdRaw
      const selectedMemberRealId = Number(selectedMemberRealIdRaw)
      const selectedMemberName = String(
        memberForm?.MEMBER_NAME ||
        memberForm?.member_name ||
        clickedMemberRow?.MEMBER_NAME ||
        clickedMemberRow?.member_name ||
        editingMember?.MEMBER_NAME ||
        editingMember?.member_name ||
        ''
      ).trim()
      const seenIdentity = new Set()
      const seenIdentityLabel = new Set()
      const identityOptions = []

      const pushIdentityOption = (identityRaw, nameRaw) => {
        const identity = Number(identityRaw)
        if (!Number.isFinite(identity) || identity <= 0) return
        const nm = String(nameRaw || '').trim() || 'Unknown Member'
        const label = `${nm} (ID:${identity})`
        const dedupeKey = `${identity}|${label}`
        if (seenIdentityLabel.has(dedupeKey)) return
        seenIdentityLabel.add(dedupeKey)
        seenIdentity.add(identity)
        identityOptions.push({ value: identity, label })
      }

      const pushCanonicalForIdentity = (identityRaw, fallbackNameRaw) => {
        const identity = Number(identityRaw)
        if (!Number.isFinite(identity) || identity <= 0) return
        const canonical = (members || []).find(mm => Number(mm?.MEMBER_ID ?? mm?.member_id) === identity)
          || (members || []).find(mm => Number(mm?.OFFICIAL_MEMBER_ID ?? mm?.official_member_id) === identity)
        const canonicalName = String(canonical?.MEMBER_NAME || canonical?.member_name || fallbackNameRaw || '').trim() || 'Unknown Member'
        pushIdentityOption(identity, canonicalName)
      }

      if (Number.isFinite(selectedMemberRealId) && selectedMemberRealId > 0) {
        pushIdentityOption(selectedMemberRealId, selectedMemberName || 'Selected Member')
        const canonicalForClicked = (members || []).find(mm => Number(mm?.MEMBER_ID ?? mm?.member_id) === selectedMemberRealId)
        if (canonicalForClicked && Number(canonicalForClicked?.id) !== Number(clickedMemberRow?.id)) {
          const canonicalClickedName = String(canonicalForClicked?.MEMBER_NAME || canonicalForClicked?.member_name || '').trim()
          pushIdentityOption(selectedMemberRealId, canonicalClickedName || 'Canonical Member')
        }
      }

      const sourceMembers = (filteredMembers && filteredMembers.length ? filteredMembers : (members || []))
        ; (sourceMembers || []).forEach(m => {
          const uniqueRaw = m?.OFFICIAL_MEMBER_ID ?? m?.official_member_id
          const uniqueVal = Number(uniqueRaw)
          if (!Number.isFinite(uniqueVal) || uniqueVal <= 0) return
          pushCanonicalForIdentity(uniqueVal, m?.MEMBER_NAME || m?.member_name)
        })

        ; (sourceMembers || []).forEach(m => {
          const memberRaw = m?.MEMBER_ID ?? m?.member_id
          const memberVal = Number(memberRaw)
          if (!Number.isFinite(memberVal) || memberVal <= 0) return
          pushCanonicalForIdentity(memberVal, m?.MEMBER_NAME || m?.member_name)
        })

      if (Number.isFinite(selectedMemberIdentity) && selectedMemberIdentity > 0 && !seenIdentity.has(selectedMemberIdentity)) {
        const found = (filteredMembers || []).find(m => Number(m?.OFFICIAL_MEMBER_ID ?? m?.official_member_id) === selectedMemberIdentity || Number(m?.MEMBER_ID ?? m?.member_id) === selectedMemberIdentity)
          || (members || []).find(m => Number(m?.OFFICIAL_MEMBER_ID ?? m?.official_member_id) === selectedMemberIdentity || Number(m?.MEMBER_ID ?? m?.member_id) === selectedMemberIdentity)
        const fallbackName = String(found?.MEMBER_NAME || found?.member_name || '').trim() || 'Selected Identity'
        identityOptions.push({ value: selectedMemberIdentity, label: `${fallbackName} (ID:${selectedMemberIdentity})` })
      }

      mappedDisplay = (
        <select value={selectedMemberId || ''} onChange={e => {
          const selected = e.target.value
          const memberId = selected === '' ? null : Number(selected)
          setMemberForm(prev => ({ ...prev, [key]: memberId }))
        }}>
          <option value=''>-- select unique identity --</option>
          {identityOptions.map(opt => (
            <option key={`uid-${opt.value}`} value={opt.value}>{opt.label}</option>
          ))}
        </select>
      )
    } else if (key === 'FAMILY_ID' || key === 'family_id' || key === 'DEFAULT_FAMILY_ID' || key === 'default_family_id' || key === 'GROUP_LEADER_ID' || key === 'group_leader_id' || key === 'DEFAULT_GROUP_LEADER_ID' || key === 'default_group_leader_id') {
      const isGroup = key.toLowerCase().includes('group')
      const isDefault = key.toLowerCase().startsWith('default_')
      const displayControl = (
        <input placeholder={key} value={val || ''} type='number' onChange={e => setMemberForm(prev => ({ ...prev, [key]: e.target.value === '' ? null : Number(e.target.value) }))} />
      )

      const editingMemberOwnId = Number(memberForm?.MEMBER_ID ?? memberForm?.member_id ?? editingMember?.MEMBER_ID ?? editingMember?.member_id)
      const editingMemberFamilyId = Number(memberForm?.FAMILY_ID ?? memberForm?.family_id ?? editingMember?.FAMILY_ID ?? editingMember?.family_id)
      const effectiveDefaultVal = (isDefault && !isGroup && !val && !Number.isFinite(editingMemberFamilyId))
        ? (Number.isFinite(editingMemberOwnId) && editingMemberOwnId > 0 ? editingMemberOwnId : val)
        : val

      const selectedMemberId = (isDefault && !isGroup) ? effectiveDefaultVal : val

      const seenFam = new Set()
      const famOptions = []
      const selfId = Number.isFinite(editingMemberOwnId) && editingMemberOwnId > 0 ? editingMemberOwnId : null
      const selfName = String(memberForm?.MEMBER_NAME || memberForm?.member_name || editingMember?.MEMBER_NAME || editingMember?.member_name || '').trim()
      const addFamilyOption = (identityRaw, fallbackName) => {
        const identity = Number(identityRaw)
        if (!Number.isFinite(identity) || identity <= 0) return
        if (seenFam.has(identity)) return
        seenFam.add(identity)
        const mappedMember = (members || []).find(mm => Number(mm?.MEMBER_ID ?? mm?.member_id) === identity)
        const mappedName = String(mappedMember?.MEMBER_NAME || mappedMember?.member_name || fallbackName || '').trim() || 'Unknown'
        famOptions.push({ value: identity, label: `${mappedName} (ID:${identity})` })
      }

      if (selfId) {
        addFamilyOption(selfId, selfName || 'Current Member')
      }

      if (isGroup) {
        ; (members || []).forEach(m => {
          const refRaw = m?.GROUP_LEADER_ID ?? m?.group_leader_id
          const sourceName = String(m?.MEMBER_NAME || m?.member_name || '').trim()
          addFamilyOption(refRaw, sourceName)
        })
      } else {
        ; (members || []).forEach(m => {
          const uidRaw = m?.OFFICIAL_MEMBER_ID ?? m?.official_member_id
          const sourceName = String(m?.MEMBER_NAME || m?.member_name || '').trim()
          addFamilyOption(uidRaw, sourceName)
        })
          ; (members || []).forEach(m => {
            const famRaw = m?.FAMILY_ID ?? m?.family_id
            const sourceName = String(m?.MEMBER_NAME || m?.member_name || '').trim()
            addFamilyOption(famRaw, sourceName)
          })
      }
      const curNum = Number(selectedMemberId)
      if (Number.isFinite(curNum) && curNum > 0 && !seenFam.has(curNum)) {
        const found = (members || []).find(m => Number(m?.MEMBER_ID ?? m?.member_id) === curNum)
        const fbName = String(found?.MEMBER_NAME || found?.member_name || '').trim() || 'Selected'
        famOptions.push({ value: curNum, label: `${fbName} (ID:${curNum})` })
      }

      mappedDisplay = (
        <select value={selectedMemberId || ''} onChange={e => {
          const selected = e.target.value
          const memberId = selected === '' ? null : Number(selected)
          setMemberForm(prev => ({ ...prev, [key]: memberId }))
        }}>
          <option value=''>-- select {isGroup ? 'group leader' : 'family'} --</option>
          {famOptions.map(opt => (
            <option key={`fam-${opt.value}`} value={opt.value}>{opt.label}</option>
          ))}
        </select>
      )
      control = displayControl
    } else {
      const isNumber = typeof val === 'number' || key.toLowerCase().includes('id') || key.toLowerCase().includes('sno') || key.toLowerCase().includes('pledge')
      control = (
        <input placeholder={key} value={val || ''} type={isNumber ? 'number' : 'text'} onChange={e => setMemberForm(prev => ({ ...prev, [key]: isNumber ? (e.target.value === '' ? null : Number(e.target.value)) : e.target.value }))} />
      )
    }

    return (
      <React.Fragment key={key}>
        <label className='field-chip'>{memberLabelForColumn(key)}</label>
        <div className='kv'>{control}</div>
        <div>{mappedDisplay}</div>
      </React.Fragment>
    )
  }

  function renderMemberFormFields() {
    const sourceKeys = (membersFields && membersFields.length ? membersFields : Object.keys(memberForm || {}))
    const keys = [...sourceKeys]
    if (!keys.includes('OFFICIAL_MEMBER_ID') && !keys.includes('official_member_id')) {
      const insertAt = keys.includes('MEMBER_NAME') ? (keys.indexOf('MEMBER_NAME') + 1) : 1
      keys.splice(Math.max(0, insertAt), 0, 'OFFICIAL_MEMBER_ID')
    }
    const moveKey = (arr, keyName, toIndex) => {
      const idx = arr.indexOf(keyName)
      if (idx < 0) return
      const [item] = arr.splice(idx, 1)
      arr.splice(Math.max(0, toIndex), 0, item)
    }
    if (keys.includes('MEMBER_NAME')) moveKey(keys, 'MEMBER_NAME', 0)
    if (keys.includes('OFFICIAL_MEMBER_ID')) moveKey(keys, 'OFFICIAL_MEMBER_ID', 1)
    else if (keys.includes('official_member_id')) moveKey(keys, 'official_member_id', 1)

    return keys.map(key => {
      if (['id', 'created_at', 'church', 'MEMBER_ID', 'member_id', 'STATUS_UPDATED_AT', 'status_updated_at', 'PHONE2', 'phone2'].includes(key)) return null
      if (['GROUP_NAME', 'group_name', 'GROUP_ALIAS', 'group_alias', 'DEFAULT_GROUP_ALIAS', 'default_group_alias', 'GROUP_LEADER_ID', 'group_leader_id', 'DEFAULT_GROUP_LEADER_ID', 'default_group_leader_id'].includes(key) && !showMemberGroupInfo) return null
      if (['TRANSFER_TO_CHURCH', 'transfer_to_church', 'TRANSFER_DATE', 'transfer_date'].includes(key) && !showMembershipInfo) return null
      return renderMemberField(key)
    })
  }

  return (
    <div>
      <PageHeader
        title='Members'
        subtitle='Member records, filters, mapping, and exports'
        stats={[
          { label: 'Visible', value: visibleCount },
          { label: 'Loaded', value: loadedCount },
          { label: 'Active', value: activeCount },
          { label: 'Inactive', value: inactiveCount },
          { label: 'Churches', value: churchesRepresented },
        ]}
        actions={(
          <>
            <button type='button' className='btn btn-primary' onClick={() => { setEditingMember(null); setMemberForm({}); setShowMemberGroupInfo(false); setShowMembershipInfo(false); setShowMemberForm(true); }}>New Member</button>
            <button type='button' className='btn' onClick={() => setShowMembersColumnPicker(true)}>Column Picker</button>
          </>
        )}
      />
      <Card title="Members Directory" outline variant="primary">
        <div className='toolbar'>
        <input placeholder='Search members' value={membersQ} onChange={e => setMembersQ(e.target.value)} />
        <button type='button' className='btn' onClick={() => fetchMembers(membersQ)}>Search</button>
        <label>Filter</label>
        <select value={membersSearchField} onChange={e => setMembersSearchField(e.target.value)}>
          <option value='all'>All fields</option>
          <option value='name'>Name</option>
          <option value='member_id'>Member ID</option>
          <option value='phone'>Phone</option>
          <option value='church'>Church</option>
          <option value='sno'>SNO</option>
        </select>
        <input placeholder='Filter loaded members...' value={membersSearchText} onChange={e => setMembersSearchText(e.target.value)} />
        <button type='button' className='btn btn-sm' onClick={addMembersFilter}>Add Field Filter</button>
        <button type='button' className='btn btn-sm' onClick={clearMembersFilters}>Clear Field Filters</button>
        <button type='button' className='btn btn-sm' onClick={saveMembersColumnSelection}>Save Selection</button>
        <button type='button' className='btn btn-sm' onClick={exportMembersExcel}>Export Excel</button>
        <button type='button' className='btn btn-sm' onClick={exportMembersPdf}>Export PDF</button>
        <label>Max rows</label>
        <select value={membersMaxRows} onChange={e => setMembersMaxRows(Number(e.target.value))}>
          {[10, 30, 50, 100].map(n => <option key={n} value={n}>{n}</option>)}
        </select>
        <span className='small'>Showing {Math.min(filteredMembers.length, membersMaxRows)} of {filteredMembers.length} (filters: {membersExtraFilters.length})</span>
      </div>

      {membersExtraFilters.length > 0 && (
        <div className='subpanel mb8'>
          <div className='card-title'>Field Filters (AND logic)</div>
          <div className='kv'>
            {membersExtraFilters.map(f => (
              <div key={f.id} className='form-row'>
                <select value={f.field} onChange={e => updateMembersFilter(f.id, { field: e.target.value })}>
                  {membersFilterableFields().map(col => <option key={col} value={col}>{membersFieldLabel(col)}</option>)}
                </select>
                <select value={f.op} onChange={e => updateMembersFilter(f.id, { op: e.target.value })}>
                  <option value='contains'>contains</option>
                  <option value='equals'>equals</option>
                  <option value='starts_with'>starts with</option>
                  <option value='ends_with'>ends with</option>
                </select>
                <input placeholder='value...' value={f.value} onChange={e => updateMembersFilter(f.id, { value: e.target.value })} />
                <button type='button' className='btn btn-sm' onClick={() => removeMembersFilter(f.id)}>Remove</button>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className='subpanel mb8'>
        <span className='small'>Showing: {getMembersDisplayColumns().map(memberLabelForColumn).join(', ')}</span>
      </div>

      <ColumnPicker
        open={showMembersColumnPicker}
        title='Members Column Picker'
        allColumns={getMembersAllColumns()}
        currentColumns={getMembersDisplayColumns()}
        labelForColumn={memberLabelForColumn}
        onApply={(cols) => { setMembersVisibleCols(cols); setShowMembersColumnPicker(false) }}
        onCancel={() => setShowMembersColumnPicker(false)}
      />

      <div style={mainGridWrapStyle(400)}>
        <table style={mainGridTableStyle(true)}>
          <thead>
            <tr>
              {getMembersDisplayColumns().map(h => (
                <th key={h} style={{ cursor: 'pointer' }} onClick={() => {
                  if (membersSortKey === h) {
                    setMembersSortDir(d => d === 'asc' ? 'desc' : 'asc')
                  } else {
                    setMembersSortKey(h)
                    setMembersSortDir('asc')
                  }
                }}>
                  {memberLabelForColumn(h)} {membersSortKey === h ? (membersSortDir === 'asc' ? '▲' : '▼') : ''}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filteredMembers.slice(0, membersMaxRows).map(m => (
              <tr
                key={m.id}
                className='row-clickable'
                onClick={() => { setEditingMember(m); setMemberForm({ ...m }); setShowMemberGroupInfo(false); setShowMembershipInfo(false); setShowMemberForm(true); setMapMode(null); }}
              >
                {getMembersDisplayColumns().map(h => <td key={h}>{memberDisplayCellValue(h, m)}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>

      {showMemberForm && (
        <Modal title={editingMember ? 'Edit Member' : 'New Member'} onClose={closeMemberForm} maxWidth={1180}>
          <div className='flex gap8 mb8' style={{ alignItems: 'center', flexWrap: 'wrap' }}>
            {editingMember && (
              <div className='flex gap8' style={{ alignItems: 'center', flexWrap: 'wrap' }}>
                <button type='button' className='link-button' onClick={() => startMapping('member')}>Map Member</button>
                <button type='button' className='link-button' onClick={() => startMapping('family')}>Map Families</button>
              </div>
            )}
          </div>
          <div className='legacy-member-form-grid'>
            {renderMemberFormFields()}
          </div>
          <div className='flex gap8 mt8 mb8' style={{ alignItems: 'center', flexWrap: 'wrap' }}>
            <label className='field-chip'>
              <input type='checkbox' checked={showMemberGroupInfo} onChange={e => setShowMemberGroupInfo(Boolean(e.target.checked))} />
              Show Group Information
            </label>
            <label className='field-chip'>
              <input type='checkbox' checked={showMembershipInfo} onChange={e => setShowMembershipInfo(Boolean(e.target.checked))} />
              Show Membership Information
            </label>
          </div>
          <div className='flex gap8'>
            <button type='button' className='btn btn-primary' onClick={saveMember}>Save</button>
            <button type='button' className='btn btn-ghost' onClick={closeMemberForm}>Cancel</button>
          </div>
        </Modal>
      )}

      {mapMode === 'member' && mapTargetMember && (
        <MemberMappingPanel
          mode='member'
          targetMember={mapTargetMember}
          members={members}
          filter={mapFilter} setFilter={setMapFilter}
          right={mapRight} setRight={setMapRight}
          designated={mapDesignated} setDesignated={setMapDesignated}
          saveMessage={mapSaveMessage}
          onSave={saveMemberMapping}
          onCancel={cancelMapping}
        />
      )}

      {mapMode === 'family' && mapTargetMember && (
        <MemberMappingPanel
          mode='family'
          targetMember={mapTargetMember}
          members={members}
          filter={mapFilter} setFilter={setMapFilter}
          right={mapRight} setRight={setMapRight}
          designated={mapDesignated} setDesignated={setMapDesignated}
          saveMessage={mapSaveMessage}
          onSave={saveFamilyMapping}
          onCancel={cancelMapping}
        />
      )}
    </div>
  )
}
