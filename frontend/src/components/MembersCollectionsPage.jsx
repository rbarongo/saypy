import React from 'react'
import Modal from './Modal'
import ColumnPicker from './ColumnPicker'
import Badge from './Badge'
import Card from './Card'
import PageHeader from './members-core/PageHeader'

export default function MembersCollectionsPage({
  currentUserChurchId,
  fetchMembersCollections, hasActiveMcFilters, getDefaultMcFetchLimit,
  mcApplied, setMcApplied,
  mcPage, setMcPage, mcPageSize, setMcPageSize,
  mcSearchField, setMcSearchField, mcFilterText, setMcFilterText,
  collectionCodes, mcFilterCode, setMcFilterCode,
  mcFilterMemberId, setMcFilterMemberId, mcFilterMemberName, setMcFilterMemberName, mcFilterPhone, setMcFilterPhone,
  mcFilterVerified, setMcFilterVerified, mcFilterS1, setMcFilterS1,
  verifyNames,
  mcFrom, setMcFrom, mcTo, setMcTo, mcFilterAmountMin, setMcFilterAmountMin, mcFilterAmountMax, setMcFilterAmountMax,
  exportMembersCollectionsExcel, exportMembersCollectionsPdf,
  getFilteredMembersCollectionsRows,
  showVerifyMapping, setShowVerifyMapping,
  verifyMappingRows, setVerifyMappingRows, saveAllVerifyMappings, verifySavingAll, verifySavingRowId, saveVerifyMappingForRow,
  hasRoleRight,
  pendingDeleteRequestsCount, deleteRequestStatusFilter, setDeleteRequestStatusFilter, fetchDeleteRequests, fetchPendingDeleteRequestsCount, deleteRequestsLoading, deleteRequests, decideDeleteRequest,
  getMcDisplayColumns, getMcAllColumns, labelForColumn, applyMcColumns,
  showMcColumnPicker, setShowMcColumnPicker,
  mcSortKey, setMcSortKey, mcSortDir, setMcSortDir,
  displayCellValue,
  editingCollection, setEditingCollection,
  unlockCollectionRow, approveAndDeleteCollectionRow, requestCollectionDelete,
  membersCollectionsFields,
  canAccessChurch, denyRestrictedChurchAccess, churches,
  findMemberByCollectionFieldValue,
  normalizeCollectionFieldKey, isCollectionReferenceIdField, collectionFieldIdOptions, collectionFieldTranslation,
  membersLocal,
  authFetch, setStatus,
  mainGridWrapStyle, mainGridTableStyle,
}) {
  const rows = getFilteredMembersCollectionsRows()
  const verifiedCount = rows.filter(r => Number(r?.member_id || 0) > 0).length
  const unverifiedCount = Math.max(0, rows.length - verifiedCount)
  const totalAmount = rows.reduce((sum, row) => {
    const rowTotal = Object.keys(row || {}).reduce((inner, key) => {
      if (/^(s5|s6|s7|s8|s9|s13|c\d+|l\d+)$/i.test(String(key))) {
        const n = Number(row[key])
        return Number.isFinite(n) ? inner + n : inner
      }
      return inner
    }, 0)
    return sum + rowTotal
  }, 0)

  function submitFilters() {
    const next = {
      searchField: mcSearchField, text: mcFilterText, code: mcFilterCode,
      memberId: mcFilterMemberId, memberName: mcFilterMemberName, phone: mcFilterPhone,
      verified: mcFilterVerified, s1: mcFilterS1, amountMin: mcFilterAmountMin, amountMax: mcFilterAmountMax,
      from: mcFrom, to: mcTo,
    }
    setMcApplied(next)
    setMcPage(1)
    fetchMembersCollections({
      search: next.text, searchField: next.searchField, collectionCode: next.code,
      memberId: next.memberId, memberName: next.memberName, phone: next.phone,
      verified: next.verified, s1: next.s1, from: next.from, to: next.to,
      amountMin: next.amountMin, amountMax: next.amountMax,
    })
  }

  function clearFilters() {
    setMcFilterText(''); setMcFilterCode(''); setMcFilterMemberId(''); setMcFilterMemberName('')
    setMcFilterPhone(''); setMcFilterVerified(''); setMcFilterS1(''); setMcFilterAmountMin(''); setMcFilterAmountMax('')
    setMcFrom(''); setMcTo('')
    setMcApplied({ searchField: 'all', text: '', code: '', memberId: '', memberName: '', phone: '', verified: '', s1: '', amountMin: '', amountMax: '', from: '', to: '' })
    setMcPage(1)
    fetchMembersCollections({ limit: getDefaultMcFetchLimit(1, mcPageSize) })
  }

  async function saveCollectionEdit() {
    try {
      const id = editingCollection.id
      const payload = { ...editingCollection }
      delete payload.id
      delete payload.verified
      Object.keys(payload).forEach(key => {
        if (String(key).startsWith('__')) delete payload[key]
      })
      const res = await authFetch(`http://localhost:8000/members_collection/${id}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
      const data = await res.json(); if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus('Saved')
      setEditingCollection(null)
      await fetchMembersCollections()
    } catch (e) { setStatus('Save failed: ' + e.message) }
  }

  function updateCollectionField(field, nextValue) {
    setEditingCollection(prev => {
      const next = { ...prev, [field]: nextValue }
      if (field === 'church') {
        try {
          const s2 = next.s2 ? new Date(next.s2) : null
          const s3 = next.s3 != null && String(next.s3).trim() !== '' ? Number(next.s3) : null
          const churchId = nextValue || next.church || null
          if (s2 && !Number.isNaN(s2.getTime()) && s3 != null && !Number.isNaN(s3) && churchId) {
            const ymd = s2.toISOString().slice(0, 10).replace(/-/g, '')
            const cidn = String(Number(churchId)).padStart(3, '0')
            const s3n = String(Number(s3)).padStart(3, '0')
            next.s1 = `${ymd}${cidn}${s3n}`
          }
        } catch (e) { }
      }
      if (field === 'member_id') {
        const matched = findMemberByCollectionFieldValue(field, nextValue)
        if (matched) {
          const memberName = matched.MEMBER_NAME || matched.member_name
          if (memberName) next.s4 = memberName
          next.__verified = true
        }
      }
      return next
    })
  }

  return (
    <div>
      <PageHeader
        title='Members Collections'
        subtitle={`Latest records by default for church ${currentUserChurchId || '-'}`}
        stats={[
          { label: 'Rows', value: rows.length },
          { label: 'Verified', value: verifiedCount },
          { label: 'Pending', value: unverifiedCount },
          { label: 'Total', value: totalAmount.toLocaleString(undefined, { maximumFractionDigits: 2 }) },
        ]}
      />

      <Card title="Collections Ledger" outline variant="primary">
        <div className='toolbar'>
          <button type='button' className='btn btn-ghost btn-sm' onClick={() => fetchMembersCollections(hasActiveMcFilters() ? {
          search: mcApplied.text, searchField: mcApplied.searchField, collectionCode: mcApplied.code,
          memberId: mcApplied.memberId, memberName: mcApplied.memberName, phone: mcApplied.phone,
          verified: mcApplied.verified, s1: mcApplied.s1, from: mcApplied.from, to: mcApplied.to,
          amountMin: mcApplied.amountMin, amountMax: mcApplied.amountMax,
        } : { limit: getDefaultMcFetchLimit(mcPage, mcPageSize) })}>Refresh</button>
        <label>Search in</label>
        <select value={mcSearchField} onChange={e => setMcSearchField(e.target.value)}>
          <option value='all'>All fields</option>
          <option value='collection_code'>Collection Code</option>
          <option value='s4'>Member Name</option>
          <option value='member_id'>Member ID</option>
          <option value='s1'>S1</option>
          <option value='church'>Church</option>
        </select>
        <input placeholder='Search text' value={mcFilterText} onChange={e => setMcFilterText(e.target.value)} />
        <select value={mcFilterCode} onChange={e => setMcFilterCode(e.target.value)}>
          <option value=''>-- all codes --</option>
          {(collectionCodes || []).map(c => <option key={c.column_name} value={c.column_name}>{c.code || c.column_name}</option>)}
        </select>
        <input placeholder='Member ID' value={mcFilterMemberId} onChange={e => setMcFilterMemberId(e.target.value)} />
        <input placeholder='Member name' value={mcFilterMemberName} onChange={e => setMcFilterMemberName(e.target.value)} />
        <input placeholder='Phone' value={mcFilterPhone} onChange={e => setMcFilterPhone(e.target.value)} />
        <select value={mcFilterVerified} onChange={e => setMcFilterVerified(e.target.value)}>
          <option value=''>All</option>
          <option value='true'>Verified</option>
          <option value='false'>Unverified</option>
        </select>
        <input placeholder='S1 contains' value={mcFilterS1} onChange={e => setMcFilterS1(e.target.value)} />
        <button type='button' className='btn btn-sm' onClick={() => verifyNames()}>Verify Names</button>
        <label>From</label>
        <input type='date' value={mcFrom} onChange={e => setMcFrom(e.target.value)} />
        <label>To</label>
        <input type='date' value={mcTo} onChange={e => setMcTo(e.target.value)} />
        <label>Amount min</label>
        <input type='number' step='0.01' value={mcFilterAmountMin} onChange={e => setMcFilterAmountMin(e.target.value)} style={{ width: 110 }} />
        <label>Amount max</label>
        <input type='number' step='0.01' value={mcFilterAmountMax} onChange={e => setMcFilterAmountMax(e.target.value)} style={{ width: 110 }} />
        <button type='button' className='btn btn-primary' onClick={submitFilters}>Submit</button>
        <button type='button' className='btn btn-ghost' onClick={clearFilters}>Clear</button>
        <label>Max rows</label>
        <select value={mcPageSize} onChange={async e => {
          const nextSize = Number(e.target.value)
          setMcPageSize(nextSize)
          setMcPage(1)
          if (!hasActiveMcFilters()) {
            await fetchMembersCollections({ limit: getDefaultMcFetchLimit(1, nextSize) })
          }
        }}>
          {[10, 30, 50, 100].map(n => <option key={n} value={n}>{n}</option>)}
        </select>
        <button type='button' className='btn btn-sm' onClick={exportMembersCollectionsExcel}>Export Excel</button>
        <button type='button' className='btn btn-sm' onClick={exportMembersCollectionsPdf}>Export PDF</button>
      </div>

      <div className='subpanel mb8 flex gap8' style={{ alignItems: 'center', flexWrap: 'wrap' }}>
        <strong>Verification Summary</strong>
        <span className='small'>Total: {rows.length}</span>
        <Badge level='success'>Verified: {verifiedCount}</Badge>
        <Badge level='warning'>Unverified: {unverifiedCount}</Badge>
      </div>

      {showVerifyMapping && (
        <Modal title='Verify Names Mapping Workspace' onClose={() => setShowVerifyMapping(false)} maxWidth={1300}>
          <div className='flex gap8 mb8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-primary' onClick={saveAllVerifyMappings} disabled={verifySavingAll || verifySavingRowId !== null}>
              {verifySavingAll ? 'Saving All...' : 'Save All Mappings'}
            </button>
          </div>
          <div className='hint mb8'>
            Left side shows collection entries. Right side shows best member matches using fuzzy name plus phone and historical mapping hints.
          </div>
          <div className='table-wrap' style={mainGridWrapStyle(500)}>
            <table style={mainGridTableStyle(true)}>
              <thead>
                <tr>
                  <th>Collection Row</th>
                  <th>Collection Name</th>
                  <th>Collection Phone</th>
                  <th>Suggested Members</th>
                  <th>Selected Mapping</th>
                  <th>Status</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {(verifyMappingRows || []).length === 0 ? (
                  <tr><td colSpan={7}>No rows available for mapping.</td></tr>
                ) : (verifyMappingRows || []).map(v => {
                  const selected = (v.suggestions || []).find(s => Number(s._memberPrimaryId || s.id || 0) === Number(v.selectedMemberId || 0))
                  return (
                    <tr key={v.rowId}>
                      <td>{v.rowId}</td>
                      <td>{String(v?.row?.s4 || '')}</td>
                      <td>{String(v?.row?.phone || '')}</td>
                      <td>
                        <select
                          value={String(v.selectedMemberId || '')}
                          onChange={e => {
                            const selectedId = Number(e.target.value || 0) || null
                            const selectedMatch = (v.suggestions || []).find(s => Number(s._memberPrimaryId || s.id || 0) === Number(selectedId || 0))
                            setVerifyMappingRows(prev => (prev || []).map(r => r.rowId === v.rowId
                              ? { ...r, selectedMemberId: selectedId, selectedLabel: selectedMatch ? String(selectedMatch?.MEMBER_NAME || '') : '' }
                              : r
                            ))
                          }}
                          style={{ minWidth: 330 }}
                        >
                          <option value=''>-- select member --</option>
                          {(v.suggestions || []).map(s => {
                            const pid = Number(s._memberPrimaryId || s.id || 0)
                            if (!pid) return null
                            return (
                              <option key={`${v.rowId}-${pid}`} value={pid}>
                                {String(s?.MEMBER_NAME || '')} {s?.PHONE ? `| ${s.PHONE}` : ''} {s?._score != null ? `| score:${s._score}` : ''}
                              </option>
                            )
                          })}
                        </select>
                      </td>
                      <td>{selected ? `${selected.MEMBER_NAME || ''}${selected.PHONE ? ` | ${selected.PHONE}` : ''}` : '-'}</td>
                      <td>{v.verified ? 'Verified' : 'Unverified'}</td>
                      <td>
                        <button type='button' className='btn btn-sm' onClick={() => saveVerifyMappingForRow(v)} disabled={!v.selectedMemberId || verifySavingRowId === v.rowId}>
                          {verifySavingRowId === v.rowId ? 'Saving...' : 'Save Mapping'}
                        </button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </Modal>
      )}

      {(hasRoleRight('can_delete_members_collections', false) || hasRoleRight('can_unlock_members_collections', false)) && (
        <Card title={
          <div className='flex gap8' style={{ alignItems: 'center' }}>
            <span>Delete Requests</span>
            <Badge level='warning'>Pending: {pendingDeleteRequestsCount}</Badge>
          </div>
        } outline variant="danger" className="mb-3">
          <div className='toolbar'>
            <label>Status</label>
            <select value={deleteRequestStatusFilter} onChange={e => setDeleteRequestStatusFilter(e.target.value)}>
              <option value='pending'>Pending</option>
              <option value='approved'>Approved</option>
              <option value='rejected'>Rejected</option>
              <option value=''>All</option>
            </select>
            <button type='button' className='btn btn-sm' disabled={deleteRequestsLoading} onClick={async () => {
              await fetchDeleteRequests(deleteRequestStatusFilter)
              if (String(deleteRequestStatusFilter || '').toLowerCase() !== 'pending') {
                await fetchPendingDeleteRequestsCount()
              }
            }}>
              {deleteRequestsLoading ? 'Loading...' : 'Refresh Requests'}
            </button>
          </div>
          <div className='table-wrap' style={mainGridWrapStyle(220)}>
            <table style={mainGridTableStyle(true)}>
              <thead>
                <tr>
                  <th>ID</th><th>Row ID</th><th>Status</th><th>Reason</th><th>Requested By</th><th>Requested At</th><th>Decision Note</th><th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {deleteRequests.length === 0 ? (
                  <tr><td colSpan={8}>No delete requests found for selected filter.</td></tr>
                ) : deleteRequests.map(req => {
                  const isPending = String(req?.status || '').toLowerCase() === 'pending'
                  return (
                    <tr key={req.id}>
                      <td>{req.id}</td>
                      <td>{req.row_id}</td>
                      <td>{req.status}</td>
                      <td>{req.reason || ''}</td>
                      <td>{req.requested_by_username || req.requested_by_role || '-'}</td>
                      <td>{req.created_at ? new Date(req.created_at).toLocaleString() : ''}</td>
                      <td>{req.decision_note || ''}</td>
                      <td className='cell-actions'>
                        {isPending && hasRoleRight('can_delete_members_collections', false) ? (
                          <>
                            <button type='button' className='btn btn-sm' onClick={() => decideDeleteRequest(req.id, 'approve')}>Approve</button>
                            <button type='button' className='btn btn-sm' style={{ marginLeft: 6 }} onClick={() => decideDeleteRequest(req.id, 'reject')}>Reject</button>
                          </>
                        ) : (
                          <span className='text-muted'>No action</span>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      <div className='subpanel mb8'>
        <button type='button' className='link-button' onClick={() => setShowMcColumnPicker(true)}>
          Column Picker ({getMcDisplayColumns().length} visible)
        </button>
        <span className='small' style={{ marginLeft: 12 }}>Showing: {getMcDisplayColumns().map(labelForColumn).join(', ')}</span>
      </div>

      <ColumnPicker
        open={showMcColumnPicker}
        title='Members Collections Column Picker'
        allColumns={getMcAllColumns()}
        currentColumns={getMcDisplayColumns()}
        labelForColumn={labelForColumn}
        onApply={applyMcColumns}
        onCancel={() => setShowMcColumnPicker(false)}
      />

      <div className='table-wrap' style={mainGridWrapStyle(500)}>
        <table style={mainGridTableStyle(true)}>
          <thead>
            <tr>
              {getMcDisplayColumns().map(c => (
                <th key={c} style={{ cursor: 'pointer' }} onClick={() => {
                  if (mcSortKey === c) setMcSortDir(d => d === 'asc' ? 'desc' : 'asc'); else { setMcSortKey(c); setMcSortDir('asc') }
                }}>{labelForColumn(c)} {mcSortKey === c ? (mcSortDir === 'asc' ? '▲' : '▼') : ''}</th>
              ))}
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {(() => {
              try {
                const start = (mcPage - 1) * mcPageSize
                const end = start + mcPageSize
                const pageRows = rows.slice(start, end)
                return pageRows.map((r, idx) => (
                  <tr key={r && (r.id || idx)}>
                    {getMcDisplayColumns().map(k => {
                      if (k === 'verified') return <td key={k}>{r && r.__verified ? 'Yes' : 'No'}</td>
                      return <td key={k}>{displayCellValue(k, r)}</td>
                    })}
                    <td className='cell-actions'>
                      <button type='button' className='btn btn-sm' onClick={() => { setEditingCollection({ ...r }); }}>Edit</button>
                      {Number(r?.is_locked || 0) === 1 && hasRoleRight('can_unlock_members_collections', false) && (
                        <button type='button' className='btn btn-sm' style={{ marginLeft: 6 }} onClick={() => unlockCollectionRow(r.id)}>Unlock</button>
                      )}
                      {hasRoleRight('can_delete_members_collections', false) ? (
                        <button type='button' className='btn btn-sm btn-danger' style={{ marginLeft: 6 }} onClick={() => approveAndDeleteCollectionRow(r.id)}>Delete</button>
                      ) : (
                        <button type='button' className='btn btn-sm' style={{ marginLeft: 6 }} onClick={() => requestCollectionDelete(r.id)}>Request Delete</button>
                      )}
                    </td>
                  </tr>
                ))
              } catch (err) {
                console.error('Render error in members_collections table', err)
                return <tr><td colSpan={getMcDisplayColumns().length + 1}>Error rendering collections: {String(err.message || err)}</td></tr>
              }
            })()}
          </tbody>
        </table>
      </div>

      <div className='toolbar mt8'>
        <button type='button' className='btn btn-sm' onClick={() => setMcPage(p => Math.max(1, p - 1))} disabled={mcPage <= 1}>Prev</button>
        <span className='small'>Page {mcPage} / {Math.max(1, Math.ceil(rows.length / mcPageSize))} ({rows.length} records)</span>
        <button type='button' className='btn btn-sm btn-ghost' onClick={async () => {
          if (hasActiveMcFilters()) {
            setMcPage(p => {
              const pages = Math.max(1, Math.ceil(getFilteredMembersCollectionsRows().length / mcPageSize))
              return Math.min(pages, p + 1)
            })
            return
          }
          const nextPage = mcPage + 1
          const count = await fetchMembersCollections({ limit: getDefaultMcFetchLimit(nextPage, mcPageSize) })
          if (count > (nextPage - 1) * mcPageSize) {
            setMcPage(nextPage)
          }
        }} disabled={hasActiveMcFilters() ? mcPage >= Math.max(1, Math.ceil(rows.length / mcPageSize)) : rows.length < mcPage * mcPageSize}>Next</button>
      </div>
    </Card>

      {editingCollection && (
        <Modal title={`Edit Collection #${editingCollection.id}`} onClose={() => setEditingCollection(null)} maxWidth={1180}>
          <div className={`small mb8`} style={{ fontWeight: 700, color: Number(editingCollection?.is_locked || 0) === 1 ? 'var(--warning)' : 'var(--success)' }}>
            Status: {Number(editingCollection?.is_locked || 0) === 1 ? 'Locked (requires unlock before save)' : 'Unlocked'}
          </div>
          {(() => {
            const editFields = (membersCollectionsFields.length
              ? membersCollectionsFields
              : ['id', 'collection_code', 'member_id', 'church', 's1', 's2', 's3', 's4', 's5']
            ).filter(k => k !== 'added_at' && k !== 'verified' && !String(k).startsWith('__'))

            return (
              <div className='table-wrap' style={mainGridWrapStyle(420)}>
                <table style={{ ...mainGridTableStyle(true), minWidth: 920 }}>
                  <thead>
                    <tr>
                      <th style={{ width: 70 }}>NO.</th>
                      <th style={{ width: '28%' }}>ITEM</th>
                      <th style={{ width: '40%' }}>VALUE (EDIT)</th>
                      <th style={{ width: '32%' }}>TRANSLATED VALUE</th>
                    </tr>
                  </thead>
                  <tbody>
                    {editFields.map((k, idx) => {
                      const keyName = normalizeCollectionFieldKey(k)
                      const currentValue = editingCollection[k] === null || editingCollection[k] === undefined ? '' : String(editingCollection[k])
                      const isReadOnly = keyName === 'id' || keyName === 's1'
                      const isReferenceId = isCollectionReferenceIdField(k)
                      const idOptions = isReferenceId ? collectionFieldIdOptions(k) : []
                      const translation = collectionFieldTranslation(k, editingCollection[k])

                      let editor = null
                      if (isReadOnly) {
                        editor = <div style={{ fontWeight: 700 }}>{currentValue || '-'}</div>
                      } else if (keyName === 's2') {
                        const dateValue = currentValue ? new Date(currentValue).toISOString().slice(0, 10) : ''
                        editor = <input type='date' value={dateValue} onChange={e => updateCollectionField(k, e.target.value)} />
                      } else if (keyName === 'church') {
                        editor = (
                          <select value={currentValue || ''} onChange={e => {
                            const newChurch = e.target.value || null
                            if (!canAccessChurch(newChurch)) { denyRestrictedChurchAccess('members_collection data'); return }
                            updateCollectionField(k, newChurch)
                          }}>
                            <option value=''>-- church --</option>
                            {churches.map(c => <option key={c.id} value={c.id} disabled={!canAccessChurch(c.id)}>{c.name}</option>)}
                          </select>
                        )
                      } else if (isReferenceId) {
                        editor = (
                          <div className='kv'>
                            <div style={{ fontWeight: 700 }}>{currentValue || '-'}</div>
                            <select value={currentValue} onChange={e => updateCollectionField(k, e.target.value)}>
                              <option value=''>-- select {labelForColumn(k)} --</option>
                              {idOptions.map(opt => (
                                <option key={opt.value} value={opt.value}>{opt.label}</option>
                              ))}
                            </select>
                          </div>
                        )
                      } else if (keyName === 's4') {
                        editor = (
                          <input
                            list='edit-collection-member-name-list'
                            value={currentValue}
                            onChange={e => updateCollectionField(k, e.target.value)}
                            placeholder='Type or select member name'
                          />
                        )
                      } else {
                        editor = <input value={currentValue} onChange={e => updateCollectionField(k, e.target.value)} placeholder={labelForColumn(k)} />
                      }

                      return (
                        <tr key={k}>
                          <td style={{ fontWeight: 700 }}>{idx + 1}</td>
                          <td style={{ fontWeight: 700 }}>{labelForColumn(k)}</td>
                          <td>{editor}</td>
                          <td className='text-muted' style={{ fontWeight: 600 }}>{translation || '-'}</td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
                <datalist id='edit-collection-member-name-list'>
                  {(membersLocal || []).map(m => {
                    const nm = String(m?.MEMBER_NAME || '').trim()
                    if (!nm) return null
                    return <option key={`${m.id || nm}-${nm}`} value={nm} />
                  })}
                </datalist>
              </div>
            )
          })()}
          <div className='flex gap8 mt8'>
            <button type='button' className='btn btn-primary' onClick={saveCollectionEdit}>Save</button>
            <button type='button' className='btn btn-ghost' onClick={() => setEditingCollection(null)}>Cancel</button>
          </div>
        </Modal>
      )}
    </div>
  )
}
