import React, { useEffect, useMemo, useState } from 'react'
import { UploadCloud } from 'react-feather'
import Alert from '../../Alert'
import PageHeader from '../PageHeader'
import DataTable from '../DataTable'
import { FloatingInput, FloatingSelect } from '../FloatingField'
import { membersApi } from '../api'
import useMyPermissions from '../../../hooks/useMyPermissions'

const SOURCES = [
  { value: 'cash', label: 'Cash' },
  { value: 'mobile_money', label: 'Mobile Money' },
  { value: 'bank_transfer', label: 'Bank Transfer' },
  { value: 'cheque', label: 'Cheque' },
  { value: 'other', label: 'Other' },
]

const ID_KINDS = [
  { value: '', label: '-- not used --' },
  { value: 'name', label: 'Identification: Name' },
  { value: 'phone', label: 'Identification: Phone' },
  { value: 'email', label: 'Identification: Email' },
]

const MATCH_LABEL = { phone: 'Matched (phone)', email: 'Matched (email)', name: 'Matched (name)', manual: 'Matched (manual)', unmatched: 'Unmatched' }

function todayStr() { return new Date().toISOString().slice(0, 10) }

export default function CollectionImportWizard({ onImported }) {
  const { can, loading: permsLoading } = useMyPermissions()
  const [step, setStep] = useState(1)
  const [churches, setChurches] = useState([])
  const [types, setTypes] = useState([])
  const [churchMembers, setChurchMembers] = useState([])

  // Step 1
  const [file, setFile] = useState(null)
  const [churchId, setChurchId] = useState('')
  const [collectionDate, setCollectionDate] = useState(todayStr())
  const [collectionSource, setCollectionSource] = useState('cash')
  const [groupName, setGroupName] = useState('')
  const [inspecting, setInspecting] = useState(false)

  // Step 2
  const [headers, setHeaders] = useState([])
  const [rawRows, setRawRows] = useState([])
  const [idMapping, setIdMapping] = useState({}) // header -> 'name'|'phone'|'email'
  const [typeMapping, setTypeMapping] = useState({}) // header -> collection_type_id
  const [resolving, setResolving] = useState(false)

  // Step 3
  const [resolvedRows, setResolvedRows] = useState([])
  const [unmatchedOnly, setUnmatchedOnly] = useState(false)
  const [saving, setSaving] = useState(false)

  const [error, setError] = useState('')
  const [doneGroup, setDoneGroup] = useState(null)

  useEffect(() => { membersApi.listChurches().then(setChurches).catch(() => {}) }, [])
  useEffect(() => {
    if (!churchId) { setTypes([]); setChurchMembers([]); return }
    membersApi.listCollectionTypes({ church_id: churchId }).then(setTypes).catch(() => setTypes([]))
    membersApi.listMembers({ church_id: churchId, page_size: 500 }).then(res => setChurchMembers(res.items || [])).catch(() => setChurchMembers([]))
  }, [churchId])

  function setIdKind(header, kind) {
    setIdMapping(prev => {
      const next = { ...prev }
      if (kind === '') { delete next[header]; return next }
      // only one header per identification kind
      Object.keys(next).forEach(h => { if (next[h] === kind) delete next[h] })
      next[header] = kind
      return next
    })
  }

  function setTypeForHeader(header, typeId) {
    setTypeMapping(prev => {
      const next = { ...prev }
      if (!typeId) { delete next[header]; return next }
      next[header] = Number(typeId)
      return next
    })
  }

  async function doInspect() {
    if (!file) { setError('Choose a file first'); return }
    if (!churchId) { setError('Choose a church first'); return }
    setError(''); setInspecting(true)
    try {
      const data = await membersApi.inspectImportFile(file)
      setHeaders(data.headers || [])
      setRawRows(data.rows || [])
      setIdMapping({})
      setTypeMapping({})
      setStep(2)
    } catch (e) {
      setError(e.message || 'Could not read file')
    } finally {
      setInspecting(false)
    }
  }

  async function doResolve() {
    if (Object.keys(idMapping).length === 0) { setError('Map at least one identification column (name, phone, or email)'); return }
    if (Object.keys(typeMapping).length === 0) { setError('Map at least one column to a collection type'); return }
    setError(''); setResolving(true)
    try {
      const res = await membersApi.resolveImportMembers({
        church_id: Number(churchId),
        rows: rawRows,
        identification_mapping: idMapping,
        collection_mapping: typeMapping,
      })
      setResolvedRows(res.rows || [])
      setStep(3)
    } catch (e) {
      setError(e.message || 'Could not resolve rows')
    } finally {
      setResolving(false)
    }
  }

  const selectedTypeIds = useMemo(() => [...new Set(Object.values(typeMapping))], [typeMapping])
  const typeById = new Map(types.map(t => [t.id, t]))

  function amountFor(row, typeId) {
    const item = (row.items || []).find(i => i.collection_type_id === typeId)
    return item ? item.amount : 0
  }

  function updateRowAmount(rowIndex, typeId, valueStr) {
    setResolvedRows(prev => prev.map(r => {
      if (r.row_index !== rowIndex) return r
      const amount = valueStr === '' ? 0 : Number(valueStr)
      const items = (r.items || []).filter(i => i.collection_type_id !== typeId)
      if (amount > 0) items.push({ collection_type_id: typeId, amount })
      return { ...r, items, total_amount: items.reduce((s, i) => s + i.amount, 0) }
    }))
  }

  function updateRowMember(rowIndex, memberIdStr) {
    const member = churchMembers.find(m => String(m.id) === memberIdStr)
    setResolvedRows(prev => prev.map(r => r.row_index === rowIndex
      ? { ...r, member_id: member ? member.id : null, member_name: member ? `${member.first_name} ${member.last_name}` : null, match_confidence: member ? 'manual' : 'unmatched' }
      : r))
  }

  const stats = useMemo(() => {
    const byType = {}
    let grandTotal = 0
    resolvedRows.forEach(r => {
      (r.items || []).forEach(i => {
        byType[i.collection_type_id] = (byType[i.collection_type_id] || 0) + i.amount
        grandTotal += i.amount
      })
    })
    return { byType, grandTotal, rowCount: resolvedRows.length, unmatchedCount: resolvedRows.filter(r => !r.member_id).length }
  }, [resolvedRows])

  const visibleRows = unmatchedOnly ? resolvedRows.filter(r => !r.member_id) : resolvedRows

  const previewColumns = [
    { key: 'row_index', label: '#', render: r => r.row_index + 1 },
    {
      key: 'match', label: 'Member', sortable: false,
      render: r => (
        <div>
          <div className={`small ${r.member_id ? 'text-success' : 'text-danger'}`}>{r.member_name || r.name || '—'}</div>
          <div className='hint'>{MATCH_LABEL[r.match_confidence] || r.match_confidence}</div>
          {!r.member_id && (
            <select className='form-select form-select-sm mt-1' value='' onChange={e => updateRowMember(r.row_index, e.target.value)}>
              <option value=''>-- pick member --</option>
              {churchMembers.map(m => <option key={m.id} value={m.id}>{m.first_name} {m.last_name}</option>)}
            </select>
          )}
        </div>
      ),
    },
    ...selectedTypeIds.map(typeId => ({
      key: `type_${typeId}`,
      label: typeById.get(typeId)?.name || `Type ${typeId}`,
      sortable: false,
      render: r => (
        <input
          type='number' min='0' step='0.01' className='form-control form-control-sm' style={{ minWidth: 100 }}
          value={amountFor(r, typeId) || ''}
          onChange={e => updateRowAmount(r.row_index, typeId, e.target.value)}
        />
      ),
    })),
    { key: 'total_amount', label: 'Total', sortable: false, render: r => Number(r.total_amount || 0).toFixed(2) },
  ]

  async function doSave() {
    const unresolved = resolvedRows.filter(r => !r.member_id)
    if (unresolved.length > 0) {
      setError(`${unresolved.length} row(s) still have no matched member — pick one manually or remove them before saving.`)
      return
    }
    const withAmounts = resolvedRows.filter(r => (r.items || []).length > 0)
    if (withAmounts.length === 0) {
      setError('No rows have any collection amount entered.')
      return
    }
    setError(''); setSaving(true)
    try {
      const group = await membersApi.commitImport({
        church_id: Number(churchId),
        group_name: groupName || null,
        collection_date: collectionDate,
        collection_source: collectionSource,
        rows: withAmounts.map(r => ({
          member_id: r.member_id,
          total_amount: r.total_amount,
          items: r.items.map(i => ({ collection_type_id: i.collection_type_id, amount: i.amount })),
        })),
      })
      setDoneGroup(group)
      setStep(4)
      if (onImported) onImported(group)
    } catch (e) {
      setError(e.message || 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  function startOver() {
    setStep(1); setFile(null); setHeaders([]); setRawRows([]); setIdMapping({}); setTypeMapping({})
    setResolvedRows([]); setDoneGroup(null); setError(''); setGroupName('')
  }

  if (!permsLoading && !can('manage_collections')) {
    return (
      <div>
        <PageHeader title='Collections' subtitle='Import' icon={UploadCloud} />
        <Alert level='error' title='Access restricted'>You don't have permission to import collections.</Alert>
      </div>
    )
  }

  return (
    <div>
      <PageHeader title='Collections' subtitle='Import' />
      <p className='small mb8'>Step {Math.min(step, 4)} of 4</p>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      {step === 1 && (
        <div className='card'><div className='card-body'>
          <h5 className='card-title mb-1'>1. Upload file</h5>
          <div className='form-grid mb-3'>
            <FloatingSelect label='Church *' value={churchId} onChange={e => setChurchId(e.target.value)}>
              <option value=''>-- church --</option>
              {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
            </FloatingSelect>
            <FloatingInput label='Collection date *' type='date' value={collectionDate} onChange={e => setCollectionDate(e.target.value)} />
            <FloatingSelect label='Source' value={collectionSource} onChange={e => setCollectionSource(e.target.value)}>
              {SOURCES.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
            </FloatingSelect>
            <FloatingInput label='Group name (optional)' value={groupName} onChange={e => setGroupName(e.target.value)} />
          </div>
          <input type='file' accept='.xlsx,.xls,.csv' className='form-control' onChange={e => setFile(e.target.files[0])} />
          <div className='mt8'>
            <button type='button' className='btn btn-primary' disabled={inspecting} onClick={doInspect}>{inspecting ? 'Reading…' : 'Upload & Inspect'}</button>
          </div>
        </div></div>
      )}

      {step === 2 && (
        <div className='card'><div className='card-body'>
          <h5 className='card-title mb-1'>2. Map columns</h5>
          <p className='small mb8'>For each detected column, say whether it identifies the contributor, and/or which collection type it holds an amount for.</p>
          <div className='table-wrap'>
            <table>
              <thead><tr><th>Column</th><th>Sample</th><th>Identification</th><th>Collection type</th></tr></thead>
              <tbody>
                {headers.map(h => (
                  <tr key={h}>
                    <td>{h}</td>
                    <td className='small'>{String(rawRows[0]?.[h] ?? '—')}</td>
                    <td>
                      <select className='form-select form-select-sm' value={idMapping[h] || ''} onChange={e => setIdKind(h, e.target.value)}>
                        {ID_KINDS.map(k => <option key={k.value} value={k.value}>{k.label}</option>)}
                      </select>
                    </td>
                    <td>
                      <select className='form-select form-select-sm' value={typeMapping[h] || ''} onChange={e => setTypeForHeader(h, e.target.value)}>
                        <option value=''>-- not used --</option>
                        {types.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
                      </select>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className='flex gap8 mt8'>
            <button type='button' className='btn' onClick={() => setStep(1)}>Back</button>
            <button type='button' className='btn btn-primary' disabled={resolving} onClick={doResolve}>{resolving ? 'Matching…' : 'Next — Preview'}</button>
          </div>
        </div></div>
      )}

      {step === 3 && (
        <div>
          <div className='row row-cols-1 row-cols-sm-3 g-3 mb-3'>
            <div className='col'><div className='card'><div className='card-body'>
              <h5 className='card-title'>Rows</h5><h1 className='mt-1 mb-0'>{stats.rowCount}</h1>
            </div></div></div>
            <div className='col'><div className='card'><div className='card-body'>
              <h5 className='card-title'>Unmatched</h5><h1 className='mt-1 mb-0'>{stats.unmatchedCount}</h1>
            </div></div></div>
            <div className='col'><div className='card'><div className='card-body'>
              <h5 className='card-title'>Grand total</h5><h1 className='mt-1 mb-0'>{stats.grandTotal.toFixed(2)}</h1>
            </div></div></div>
          </div>

          {selectedTypeIds.length > 0 && (
            <div className='flex gap8 mb-3' style={{ flexWrap: 'wrap' }}>
              {selectedTypeIds.map(typeId => (
                <div key={typeId} className='field-chip'>
                  <strong>{typeById.get(typeId)?.name || typeId}:</strong>&nbsp;{(stats.byType[typeId] || 0).toFixed(2)}
                </div>
              ))}
            </div>
          )}

          <div className='toolbar mb-3'>
            <label className='form-check'>
              <input type='checkbox' className='form-check-input' checked={unmatchedOnly} onChange={e => setUnmatchedOnly(e.target.checked)} />
              <span className='form-check-label'>&nbsp;Show unmatched rows only</span>
            </label>
          </div>

          <DataTable
            columns={previewColumns}
            rows={visibleRows}
            getRowId={r => r.row_index}
            emptyMessage='No rows.'
          />

          <div className='flex gap8 mt8'>
            <button type='button' className='btn' onClick={() => setStep(2)}>Back</button>
            <button type='button' className='btn btn-primary' disabled={saving} onClick={doSave}>{saving ? 'Saving…' : 'Save as pending'}</button>
          </div>
        </div>
      )}

      {step === 4 && doneGroup && (
        <div className='card'><div className='card-body'>
          <h5 className='card-title mb-1'>Saved</h5>
          <p className='small'>
            Group <strong>{doneGroup.code}</strong> created with {doneGroup.collections.length} collection(s), all pending review.
          </p>
          <button type='button' className='btn btn-primary' onClick={startOver}>Import another file</button>
        </div></div>
      )}
    </div>
  )
}
