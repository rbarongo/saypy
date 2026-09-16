import React, { useEffect, useState } from 'react'
import { Clock, CreditCard, Edit3, Eye } from 'react-feather'
import Alert from '../../Alert'
import Badge from '../../Badge'
import Modal from '../../Modal'
import PageHeader from '../PageHeader'
import DataTable from '../DataTable'
import { FloatingInput, FloatingSelect } from '../FloatingField'
import { membersApi } from '../api'
import CollectionReceiptView from './CollectionReceiptView'
import CollectionCorrectionModal from './CollectionCorrectionModal'
import CollectionAuditTrailView from './CollectionAuditTrailView'
import useMyPermissions from '../../../hooks/useMyPermissions'

const PAGE_SIZE = 10
const EMPTY_FILTERS = { church_id: '', status: '', collection_type_id: '' }
const SOURCES = [
  { value: 'cash', label: 'Cash' },
  { value: 'mobile_money', label: 'Mobile Money' },
  { value: 'bank_transfer', label: 'Bank Transfer' },
  { value: 'cheque', label: 'Cheque' },
  { value: 'other', label: 'Other' },
]

function statusLevel(status) {
  if (status === 'approved') return 'success'
  if (status === 'rejected') return 'danger'
  return 'warning'
}

const EMPTY_MANUAL_FORM = { member_id: '', church_id: '', collection_date: '', collection_source: 'cash', notes: '', is_anonymous: false, payment_reference: '' }
const REFERENCE_SOURCES = new Set(['mobile_money', 'bank_transfer'])

function ManualCollectionModal({ churches, onClose, onSaved }) {
  const [form, setForm] = useState(EMPTY_MANUAL_FORM)
  const [members, setMembers] = useState([])
  const [types, setTypes] = useState([])
  const [items, setItems] = useState([{ collection_type_id: '', amount: '' }])
  const [evidenceFile, setEvidenceFile] = useState(null)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    const today = new Date().toISOString().slice(0, 10)
    setForm(prev => ({ ...prev, collection_date: today }))
  }, [])

  useEffect(() => {
    if (!form.church_id) { setMembers([]); setTypes([]); return }
    membersApi.listMembers({ church_id: form.church_id, page_size: 200 }).then(res => setMembers(res.items || [])).catch(() => setMembers([]))
    membersApi.listCollectionTypes({ church_id: form.church_id }).then(setTypes).catch(() => setTypes([]))
  }, [form.church_id])

  function updateItem(idx, patch) { setItems(prev => prev.map((it, i) => i === idx ? { ...it, ...patch } : it)) }
  function addItem() { setItems(prev => [...prev, { collection_type_id: '', amount: '' }]) }
  function removeItem(idx) { setItems(prev => prev.filter((_, i) => i !== idx)) }

  const total = items.reduce((sum, it) => sum + (Number(it.amount) || 0), 0)

  async function save() {
    setError('')
    const validItems = items.filter(it => it.collection_type_id && Number(it.amount) > 0)
    if (!form.is_anonymous && !form.member_id) { setError('Choose a member, or mark the contribution anonymous'); return }
    if (!form.church_id) { setError('Choose a church'); return }
    if (!form.collection_date) { setError('Choose a collection date'); return }
    if (validItems.length === 0) { setError('Add at least one collection item with an amount'); return }
    setSaving(true)
    try {
      const created = await membersApi.createCollection({
        member_id: form.is_anonymous ? null : Number(form.member_id),
        is_anonymous: form.is_anonymous,
        church_id: Number(form.church_id),
        collection_date: form.collection_date,
        collection_source: form.collection_source,
        payment_reference: form.payment_reference || null,
        notes: form.notes || null,
        items: validItems.map(it => ({ collection_type_id: Number(it.collection_type_id), amount: Number(it.amount) })),
      })
      if (evidenceFile) {
        try { await membersApi.uploadCollectionEvidence(created.id, evidenceFile) }
        catch (e) { /* the collection itself saved fine — evidence upload failing shouldn't block that */ }
      }
      onSaved()
    } catch (e) {
      setError(e.message || 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal title='New Collection' onClose={onClose} maxWidth={640}>
      {error && <div className='mb8'><Alert level='error' title='Could not save'>{error}</Alert></div>}
      <div className='form-check mb-3'>
        <input
          type='checkbox' className='form-check-input' id='cc-anon'
          checked={form.is_anonymous}
          onChange={e => setForm(prev => ({ ...prev, is_anonymous: e.target.checked, member_id: '' }))}
        />
        <label className='form-check-label' htmlFor='cc-anon'>&nbsp;Anonymous contribution (no member attached)</label>
      </div>

      <div className='form-grid mb-3'>
        <FloatingSelect label='Church *' value={form.church_id} onChange={e => setForm(prev => ({ ...prev, church_id: e.target.value, member_id: '' }))}>
          <option value=''>-- church --</option>
          {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </FloatingSelect>
        {!form.is_anonymous && (
          <FloatingSelect label='Member *' value={form.member_id} onChange={e => setForm(prev => ({ ...prev, member_id: e.target.value }))}>
            <option value=''>-- member --</option>
            {members.map(m => <option key={m.id} value={m.id}>{m.first_name} {m.last_name}</option>)}
          </FloatingSelect>
        )}
        <FloatingInput label='Collection date *' type='date' value={form.collection_date} onChange={e => setForm(prev => ({ ...prev, collection_date: e.target.value }))} />
        <FloatingSelect label='Source' value={form.collection_source} onChange={e => setForm(prev => ({ ...prev, collection_source: e.target.value }))}>
          {SOURCES.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
        </FloatingSelect>
        {REFERENCE_SOURCES.has(form.collection_source) && (
          <FloatingInput label='Transaction reference' value={form.payment_reference} onChange={e => setForm(prev => ({ ...prev, payment_reference: e.target.value }))} />
        )}
      </div>

      <div className='mb-3'>
        <label className='small' style={{ display: 'block', marginBottom: 4 }}>Payment evidence (optional)</label>
        <input type='file' className='form-control' onChange={e => setEvidenceFile(e.target.files[0] || null)} />
      </div>

      <h5 className='card-title mb-1'>Items</h5>
      {items.map((it, idx) => (
        <div className='form-row mb8' key={idx} style={{ alignItems: 'flex-end' }}>
          <FloatingSelect label='Collection type' value={it.collection_type_id} onChange={e => updateItem(idx, { collection_type_id: e.target.value })} style={{ minWidth: 200 }}>
            <option value=''>-- type --</option>
            {types.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
          </FloatingSelect>
          <FloatingInput label='Amount' type='number' min='0' step='0.01' value={it.amount} onChange={e => updateItem(idx, { amount: e.target.value })} />
          {items.length > 1 && <button type='button' className='btn btn-sm btn-danger' onClick={() => removeItem(idx)}>Remove</button>}
        </div>
      ))}
      <button type='button' className='btn btn-sm' onClick={addItem}>+ Add item</button>

      <p className='small mt8'>Total: <strong>{total.toFixed(2)}</strong></p>

      <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
        <button type='button' className='btn btn-ghost' onClick={onClose}>Cancel</button>
        <button type='button' className='btn btn-primary' disabled={saving} onClick={save}>{saving ? 'Saving…' : 'Save as pending'}</button>
      </div>
    </Modal>
  )
}

export default function CollectionsTab() {
  const { can } = useMyPermissions()
  const [churches, setChurches] = useState([])
  const [types, setTypes] = useState([])
  const [collections, setCollections] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [filters, setFilters] = useState(EMPTY_FILTERS)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const [showManual, setShowManual] = useState(false)
  const [receiptForCollectionId, setReceiptForCollectionId] = useState(null)
  const [correctingCollection, setCorrectingCollection] = useState(null)
  const [auditTrailCollectionId, setAuditTrailCollectionId] = useState(null)

  const churchById = new Map(churches.map(c => [c.id, c]))

  useEffect(() => {
    membersApi.listChurches().then(setChurches).catch(() => {})
    membersApi.listCollectionTypes().then(setTypes).catch(() => {})
  }, [])

  async function refresh() {
    setLoading(true); setError('')
    try {
      const res = await membersApi.listCollections({ page, page_size: PAGE_SIZE, q: search, ...filters })
      setCollections(res.items || [])
      setTotal(res.total || 0)
    } catch (e) {
      setError(e.message || 'Failed to load collections')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [page, search, filters.church_id, filters.status, filters.collection_type_id]) // eslint-disable-line react-hooks/exhaustive-deps

  function setFilter(key, value) { setFilters(prev => ({ ...prev, [key]: value })); setPage(1) }
  function handleSearch(value) { setSearch(value); setPage(1) }

  const columns = [
    { key: 'member_name', label: 'Member', sortable: false },
    { key: 'church', label: 'Church', sortable: false, render: c => churchById.get(c.church_id)?.name || c.church_id },
    { key: 'collection_date', label: 'Date', sortable: false },
    {
      key: 'items', label: 'Items', sortable: false,
      render: c => (c.items || []).map(i => `${i.collection_type.name}: ${Number(i.amount).toFixed(2)}`).join(', '),
    },
    { key: 'total_amount', label: 'Total', sortable: false, render: c => Number(c.total_amount).toFixed(2) },
    { key: 'collection_source', label: 'Source', sortable: false, render: c => c.payment_reference ? `${c.collection_source} (${c.payment_reference})` : c.collection_source },
    { key: 'entry_method', label: 'Entry', sortable: false },
    { key: 'status', label: 'Status', sortable: false, render: c => <Badge level={statusLevel(c.status)}>{c.status}</Badge> },
    {
      key: 'receipt', label: 'Receipt', sortable: false,
      render: c => c.receipt_number ? (
        <button type='button' className='btn btn-sm btn-ghost' onClick={() => setReceiptForCollectionId(c.id)}><Eye size={14} /> {c.receipt_number}</button>
      ) : '—',
    },
  ]

  return (
    <div>
      <PageHeader
        title='Collections'
        subtitle='All Collections'
        icon={CreditCard}
        actions={can('manage_collections') && <button type='button' className='btn btn-primary' onClick={() => setShowManual(true)}>+ New Collection</button>}
      />

      <div className='toolbar mb-3'>
        <select className='form-select' style={{ maxWidth: 220 }} value={filters.church_id} onChange={e => setFilter('church_id', e.target.value)}>
          <option value=''>-- all churches --</option>
          {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
        <select className='form-select' style={{ maxWidth: 220 }} value={filters.status} onChange={e => setFilter('status', e.target.value)}>
          <option value=''>-- all statuses --</option>
          <option value='pending'>Pending</option>
          <option value='approved'>Approved</option>
          <option value='rejected'>Rejected</option>
        </select>
        <select className='form-select' style={{ maxWidth: 220 }} value={filters.collection_type_id} onChange={e => setFilter('collection_type_id', e.target.value)}>
          <option value=''>-- all types --</option>
          {types.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
        </select>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <DataTable
        columns={columns}
        rows={collections}
        loading={loading}
        emptyMessage='No collections found.'
        serverMode
        total={total}
        page={page}
        onPageChange={setPage}
        pageSize={PAGE_SIZE}
        searchValue={search}
        onSearchChange={handleSearch}
        actions={c => (
          <div className='flex gap8'>
            {c.status === 'approved' && can('correct_collections', c.church_id) && (
              <button type='button' className='btn btn-sm btn-ghost' title='Request a correction' onClick={() => setCorrectingCollection(c)}><Edit3 size={14} /></button>
            )}
            <button type='button' className='btn btn-sm btn-ghost' title='Audit trail' onClick={() => setAuditTrailCollectionId(c.id)}><Clock size={14} /></button>
          </div>
        )}
      />

      {showManual && (
        <ManualCollectionModal
          churches={churches}
          onClose={() => setShowManual(false)}
          onSaved={() => { setShowManual(false); refresh() }}
        />
      )}

      {receiptForCollectionId && (
        <CollectionReceiptView collectionId={receiptForCollectionId} onClose={() => setReceiptForCollectionId(null)} />
      )}

      {correctingCollection && (
        <CollectionCorrectionModal
          collection={correctingCollection}
          onClose={() => setCorrectingCollection(null)}
          onSaved={refresh}
        />
      )}

      {auditTrailCollectionId && (
        <CollectionAuditTrailView collectionId={auditTrailCollectionId} onClose={() => setAuditTrailCollectionId(null)} />
      )}
    </div>
  )
}
