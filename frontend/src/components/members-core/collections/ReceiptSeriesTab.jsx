import React, { useEffect, useState } from 'react'
import { Key } from 'react-feather'
import Alert from '../../Alert'
import Badge from '../../Badge'
import Modal from '../../Modal'
import DataTable from '../DataTable'
import PageHeader from '../PageHeader'
import { FloatingInput, FloatingSelect } from '../FloatingField'
import { membersApi } from '../api'
import useMyPermissions from '../../../hooks/useMyPermissions'

const EMPTY_FORM = { church_id: '', prefix: '', range_start: '', range_end: '' }
const LOW_REMAINING_THRESHOLD = 20

function remainingLevel(series) {
  if (series.remaining_count === null || series.remaining_count === undefined) return 'secondary'
  if (series.remaining_count <= 0) return 'danger'
  if (series.remaining_count <= LOW_REMAINING_THRESHOLD) return 'warning'
  return 'success'
}

/** Admin screen for the persisted, per-church receipt-numbering register
 * (see receipt_service.py) — mirrors a real pre-numbered physical receipt
 * booklet: a declared range (from # to #), numbering starts at range_start,
 * and `remaining_count` tracks how many numbers are left before the series
 * is exhausted and a new range must be configured. `next_sequence` is
 * never directly editable, only `prefix`/`is_active`/`range_end`
 * (extend-only), to prevent number gaming. */
export default function ReceiptSeriesTab() {
  const { can } = useMyPermissions()
  const [churches, setChurches] = useState([])
  const [churchFilter, setChurchFilter] = useState('')
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [showNew, setShowNew] = useState(false)
  const [form, setForm] = useState(EMPTY_FORM)
  const [saving, setSaving] = useState(false)
  const [extending, setExtending] = useState(null)
  const [extendValue, setExtendValue] = useState('')

  const churchById = new Map(churches.map(c => [c.id, c]))
  const canManage = can('manage_receipts')

  useEffect(() => { membersApi.listChurches().then(setChurches).catch(() => {}) }, [])

  async function refresh() {
    setLoading(true); setError('')
    try {
      setItems(await membersApi.listReceiptSeries({ church_id: churchFilter }))
    } catch (e) {
      setError(e.message || 'Failed to load receipt series')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [churchFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  async function create() {
    const rangeStart = Number(form.range_start)
    const rangeEnd = Number(form.range_end)
    if (!form.church_id || !form.prefix.trim()) { setError('Church and prefix are both required'); return }
    if (!form.range_start || !form.range_end) { setError('Specify the receipt number range (from # to #)'); return }
    if (rangeEnd < rangeStart) { setError('The range end must be greater than or equal to the range start'); return }
    setSaving(true); setError('')
    try {
      await membersApi.createReceiptSeries({
        church_id: Number(form.church_id),
        prefix: form.prefix.trim(),
        range_start: rangeStart,
        range_end: rangeEnd,
      })
      setShowNew(false); setForm(EMPTY_FORM)
      await refresh()
    } catch (e) {
      setError(e.message || 'Failed to create receipt series')
    } finally {
      setSaving(false)
    }
  }

  async function toggleActive(series) {
    try {
      await membersApi.updateReceiptSeries(series.id, { is_active: !series.is_active })
      await refresh()
    } catch (e) {
      setError(e.message || 'Failed to update receipt series')
    }
  }

  async function submitExtend() {
    if (!extendValue || Number(extendValue) <= extending.range_end) { setError('Enter a new range end greater than the current one'); return }
    setSaving(true); setError('')
    try {
      await membersApi.updateReceiptSeries(extending.id, { range_end: Number(extendValue) })
      setExtending(null); setExtendValue('')
      await refresh()
    } catch (e) {
      setError(e.message || 'Failed to extend receipt series')
    } finally {
      setSaving(false)
    }
  }

  const columns = [
    { key: 'church', label: 'Church', sortable: false, render: s => churchById.get(s.church_id)?.name || s.church_id },
    { key: 'prefix', label: 'Prefix' },
    { key: 'range', label: 'Range', sortable: false, render: s => `${s.range_start} – ${s.range_end ?? '∞'}` },
    { key: 'next_sequence', label: 'Next number', render: s => `${s.prefix}-${String(s.next_sequence).padStart(6, '0')}` },
    {
      key: 'remaining_count', label: 'Remaining', render: s => (
        <Badge level={remainingLevel(s)}>{s.remaining_count === null || s.remaining_count === undefined ? 'unbounded' : `${s.remaining_count} left`}</Badge>
      ),
    },
    { key: 'is_active', label: 'Status', render: s => <span className={`badge text-bg-${s.is_active ? 'success' : 'secondary'}`}>{s.is_active ? 'active' : 'inactive'}</span> },
  ]

  if (!canManage) {
    return (
      <div>
        <PageHeader title='Collections' subtitle='Receipt Series' icon={Key} />
        <Alert level='error' title='Access restricted'>You don't have permission to manage receipt series.</Alert>
      </div>
    )
  }

  return (
    <div>
      <PageHeader
        title='Collections'
        subtitle='Receipt Series'
        icon={Key}
        actions={<button type='button' className='btn btn-primary' onClick={() => setShowNew(true)}>+ New Series</button>}
      />

      <div className='toolbar mb-3'>
        <select className='form-select' style={{ maxWidth: 220 }} value={churchFilter} onChange={e => setChurchFilter(e.target.value)}>
          <option value=''>-- all churches --</option>
          {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      {showNew && (
        <Modal title='New receipt series' onClose={() => { setShowNew(false); setForm(EMPTY_FORM) }} maxWidth={480}>
          <p className='hint mb8'>Matches a real pre-numbered physical receipt booklet — specify the exact number range it covers. A church can only have one active series at a time.</p>
          <div className='form-grid'>
            <FloatingSelect label='Church *' value={form.church_id} onChange={e => setForm(prev => ({ ...prev, church_id: e.target.value }))}>
              <option value=''>-- church --</option>
              {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
            </FloatingSelect>
            <FloatingInput label='Prefix *' value={form.prefix} onChange={e => setForm(prev => ({ ...prev, prefix: e.target.value }))} placeholder='e.g. RCT-KIB' />
            <FloatingInput label='Range start #*' type='number' min='1' value={form.range_start} onChange={e => setForm(prev => ({ ...prev, range_start: e.target.value }))} />
            <FloatingInput label='Range end #*' type='number' min='1' value={form.range_end} onChange={e => setForm(prev => ({ ...prev, range_end: e.target.value }))} />
          </div>
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={() => { setShowNew(false); setForm(EMPTY_FORM) }}>Cancel</button>
            <button type='button' className='btn btn-primary' disabled={saving} onClick={create}>{saving ? 'Saving…' : 'Save'}</button>
          </div>
        </Modal>
      )}

      <DataTable
        columns={columns}
        rows={items}
        loading={loading}
        emptyMessage='No receipt series configured yet — one is created automatically the first time a receipt is issued for a church.'
        actions={s => (
          <div className='flex gap8'>
            {s.range_end !== null && (
              <button type='button' className='btn btn-sm btn-ghost' onClick={() => { setExtending(s); setExtendValue(String(s.range_end)) }}>Extend range</button>
            )}
            <button type='button' className='btn btn-sm btn-light' onClick={() => toggleActive(s)}>{s.is_active ? 'Deactivate' : 'Activate'}</button>
          </div>
        )}
      />

      {extending && (
        <Modal title={`Extend ${extending.prefix}'s range`} onClose={() => setExtending(null)} maxWidth={380}>
          <p className='small mb8'>Current range: {extending.range_start} – {extending.range_end} ({extending.remaining_count} remaining). Set a new, higher range end.</p>
          <FloatingInput label='New range end #*' type='number' min={extending.range_end + 1} value={extendValue} onChange={e => setExtendValue(e.target.value)} />
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={() => setExtending(null)}>Cancel</button>
            <button type='button' className='btn btn-primary' disabled={saving} onClick={submitExtend}>{saving ? 'Saving…' : 'Extend'}</button>
          </div>
        </Modal>
      )}
    </div>
  )
}
