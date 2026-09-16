import React, { useEffect, useState } from 'react'
import { Edit2, Trash2 } from 'react-feather'
import Alert from '../../Alert'
import Modal from '../../Modal'
import DataTable from '../DataTable'
import PageHeader from '../PageHeader'
import { FloatingInput, FloatingSelect } from '../FloatingField'
import { membersApi } from '../api'

const EMPTY_FORM = { code: '', name: '', church_id: '', status: 'active' }

/**
 * CRUD for a `{code, name, church_id, status}` lookup — a collection type,
 * organisation, Sabbath School group, or church role, all sharing this
 * exact shape (NULL church_id = a global default available to every
 * church). Generalized out of the collection-types panel that was first
 * written for this shape, same dedup move as `LookupTablePanel` for pure
 * global lookups.
 *
 * `api` must expose `{list, create, update, remove}` (see
 * `buildChurchScopedLookupApi` in api.js).
 */
export default function ChurchScopedLookupTab({ api, title, subtitle, entityLabel, newButtonLabel, readOnly = false, icon }) {
  const [churches, setChurches] = useState([])
  const [churchFilter, setChurchFilter] = useState('')
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const [editingId, setEditingId] = useState(null) // null | 'new' | id
  const [form, setForm] = useState(EMPTY_FORM)
  const [saving, setSaving] = useState(false)

  const churchById = new Map(churches.map(c => [c.id, c]))

  useEffect(() => { membersApi.listChurches().then(setChurches).catch(() => {}) }, [])

  async function refresh() {
    setLoading(true); setError('')
    try {
      setItems(await api.list({ church_id: churchFilter }))
    } catch (e) {
      setError(e.message || `Failed to load ${entityLabel}s`)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [churchFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  function startCreate() { setForm({ ...EMPTY_FORM, church_id: churchFilter }); setEditingId('new') }
  function startEdit(item) { setForm({ code: item.code, name: item.name, church_id: item.church_id ?? '', status: item.status }); setEditingId(item.id) }
  function cancelEdit() { setEditingId(null); setForm(EMPTY_FORM) }

  async function save() {
    setSaving(true); setError('')
    try {
      if (editingId === 'new') {
        await api.create({
          code: form.code, name: form.name, status: form.status,
          church_id: form.church_id === '' ? null : Number(form.church_id),
        })
      } else {
        await api.update(editingId, { code: form.code, name: form.name, status: form.status })
      }
      cancelEdit()
      await refresh()
    } catch (e) {
      setError(e.message || 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  async function remove(item) {
    if (!window.confirm(`Delete "${item.name}"?`)) return
    setError('')
    try {
      await api.remove(item.id)
      await refresh()
    } catch (e) {
      setError(e.message || 'Delete failed')
    }
  }

  const columns = [
    { key: 'code', label: 'Code' },
    { key: 'name', label: 'Name' },
    { key: 'church', label: 'Church', sortable: false, render: item => item.church_id ? (churchById.get(item.church_id)?.name || item.church_id) : <span className='badge text-bg-secondary'>Global</span> },
    { key: 'status', label: 'Status', render: item => <span className={`badge text-bg-${item.status === 'active' ? 'success' : 'secondary'}`}>{item.status}</span> },
  ]

  const newButton = <button type='button' className='btn btn-primary' onClick={startCreate}>{newButtonLabel}</button>

  return (
    <div>
      {title ? (
        <PageHeader title={title} subtitle={subtitle} icon={icon} actions={readOnly ? null : newButton} />
      ) : (
        !readOnly && <div className='d-flex justify-content-end mb-3'>{newButton}</div>
      )}

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <div className='toolbar mb-3'>
        <select className='form-select' style={{ maxWidth: 220 }} value={churchFilter} onChange={e => setChurchFilter(e.target.value)}>
          <option value=''>-- all churches --</option>
          {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
      </div>

      {editingId !== null && (
        <Modal title={editingId === 'new' ? `New ${entityLabel}` : `Edit ${entityLabel}`} onClose={cancelEdit} maxWidth={520}>
          <div className='form-grid'>
            <FloatingInput label='Code *' value={form.code} onChange={e => setForm(prev => ({ ...prev, code: e.target.value }))} />
            <FloatingInput label='Name *' value={form.name} onChange={e => setForm(prev => ({ ...prev, name: e.target.value }))} />
            {editingId === 'new' && (
              <FloatingSelect label='Church' value={form.church_id} onChange={e => setForm(prev => ({ ...prev, church_id: e.target.value }))}>
                <option value=''>-- global (all churches) --</option>
                {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </FloatingSelect>
            )}
            <FloatingSelect label='Status' value={form.status} onChange={e => setForm(prev => ({ ...prev, status: e.target.value }))}>
              <option value='active'>Active</option>
              <option value='inactive'>Inactive</option>
            </FloatingSelect>
          </div>
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={cancelEdit}>Cancel</button>
            <button type='button' className='btn btn-primary' disabled={saving} onClick={save}>{saving ? 'Saving…' : 'Save'}</button>
          </div>
        </Modal>
      )}

      <DataTable
        columns={columns}
        rows={items}
        loading={loading}
        emptyMessage={`No ${entityLabel}s yet.`}
        actions={readOnly ? undefined : item => (
          <div className='flex gap8'>
            <button type='button' className='btn btn-sm btn-light' onClick={() => startEdit(item)} title='Edit'><Edit2 size={16} /></button>
            <button type='button' className='btn btn-sm btn-outline-danger' onClick={() => remove(item)} title='Delete'><Trash2 size={16} /></button>
          </div>
        )}
      />
    </div>
  )
}
