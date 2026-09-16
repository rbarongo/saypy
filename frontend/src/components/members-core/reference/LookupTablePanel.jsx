import React, { useEffect, useState } from 'react'
import { Edit2, Trash2 } from 'react-feather'
import Alert from '../../Alert'
import Modal from '../../Modal'
import DataTable from '../DataTable'
import { FloatingInput } from '../FloatingField'

const EMPTY_EXTRA = []

/**
 * Generic CRUD table for a {code, name, ...extraFields} reference-data
 * resource — reused for household roles, membership categories, member
 * statuses, contact types, nationalities, churches, roles, and permissions
 * instead of writing each one out by hand. Search/sort/pagination come from
 * the shared DataTable; the add/edit form stays hidden until "+ Add"/"Edit"
 * is clicked.
 *
 * `api` must expose { list, create, update, remove }.
 * `extraFields` (optional): [{ key, label, placeholder }] for resource-specific
 * columns beyond code/name (e.g. nationalities' `dial_code`).
 */
export default function LookupTablePanel({ api, extraFields = EMPTY_EXTRA }) {
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const [editingId, setEditingId] = useState(null) // null = none, 'new' = create form
  const [form, setForm] = useState({})
  const [saving, setSaving] = useState(false)

  async function refresh() {
    setLoading(true); setError('')
    try {
      setItems(await api.list())
    } catch (e) {
      setError(e.message || 'Failed to load')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [api])

  function startCreate() {
    const blank = { code: '', name: '' }
    extraFields.forEach(f => { blank[f.key] = '' })
    setForm(blank)
    setEditingId('new')
  }

  function startEdit(item) {
    const next = { code: item.code, name: item.name }
    extraFields.forEach(f => { next[f.key] = item[f.key] ?? '' })
    setForm(next)
    setEditingId(item.id)
  }

  function cancelEdit() { setEditingId(null); setForm({}) }

  async function save() {
    setSaving(true); setError('')
    try {
      const payload = { ...form }
      extraFields.forEach(f => { if (payload[f.key] === '') payload[f.key] = null })
      if (editingId === 'new') {
        await api.create(payload)
      } else {
        await api.update(editingId, payload)
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
    ...extraFields.map(f => ({ key: f.key, label: f.label, render: row => row[f.key] ?? '—' })),
  ]

  return (
    <div>
      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      {editingId !== null && (
        <Modal title={editingId === 'new' ? 'New entry' : 'Edit entry'} onClose={cancelEdit} maxWidth={480}>
          <div className='kv' style={{ gap: 10 }}>
            <FloatingInput label='Code' value={form.code || ''} onChange={e => setForm(prev => ({ ...prev, code: e.target.value }))} />
            <FloatingInput label='Name' value={form.name || ''} onChange={e => setForm(prev => ({ ...prev, name: e.target.value }))} />
            {extraFields.map(f => (
              <FloatingInput
                key={f.key}
                label={f.label}
                value={form[f.key] || ''}
                onChange={e => setForm(prev => ({ ...prev, [f.key]: e.target.value }))}
              />
            ))}
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
        emptyMessage='No entries yet.'
        toolbarExtra={editingId === null && <button type='button' className='btn btn-sm btn-primary' onClick={startCreate}>+ Add</button>}
        actions={item => (
          <div className='flex gap8'>
            <button type='button' className='btn btn-sm btn-light' onClick={() => startEdit(item)} title='Edit'><Edit2 size={16} /></button>
            <button type='button' className='btn btn-sm btn-outline-danger' onClick={() => remove(item)} title='Delete'><Trash2 size={16} /></button>
          </div>
        )}
      />
    </div>
  )
}
