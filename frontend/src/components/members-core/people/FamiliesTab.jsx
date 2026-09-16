import React, { useEffect, useState } from 'react'
import { Edit2, Trash2, Users, List, GitBranch } from 'react-feather'
import Alert from '../../Alert'
import Modal from '../../Modal'
import PageHeader from '../PageHeader'
import DataTable from '../DataTable'
import { membersApi } from '../api'
import { fullName } from '../constants'
import { FloatingInput, FloatingSelect } from '../FloatingField'
import useReferenceData from '../useReferenceData'
import FamilyTreeView from './FamilyTreeView'

const EMPTY_FORM = { name: '', alias: '', church_id: '', leader_id: '' }

export default function FamiliesTab(){
  const ref = useReferenceData()

  const [families, setFamilies] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [churchFilter, setChurchFilter] = useState('')

  const [showForm, setShowForm] = useState(false)
  const [editingFamily, setEditingFamily] = useState(null)
  const [form, setForm] = useState(EMPTY_FORM)
  const [saving, setSaving] = useState(false)
  const [churchMembers, setChurchMembers] = useState([])

  const [rosterFor, setRosterFor] = useState(null)
  const [roster, setRoster] = useState(null)
  const [rosterLoading, setRosterLoading] = useState(false)
  const [rosterError, setRosterError] = useState('')
  const [rosterView, setRosterView] = useState('table') // 'table' | 'tree'

  const churchById = new Map(ref.churches.map(c => [c.id, c]))

  async function refresh(){
    setLoading(true); setError('')
    try{
      setFamilies(await membersApi.listFamilies({ church_id: churchFilter, page_size: 200 }))
    }catch(e){
      setError(e.message || 'Failed to load families')
    }finally{
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [churchFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if(!showForm || !form.church_id){ setChurchMembers([]); return }
    let cancelled = false
    membersApi.listMembers({ church_id: form.church_id, page_size: 200 })
      .then(res => { if(!cancelled) setChurchMembers(res.items || []) })
      .catch(() => { if(!cancelled) setChurchMembers([]) })
    return () => { cancelled = true }
  }, [showForm, form.church_id])

  function openNew(){ setEditingFamily(null); setForm({ ...EMPTY_FORM, church_id: churchFilter }); setShowForm(true) }
  function openEdit(f){ setEditingFamily(f); setForm({ name: f.name, alias: f.alias || '', church_id: f.church_id, leader_id: f.leader_id ?? '' }); setShowForm(true) }
  function closeForm(){ setShowForm(false); setEditingFamily(null) }

  async function save(){
    setSaving(true); setError('')
    try{
      const leaderId = form.leader_id === '' ? null : Number(form.leader_id)
      if(editingFamily){
        await membersApi.updateFamily(editingFamily.id, { name: form.name, alias: form.alias || null, leader_id: leaderId })
      }else{
        await membersApi.createFamily({ name: form.name, alias: form.alias || null, church_id: Number(form.church_id), leader_id: leaderId })
      }
      closeForm()
      await refresh()
    }catch(e){
      setError(e.message || 'Save failed')
    }finally{
      setSaving(false)
    }
  }

  async function remove(f){
    if(!window.confirm(`Delete "${f.name}"? Members must be moved out of the family first.`)) return
    try{
      await membersApi.deleteFamily(f.id)
      await refresh()
    }catch(e){
      setError(e.message || 'Delete failed')
    }
  }

  async function openRoster(f){
    setRosterFor(f); setRoster(null); setRosterLoading(true); setRosterError(''); setRosterView('table')
    try{
      setRoster(await membersApi.getFamilyRoster(f.id))
    }catch(e){
      setRosterError(e.message || 'Failed to load roster')
    }finally{
      setRosterLoading(false)
    }
  }

  async function setLeader(memberId){
    if(!rosterFor) return
    setRosterError('')
    try{
      await membersApi.updateFamily(rosterFor.id, { leader_id: memberId })
      setRoster(await membersApi.getFamilyRoster(rosterFor.id))
      await refresh()
    }catch(e){
      setRosterError(e.message || 'Failed to set leader')
    }
  }

  const columns = [
    { key: 'name', label: 'Name' },
    { key: 'alias', label: 'Alias', render: f => f.alias || '—' },
    { key: 'church', label: 'Church', sortable: false, render: f => churchById.get(f.church_id)?.name || f.church_id },
    { key: 'leader_name', label: 'Leader', render: f => f.leader_name || '—' },
    { key: 'member_count', label: 'Members', render: f => <span className='badge text-bg-secondary'>{f.member_count}</span> },
  ]

  return (
    <div>
      <PageHeader
        title='Member Management'
        subtitle='Families'
        icon={Users}
        actions={<button type='button' className='btn btn-primary' onClick={openNew}>+ New Family</button>}
      />

      <div className='row row-cols-1 row-cols-sm-3 g-3 mb-1'>
        <div className='col'>
          <div className='card'><div className='card-body'>
            <h5 className='card-title'>Families</h5>
            <h1 className='mt-1 mb-0'>{families.length}</h1>
          </div></div>
        </div>
      </div>

      <div className='toolbar'>
        <select className='form-select' style={{ maxWidth: 220 }} value={churchFilter} onChange={e => setChurchFilter(e.target.value)}>
          <option value=''>-- all churches --</option>
          {ref.churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <DataTable
        columns={columns}
        rows={families}
        loading={loading}
        emptyMessage='No families yet.'
        actions={f => (
          <div className='flex gap8'>
            <button type='button' className='btn btn-sm btn-light' onClick={() => openEdit(f)} title='Edit'><Edit2 size={16} /></button>
            <button type='button' className='btn btn-sm btn-ghost' onClick={() => openRoster(f)} title='Roster'><Users size={16} /></button>
            <button type='button' className='btn btn-sm btn-outline-danger' onClick={() => remove(f)} title='Delete'><Trash2 size={16} /></button>
          </div>
        )}
      />

      {showForm && (
        <Modal title={editingFamily ? `Edit ${editingFamily.name}` : 'New Family'} onClose={closeForm} maxWidth={480}>
          <div className='kv' style={{ gap: 10 }}>
            <FloatingInput label='Family name *' value={form.name} onChange={e => setForm(prev => ({ ...prev, name: e.target.value }))} />
            <FloatingInput label='Alias' value={form.alias} onChange={e => setForm(prev => ({ ...prev, alias: e.target.value }))} />
            {!editingFamily && (
              <FloatingSelect label='Church *' value={form.church_id} onChange={e => setForm(prev => ({ ...prev, church_id: e.target.value, leader_id: '' }))}>
                <option value=''>-- church --</option>
                {ref.churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </FloatingSelect>
            )}
            <FloatingSelect label='Family leader' value={form.leader_id} onChange={e => setForm(prev => ({ ...prev, leader_id: e.target.value }))}>
              <option value=''>-- none --</option>
              {churchMembers.map(m => <option key={m.id} value={m.id}>{fullName(m)}</option>)}
            </FloatingSelect>
            <div className='flex gap8' style={{ justifyContent: 'flex-end' }}>
              <button type='button' className='btn btn-ghost' onClick={closeForm}>Cancel</button>
              <button type='button' className='btn btn-primary' disabled={saving} onClick={save}>{saving ? 'Saving…' : 'Save'}</button>
            </div>
          </div>
        </Modal>
      )}

      {rosterFor && (
        <Modal title={`${rosterFor.name} — Roster`} onClose={() => setRosterFor(null)} maxWidth={680}>
          {rosterLoading && <p className='small'>Loading…</p>}
          {rosterError && <Alert level='error' title='Failed to load roster'>{rosterError}</Alert>}
          {roster && (
            <div>
              <div className='flex gap8 mb8' style={{ justifyContent: 'space-between', alignItems: 'center' }}>
                <p className='small mb-0'>
                  Leader: {roster.leader ? `${roster.leader.first_name} ${roster.leader.last_name}` : 'none set'}
                  {' · '}
                  {roster.members.length} member{roster.members.length === 1 ? '' : 's'}
                </p>
                <div className='view-toggle'>
                  <button type='button' className={rosterView === 'table' ? 'active' : ''} onClick={() => setRosterView('table')}>
                    <List size={14} /> Table
                  </button>
                  <button type='button' className={rosterView === 'tree' ? 'active' : ''} onClick={() => setRosterView('tree')}>
                    <GitBranch size={14} /> Tree
                  </button>
                </div>
              </div>

              {rosterView === 'table' ? (
                <div className='table-wrap'>
                  <table>
                    <thead><tr><th>Name</th><th>Household role</th><th>Actions</th></tr></thead>
                    <tbody>
                      {roster.members.length === 0 && <tr><td colSpan={3} className='small'>No members in this family yet.</td></tr>}
                      {roster.members.map(m => (
                        <tr key={m.id}>
                          <td>{m.first_name} {m.last_name}</td>
                          <td>{m.household_role.name}</td>
                          <td>
                            {roster.leader && roster.leader.id === m.id
                              ? <span className='badge text-bg-success'>leader</span>
                              : <button type='button' className='btn btn-sm btn-ghost' onClick={() => setLeader(m.id)}>Make leader</button>}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <FamilyTreeView roster={roster} onSetLeader={setLeader} />
              )}
            </div>
          )}
        </Modal>
      )}
    </div>
  )
}
