import React, { useEffect, useState } from 'react'
import { Shield, UserCheck, UserX, Users } from 'react-feather'
import Alert from '../../Alert'
import Badge from '../../Badge'
import Modal from '../../Modal'
import PageHeader from '../PageHeader'
import StatCard, { StatGrid } from '../StatCard'
import DataTable from '../DataTable'
import { FloatingInput, FloatingSelect } from '../FloatingField'
import { membersApi } from '../api'

const EMPTY_NEW_USER = { username: '', password: '', email: '' }

export default function UsersTab() {
  const [users, setUsers] = useState([])
  const [roles, setRoles] = useState([])
  const [churches, setChurches] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const [showNewUser, setShowNewUser] = useState(false)
  const [newUser, setNewUser] = useState(EMPTY_NEW_USER)
  const [saving, setSaving] = useState(false)

  const [assignFor, setAssignFor] = useState(null) // user id currently showing the assign-role row
  const [assignRoleId, setAssignRoleId] = useState('')
  const [assignChurchId, setAssignChurchId] = useState('')

  const [resetFor, setResetFor] = useState(null)
  const [resetPassword, setResetPassword] = useState('')

  const churchById = new Map(churches.map(c => [c.id, c]))

  async function refresh() {
    setLoading(true); setError('')
    try {
      const [usersList, rolesList, churchesList] = await Promise.all([
        membersApi.listUsers(), membersApi.listRoles(), membersApi.listChurches(),
      ])
      setUsers(usersList); setRoles(rolesList); setChurches(churchesList)
    } catch (e) {
      setError(e.message || 'Failed to load users')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [])

  async function createUser() {
    if (!newUser.username || !newUser.password) { setError('Username and password are required'); return }
    setSaving(true); setError('')
    try {
      const payload = { username: newUser.username, password: newUser.password }
      if (newUser.email) payload.email = newUser.email
      await membersApi.createUser(payload)
      setNewUser(EMPTY_NEW_USER); setShowNewUser(false)
      await refresh()
    } catch (e) {
      setError(e.message || 'Failed to create user')
    } finally {
      setSaving(false)
    }
  }

  async function toggleActive(user) {
    try {
      await membersApi.updateUser(user.id, { is_active: !user.is_active })
      await refresh()
    } catch (e) { setError(e.message || 'Update failed') }
  }

  async function submitReset() {
    if (!resetPassword || resetPassword.length < 6) { setError('New password must be at least 6 characters'); return }
    try {
      await membersApi.resetUserPassword(resetFor, resetPassword)
      setResetFor(null); setResetPassword('')
    } catch (e) { setError(e.message || 'Reset failed') }
  }

  async function assignRole(userId) {
    if (!assignRoleId) { setError('Choose a role'); return }
    try {
      await membersApi.addUserRole(userId, { role_id: Number(assignRoleId), church_id: assignChurchId ? Number(assignChurchId) : null })
      setAssignFor(null); setAssignRoleId(''); setAssignChurchId('')
      await refresh()
    } catch (e) { setError(e.message || 'Failed to assign role') }
  }

  async function removeRole(userId, userRoleId) {
    if (!window.confirm('Remove this role from the user?')) return
    try {
      await membersApi.removeUserRole(userId, userRoleId)
      await refresh()
    } catch (e) { setError(e.message || 'Failed to remove role') }
  }

  const activeCount = users.filter(u => u.is_active).length

  const columns = [
    { key: 'username', label: 'Username' },
    { key: 'email', label: 'Email', render: u => u.email || '—' },
    {
      key: 'is_active', label: 'Active', sortValue: u => (u.is_active ? 1 : 0),
      render: u => (
        <button type='button' className='btn btn-sm btn-ghost' onClick={() => toggleActive(u)}>
          <Badge level={u.is_active ? 'success' : 'danger'}>{u.is_active ? 'active' : 'disabled'}</Badge>
        </button>
      ),
    },
    {
      key: 'roles', label: 'Roles', sortable: false,
      render: u => (
        <div>
          <div className='flex gap8' style={{ flexWrap: 'wrap' }}>
            {(u.roles || []).map(ur => (
              <span key={ur.id} className='badge text-bg-info'>
                {ur.role.name}{ur.church_id ? ` @ ${churchById.get(ur.church_id)?.name || ur.church_id}` : ' (all churches)'}
                <button type='button' className='link-button' style={{ marginLeft: 6 }} onClick={() => removeRole(u.id, ur.id)}>✕</button>
              </span>
            ))}
            {(u.roles || []).length === 0 && <span className='small'>No roles assigned</span>}
          </div>
          <button type='button' className='btn btn-sm btn-ghost mt8' onClick={() => { setAssignFor(u); setAssignRoleId(''); setAssignChurchId('') }}>+ Assign role</button>
        </div>
      ),
    },
  ]

  return (
    <div>
      <PageHeader
        title='User Management'
        subtitle='User Accounts'
        icon={UserCheck}
        actions={<button type='button' className='btn btn-primary' onClick={() => setShowNewUser(v => !v)}>{showNewUser ? 'Cancel' : '+ New User'}</button>}
      />

      <StatGrid>
        <StatCard label='Total Users' value={users.length} icon={Users} color='primary' />
        <StatCard label='Active' value={activeCount} icon={UserCheck} color='success' />
        <StatCard label='Disabled' value={users.length - activeCount} icon={UserX} color='danger' />
        <StatCard label='Roles Defined' value={roles.length} icon={Shield} color='info' />
      </StatGrid>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      {showNewUser && (
        <Modal title='New user' onClose={() => { setShowNewUser(false); setNewUser(EMPTY_NEW_USER) }} maxWidth={440}>
          <div className='kv' style={{ gap: 10 }}>
            <FloatingInput label='Username' value={newUser.username} onChange={e => setNewUser(prev => ({ ...prev, username: e.target.value }))} />
            <FloatingInput label='Password' type='password' value={newUser.password} onChange={e => setNewUser(prev => ({ ...prev, password: e.target.value }))} />
            <FloatingInput label='Email (optional)' value={newUser.email} onChange={e => setNewUser(prev => ({ ...prev, email: e.target.value }))} />
          </div>
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={() => { setShowNewUser(false); setNewUser(EMPTY_NEW_USER) }}>Cancel</button>
            <button type='button' className='btn btn-primary' disabled={saving} onClick={createUser}>{saving ? 'Creating…' : 'Create'}</button>
          </div>
        </Modal>
      )}

      <DataTable
        columns={columns}
        rows={users}
        loading={loading}
        emptyMessage='No users yet.'
        actions={u => (
          <button type='button' className='btn btn-sm' onClick={() => { setResetFor(u.id); setResetPassword('') }}>Reset password</button>
        )}
      />

      {resetFor !== null && (
        <Modal title='Reset password' onClose={() => { setResetFor(null); setResetPassword('') }} maxWidth={380}>
          <FloatingInput label='New password' type='password' value={resetPassword} onChange={e => setResetPassword(e.target.value)} />
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={() => { setResetFor(null); setResetPassword('') }}>Cancel</button>
            <button type='button' className='btn btn-primary' onClick={submitReset}>Save</button>
          </div>
        </Modal>
      )}

      {assignFor && (
        <Modal title={`Assign role — ${assignFor.username}`} onClose={() => setAssignFor(null)} maxWidth={420}>
          <div className='kv' style={{ gap: 10 }}>
            <FloatingSelect label='Role' value={assignRoleId} onChange={e => setAssignRoleId(e.target.value)}>
              <option value=''>-- role --</option>
              {roles.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
            </FloatingSelect>
            <FloatingSelect label='Church' value={assignChurchId} onChange={e => setAssignChurchId(e.target.value)}>
              <option value=''>-- all churches --</option>
              {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
            </FloatingSelect>
          </div>
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={() => setAssignFor(null)}>Cancel</button>
            <button type='button' className='btn btn-primary' onClick={() => assignRole(assignFor.id)}>Assign</button>
          </div>
        </Modal>
      )}
    </div>
  )
}
