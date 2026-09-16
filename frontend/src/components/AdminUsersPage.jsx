import React, { useState } from 'react'
import { RefreshCw, UserPlus } from 'react-feather'
import CreateUserForm from './CreateUserForm'
import Card from './Card'
import Modal from './Modal'
import PageHeader from './members-core/PageHeader'

export default function AdminUsersPage({
  fetchUsers,
  createUser,
  churches,
  currentUserChurchId,
  canAccessChurch,
  denyRestrictedChurchAccess,
  roleOptionsForCurrentUser,
  roleLabel,
  usersSearchField, setUsersSearchField,
  usersSearchText, setUsersSearchText,
  usersMaxRows, setUsersMaxRows,
  filteredUsers,
  beginEditUser,
  editingUser, setEditingUser,
  userEditForm, setUserEditForm,
  saveUserEdit,
  resetUser, setResetUser,
  resetPassword, setResetPassword,
  submitResetPassword,
  PASSWORD_HINT,
}){
  const [showCreateUser, setShowCreateUser] = useState(false)
  const activeUsers = (filteredUsers || []).filter(u => !u.disabled && !u.is_disabled && u.active !== false).length
  const churchesRepresented = new Set((filteredUsers || []).map(u => u.church).filter(v => v !== null && v !== undefined && v !== '')).size
  const rolesRepresented = new Set((filteredUsers || []).map(u => u.role).filter(Boolean)).size

  return (
    <div>
      <PageHeader
        title='Admin Users'
        subtitle='Accounts, roles, and church access'
        stats={[
          { label: 'Users', value: filteredUsers.length },
          { label: 'Active', value: activeUsers },
          { label: 'Roles', value: rolesRepresented },
          { label: 'Churches', value: churchesRepresented },
        ]}
        actions={(
          <>
            <button type='button' className='btn btn-ghost btn-sm' onClick={fetchUsers}><RefreshCw size={15} />Refresh</button>
            <button type='button' className='btn btn-primary btn-sm' onClick={() => setShowCreateUser(true)}><UserPlus size={15} />New User</button>
          </>
        )}
      />

      {showCreateUser && (
        <Modal title='Create user' onClose={() => setShowCreateUser(false)} maxWidth={760}>
          <CreateUserForm
            onCreate={async (...args) => {
              await createUser(...args)
              setShowCreateUser(false)
            }}
            churches={churches}
            scopedChurchId={currentUserChurchId}
            canAccessChurch={canAccessChurch}
            onRestrictedChurchAttempt={denyRestrictedChurchAccess}
            roleOptions={roleOptionsForCurrentUser()}
            roleLabel={roleLabel}
          />
        </Modal>
      )}

      <Card title="Users" outline variant="primary">
        <div className='toolbar'>
          <label>Search</label>
          <select value={usersSearchField} onChange={e=>setUsersSearchField(e.target.value)}>
            <option value='all'>All fields</option>
            <option value='username'>Username</option>
            <option value='name'>Name</option>
            <option value='email'>Email</option>
            <option value='phone'>Phone</option>
            <option value='role'>Role</option>
            <option value='church'>Church</option>
          </select>
          <input placeholder='Search users...' value={usersSearchText} onChange={e=>setUsersSearchText(e.target.value)} />
          <label>Max rows</label>
          <select value={usersMaxRows} onChange={e=>setUsersMaxRows(Number(e.target.value))}>
            {[10,30,50,100].map(n=> <option key={n} value={n}>{n}</option>)}
          </select>
          <span className='small'>Showing {Math.min(filteredUsers.length, usersMaxRows)} of {filteredUsers.length}</span>
        </div>

        <div className='table-wrap'>
          <table>
            <thead><tr><th>ID</th><th>Username</th><th>Name</th><th>Email</th><th>Phone</th><th>Role</th><th>Church</th><th>Actions</th></tr></thead>
            <tbody>
              {filteredUsers.slice(0, usersMaxRows).map(u=> {
                const chName = (churches||[]).find(c=> Number(c.id)===Number(u.church))?.name || u.church
                const fullName = [u.first_name, u.middle_name, u.last_name].filter(Boolean).join(' ')
                return (
                  <tr key={u.id}>
                    <td>{u.id}</td>
                    <td>{u.username}</td>
                    <td>{fullName || '-'}</td>
                    <td>{u.email || '-'}</td>
                    <td>{u.phone || '-'}</td>
                    <td>{roleLabel(u.role)}</td>
                    <td>{chName}</td>
                    <td className='cell-actions'>
                      <button type='button' className='btn btn-sm' onClick={()=>beginEditUser(u)}>Edit</button>
                      <button type='button' className='btn btn-sm' style={{marginLeft:8}} onClick={()=>{ setResetUser(u); setResetPassword('') }}>Reset Password</button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </Card>

      {editingUser && (
          <Modal title={`Edit User: ${editingUser.username}`} onClose={()=>setEditingUser(null)} maxWidth={760}>
            <div className='form-row'>
              <input value={userEditForm.username} placeholder='username' onChange={e=>setUserEditForm(prev=>({...prev, username:e.target.value}))} />
              <input value={userEditForm.first_name||''} placeholder='first name' onChange={e=>setUserEditForm(prev=>({...prev, first_name:e.target.value}))} />
              <input value={userEditForm.middle_name||''} placeholder='middle name' onChange={e=>setUserEditForm(prev=>({...prev, middle_name:e.target.value}))} />
              <input value={userEditForm.last_name||''} placeholder='last name' onChange={e=>setUserEditForm(prev=>({...prev, last_name:e.target.value}))} />
              <input value={userEditForm.email||''} placeholder='email' onChange={e=>setUserEditForm(prev=>({...prev, email:e.target.value}))} />
              <input value={userEditForm.phone||''} placeholder='phone' onChange={e=>setUserEditForm(prev=>({...prev, phone:e.target.value}))} />
              <select value={userEditForm.role} onChange={e=>setUserEditForm(prev=>({...prev, role:e.target.value}))}>
                {roleOptionsForCurrentUser().map(r => <option key={r} value={r}>{roleLabel(r)}</option>)}
              </select>
              <select value={userEditForm.church ?? ''} onChange={e=>{
                const v = e.target.value
                if(!canAccessChurch(v)){ denyRestrictedChurchAccess('user management'); return }
                setUserEditForm(prev=>({...prev, church:v}))
              }}>
                <option value=''>-- church --</option>
                {(churches||[]).map(ch=> <option key={ch.id} value={ch.id} disabled={!canAccessChurch(ch.id)}>{ch.name}</option>)}
              </select>
              <button type='button' className='btn btn-primary' onClick={saveUserEdit}>Save</button>
              <button type='button' className='btn btn-ghost' onClick={()=>setEditingUser(null)}>Cancel</button>
            </div>
          </Modal>
        )}

        {resetUser && (
          <Modal title={`Reset Password: ${resetUser.username}`} onClose={()=>{ setResetUser(null); setResetPassword('') }} maxWidth={460}>
            <div className='form-row'>
              <input type='password' placeholder='new password' value={resetPassword} onChange={e=>setResetPassword(e.target.value)} />
              <button type='button' className='btn btn-primary' onClick={submitResetPassword}>Set New Password</button>
              <button type='button' className='btn btn-ghost' onClick={()=>{ setResetUser(null); setResetPassword('') }}>Cancel</button>
            </div>
            <div className='hint'>{PASSWORD_HINT}</div>
          </Modal>
        )}
    </div>
  )
}
