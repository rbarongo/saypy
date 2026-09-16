import React, { useState } from 'react'
import { Edit3, Plus } from 'react-feather'
import Alert from './Alert'
import Card from './Card'
import Modal from './Modal'
import PageHeader from './members-core/PageHeader'

const ROLE_FLAGS = [
  ['can_view_dashboard', 'Dashboard'],
  ['can_manage_users', 'Users'],
  ['can_manage_members', 'Members'],
  ['can_manage_collections', 'Collections'],
  ['can_manage_members_collections', 'Members Collections'],
  ['can_delete_members_collections', 'Delete Collections'],
  ['can_unlock_members_collections', 'Unlock Collections'],
  ['can_view_collection_codes', 'View Collection Codes'],
  ['can_view_reports', 'Reports'],
  ['can_manage_settings', 'Settings'],
]

export default function SettingsPage({
  user,
  currentUserChurchId,
  systemName,
  showEditSystemName, setShowEditSystemName,
  editingSystemName, setEditingSystemName,
  submitSystemName,
  appName,
  showEditAppName, setShowEditAppName,
  editingAppName, setEditingAppName,
  submitAppName,
  rolesCatalog,
  newRole, setNewRole,
  createRolePolicy,
  editingRole, setEditingRole,
  saveRolePolicy,
  removeRolePolicy,
}){
  const isSystemAdmin = String(user?.role || '').toLowerCase() === 'system_admin'
  const [showAddRole, setShowAddRole] = useState(false)
  const editablePolicyCount = (rolesCatalog || []).filter(r => !r.system_protected).length

  return (
    <div>
      <PageHeader
        title='Settings'
        subtitle='System identity and role policies'
        stats={[
          { label: 'Role policies', value: (rolesCatalog || []).length },
          { label: 'Editable', value: editablePolicyCount },
          { label: 'Church scope', value: currentUserChurchId || '-' },
        ]}
        actions={isSystemAdmin ? <button type='button' className='btn btn-primary' onClick={() => setShowAddRole(true)}><Plus size={15} />Add Role</button> : null}
      />

      {!currentUserChurchId && (
        <div className='mb8'>
          <Alert level='warning' title='No church assigned'>Only local church admins can access settings.</Alert>
        </div>
      )}

      {isSystemAdmin && (
        <Card title={<span>System Name <span className='small text-muted' style={{ fontWeight: 'normal' }}>(shown on login screen)</span></span>} outline variant="primary">
          <div className='settings-value-row'>
            <span>{systemName}</span>
            <button type='button' className='btn btn-sm btn-ghost' onClick={()=>{ setEditingSystemName(systemName); setShowEditSystemName(true) }}><Edit3 size={14} />Edit</button>
          </div>
        </Card>
      )}

      <Card title="Application Name" outline variant="info">
        <div className='settings-value-row'>
          <span>{appName}</span>
          <button type='button' className='btn btn-sm btn-ghost' onClick={()=>{ setEditingAppName(appName); setShowEditAppName(true); }} disabled={!currentUserChurchId}><Edit3 size={14} />Edit</button>
        </div>
      </Card>

      <Card title="Role Policy Grid" outline variant="success" 
        footer={!isSystemAdmin ? <p className='small mb-0 text-muted'>Local admin can assign existing roles to local church users but cannot define or update role policies.</p> : null}
      >
        {isSystemAdmin && (
          <div className='table-wrap'>
            <table>
              <thead>
                <tr>
                  <th>Role</th>
                  <th>Display Name</th>
                  {ROLE_FLAGS.map(([flag, label]) => <th key={flag}>{label}</th>)}
                  <th>Manage Roles</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {(rolesCatalog || []).map(r=> (
                  <tr key={r.role}>
                    <td>{r.role}</td>
                    <td>{r.display_name || r.role}</td>
                    {ROLE_FLAGS.map(([flag]) => (
                      <td key={flag}>{r[flag] ? 'Yes' : 'No'}</td>
                    ))}
                    <td>{r.can_manage_roles ? 'Yes':'No'}</td>
                    <td className='cell-actions'>
                      <button type='button' className='btn btn-sm btn-ghost' onClick={()=>setEditingRole({...r})}>Edit</button>
                      {!r.system_protected && <button type='button' className='btn btn-sm btn-ghost text-danger' style={{marginLeft:6}} onClick={()=>removeRolePolicy(r.role)}>Delete</button>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {showEditSystemName && (
        <Modal title='Edit system name' onClose={()=>{ setShowEditSystemName(false); setEditingSystemName('') }} maxWidth={460}>
          <div className='form-row'>
            <input type='text' value={editingSystemName} onChange={e=>setEditingSystemName(e.target.value)} placeholder='System name' style={{minWidth:220}} />
            <button type='button' className='btn btn-primary' onClick={submitSystemName}>Save</button>
            <button type='button' className='btn btn-ghost' onClick={()=>{ setShowEditSystemName(false); setEditingSystemName('') }}>Cancel</button>
          </div>
        </Modal>
      )}

      {showEditAppName && (
        <Modal title='Edit application name' onClose={()=>{ setShowEditAppName(false); setEditingAppName(''); }} maxWidth={460}>
          <div className='form-row'>
            <input type='text' value={editingAppName} onChange={e=>setEditingAppName(e.target.value)} placeholder='Application name' />
            <button type='button' className='btn btn-primary' onClick={submitAppName}>Save</button>
            <button type='button' className='btn btn-ghost' onClick={()=>{ setShowEditAppName(false); setEditingAppName(''); }}>Cancel</button>
          </div>
        </Modal>
      )}

      {showAddRole && (
        <Modal title='Add role policy' onClose={()=>setShowAddRole(false)} maxWidth={720}>
          <div className='form-grid mb8'>
            <input placeholder='role key (e.g. auditor)' value={newRole.role} onChange={e=>setNewRole(prev=>({...prev, role:e.target.value}))} />
            <input placeholder='display name' value={newRole.display_name} onChange={e=>setNewRole(prev=>({...prev, display_name:e.target.value}))} />
          </div>
          <div className='permission-grid mb8'>
            {ROLE_FLAGS.map(([flag, label]) => (
              <label key={flag} className='field-chip'>
                <input type='checkbox' checked={!!newRole[flag]} onChange={e=>setNewRole(prev=>({...prev, [flag]:e.target.checked}))} /> {label}
              </label>
            ))}
          </div>
          <div className='modal-actions'>
            <button type='button' className='btn btn-primary' onClick={async () => { await createRolePolicy(); setShowAddRole(false) }}>Add Role</button>
            <button type='button' className='btn btn-ghost' onClick={()=>setShowAddRole(false)}>Cancel</button>
          </div>
        </Modal>
      )}

      {editingRole && (
        <Modal title={`Edit role: ${editingRole.role}`} onClose={()=>setEditingRole(null)} maxWidth={760}>
          <div className='form-grid mb8'>
            <input value={editingRole.display_name || ''} onChange={e=>setEditingRole(prev=>({...prev, display_name:e.target.value}))} placeholder='Display name' />
            <label className='field-chip'>
              <input type='checkbox' checked={!!editingRole.can_manage_roles} onChange={e=>setEditingRole(prev=>({...prev, can_manage_roles:e.target.checked}))} /> Manage Roles
            </label>
          </div>
          <div className='permission-grid mb8'>
            {ROLE_FLAGS.map(([flag, label]) => (
              <label key={flag} className='field-chip'>
                <input type='checkbox' checked={!!editingRole[flag]} onChange={e=>setEditingRole(prev=>({...prev, [flag]:e.target.checked}))} /> {label}
              </label>
            ))}
          </div>
          <div className='modal-actions'>
            <button type='button' className='btn btn-primary' onClick={saveRolePolicy}>Save</button>
            <button type='button' className='btn btn-ghost' onClick={()=>setEditingRole(null)}>Cancel</button>
          </div>
        </Modal>
      )}
    </div>
  )
}
