import React, { useEffect, useState } from 'react'
import { Grid, Key, Lock, Shield } from 'react-feather'
import Alert from '../Alert'
import { FloatingInput } from './FloatingField'
import { membersApi } from './api'
import PageHeader from './PageHeader'
import TabNav from './TabNav'
import DashboardTab from './DashboardTab'
import MembersTab from './people/MembersTab'
import FamiliesTab from './people/FamiliesTab'
import PendingRegistrationsTab from './people/PendingRegistrationsTab'
import CorrectionRequestsTab from './people/CorrectionRequestsTab'
import ReportsHub from './reports/ReportsHub'
import ReferenceDataTab from './reference/ReferenceDataTab'
import ChurchesTab from './access/ChurchesTab'
import RolesTab from './access/RolesTab'
import PermissionsTab from './access/PermissionsTab'
import AccessMatrixTab from './access/AccessMatrixTab'
import UsersTab from './access/UsersTab'
import CollectionTypesTab from './collections/CollectionTypesTab'
import CollectionsTab from './collections/CollectionsTab'
import CollectionImportWizard from './collections/CollectionImportWizard'
import CollectionGroupsTab from './collections/CollectionGroupsTab'
import ReceiptSeriesTab from './collections/ReceiptSeriesTab'
import CollectionAuditLogTab from './collections/CollectionAuditLogTab'
import AgentSubmissionsTab from './collections/AgentSubmissionsTab'

const DEFAULT_ROUTE = { menu: 'dashboard' }

function LoginGate({ onLoggedIn }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)

  async function doLogin() {
    if (!username.trim() || !password) { setError('Enter your username and password.'); return }
    setBusy(true); setError('')
    try {
      const data = await membersApi.login(username.trim(), password)
      membersApi.setSession(data.access_token, data.user)
      onLoggedIn(data.user)
    } catch (e) {
      setError(e.message || 'Sign in failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className='card' style={{ maxWidth: 420, margin: '48px auto' }}>
      <h3 className='page-title' style={{ marginBottom: 4 }}>Setup sign in</h3>
      <p className='small' style={{ marginBottom: 16 }}>Manage churches, members, families, and access control.</p>
      <div className='kv' style={{ gap: 10 }}>
        <FloatingInput label='Username' value={username} onChange={e => setUsername(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') doLogin() }} autoComplete='username' />
        <FloatingInput label='Password' type='password' value={password} onChange={e => setPassword(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') doLogin() }} autoComplete='current-password' />
        <button type='button' className='btn btn-primary' disabled={busy} onClick={doLogin}>{busy ? 'Signing in…' : 'Sign In'}</button>
      </div>
      {error && <div className='mt8'><Alert level='error' title='Sign in failed'>{error}</Alert></div>}
    </div>
  )
}

/**
 * Pure content renderer for the Setup module — navigation itself now lives in
 * the global left sidebar (SidebarNav.jsx's Setup tree), which drives this
 * via the `route` prop ({ menu, sub, subsub }). This component only owns the
 * login/session gate.
 */
export default function CoreApp({ sharedToken, route, setSetupRoute } = {}) {
  const [checkingSession, setCheckingSession] = useState(true)
  const [user, setUser] = useState(null)

  useEffect(() => {
    const token = sharedToken || membersApi.getToken()
    if (!token) {
      membersApi.setSession(null, null)
      setUser(null)
      setCheckingSession(false)
      return
    }
    if (sharedToken) membersApi.setSession(sharedToken, membersApi.getStoredUser())
    membersApi.me()
      .then(u => { setUser(u); if (sharedToken) membersApi.setSession(sharedToken, u) })
      .catch(() => { membersApi.setSession(null, null); setUser(null) })
      .finally(() => setCheckingSession(false))
  }, [sharedToken])

  function logout() {
    membersApi.setSession(null, null)
    setUser(null)
  }

  if (checkingSession) return <p className='small'>Checking session…</p>
  if (!user) return <LoginGate onLoggedIn={setUser} />

  const r = route || DEFAULT_ROUTE
  const activeTab = r.subsub || 'roles'

  function handleSelectTab(tabId) {
    if (setSetupRoute) {
      setSetupRoute({ menu: 'user_management', sub: 'access_control', subsub: tabId })
    }
  }

  return (
    <div key={`${r.menu}-${r.sub || ''}-${activeTab}`} className='core-content-area'>
      {/* <div className='toolbar' style={{ justifyContent: 'space-between' }}>
        <span className='small'>Signed in as <strong>{user.username}</strong> — {(user.roles || []).map(ur => ur.role.name).join(', ') || 'no roles assigned'}</span>
        {!sharedToken && <button type='button' className='btn btn-sm btn-ghost' onClick={logout}>Sign out</button>}
      </div> */}

      {r.menu === 'dashboard' && <DashboardTab />}
      {r.menu === 'member_management' && r.sub === 'families' && <FamiliesTab />}
      {r.menu === 'member_management' && r.sub === 'pending_registrations' && <PendingRegistrationsTab />}
      {r.menu === 'member_management' && r.sub === 'correction_requests' && <CorrectionRequestsTab />}
      {r.menu === 'member_management' && r.sub === 'reports' && <ReportsHub />}
      {r.menu === 'member_management' && r.sub !== 'families' && r.sub !== 'pending_registrations' && r.sub !== 'correction_requests' && r.sub !== 'reports' && <MembersTab />}
      {r.menu === 'church_management' && <ChurchesTab />}
      {r.menu === 'user_management' && r.sub === 'accounts' && <UsersTab />}
      {r.menu === 'user_management' && r.sub === 'access_control' && (
        <div>
          <PageHeader title='User Management' subtitle='Access Control' icon={Lock} />

          <TabNav
            tabs={[
              { id: 'roles', label: 'Roles', icon: Shield },
              { id: 'permissions', label: 'Permissions', icon: Key },
              { id: 'matrix', label: 'Access Matrix', icon: Grid },
            ]}
            activeId={activeTab}
            onSelect={handleSelectTab}
          />

          <div className="tab-content">
            {activeTab === 'roles' && <RolesTab />}
            {activeTab === 'permissions' && <PermissionsTab />}
            {activeTab === 'matrix' && <AccessMatrixTab />}
          </div>
        </div>
      )}
      {r.menu === 'configuration' && <ReferenceDataTab sub={r.sub} setSetupRoute={setSetupRoute} />}
      {r.menu === 'collection_management' && r.sub === 'types' && <CollectionTypesTab />}
      {r.menu === 'collection_management' && r.sub === 'import' && <CollectionImportWizard />}
      {r.menu === 'collection_management' && r.sub === 'groups' && <CollectionGroupsTab />}
      {r.menu === 'collection_management' && r.sub === 'receipt_series' && <ReceiptSeriesTab />}
      {r.menu === 'collection_management' && r.sub === 'audit_log' && <CollectionAuditLogTab />}
      {r.menu === 'collection_management' && r.sub === 'agent_submissions' && <AgentSubmissionsTab />}
      {r.menu === 'collection_management' && (r.sub === 'collections' || !r.sub) && <CollectionsTab />}
    </div>
  )
}
