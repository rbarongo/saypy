import React, { useEffect, useState } from 'react'
import { CreditCard, Edit3, LogOut, MessageSquare, User } from 'react-feather'
import Alert from '../Alert'
import { membersApi } from '../members-core/api'
import ProfileTab from './ProfileTab'
import CorrectionsTab from './CorrectionsTab'
import ContributionsTab from './ContributionsTab'
import FeedbackTab from './FeedbackTab'

const TABS = [
  { id: 'profile', label: 'My Profile', icon: User },
  { id: 'contributions', label: 'My Contributions', icon: CreditCard },
  { id: 'corrections', label: 'Corrections', icon: Edit3 },
  { id: 'feedback', label: 'Feedback', icon: MessageSquare },
]

function initialsOf(member) {
  if (!member) return '·'
  return `${member.first_name?.[0] || ''}${member.last_name?.[0] || ''}`.toUpperCase() || '·'
}

// Rendered by App.jsx instead of <CoreApp> whenever the logged-in account is
// member-linked (`user.member_id` truthy) — see App.jsx's routing-split
// branch. Bridges the already-authenticated top-level session into
// members-core's `api.js` (same backend, same JWT — mirrors how CoreApp
// bridges `sharedToken`), then owns its own simple top nav instead of the
// admin sidebar shell.
export default function MemberPortalApp({ token, user, onLogout }) {
  const [activeTab, setActiveTab] = useState('profile')
  const [member, setMember] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    if (token) membersApi.setSession(token, user)
  }, [token, user])

  async function refreshMember() {
    setLoading(true); setError('')
    try {
      const m = await membersApi.getMyMember()
      setMember(m)
    } catch (e) {
      setError(e.message || 'Failed to load your profile')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refreshMember() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  function logout() {
    membersApi.setSession(null, null)
    onLogout()
  }

  return (
    <div className='portal-shell'>
      <header className='portal-header'>
        <div className='portal-header-identity'>
          <div className='portal-avatar'>{initialsOf(member)}</div>
          <div>
            <div className='portal-header-name'>{member ? `${member.first_name} ${member.last_name}` : 'Member Portal'}</div>
            {member && <div className='portal-header-meta'>Member # {member.member_number}{member.church_name ? ` · ${member.church_name}` : ''}</div>}
          </div>
        </div>
        <nav className='portal-nav'>
          {TABS.map(t => {
            const Icon = t.icon
            return (
              <button
                key={t.id}
                type='button'
                className={`portal-nav-pill${activeTab === t.id ? ' portal-nav-pill--active' : ''}`}
                onClick={() => setActiveTab(t.id)}
              >
                <Icon size={14} /> {t.label}
              </button>
            )
          })}
          <button type='button' className='portal-nav-pill portal-nav-pill--ghost' onClick={logout}>
            <LogOut size={14} /> Sign out
          </button>
        </nav>
      </header>

      <main className='portal-main'>
        <div className='portal-container'>
          {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}
          {loading && !member && <p className='small'>Loading your profile…</p>}
          {member && activeTab === 'profile' && <ProfileTab member={member} onSaved={refreshMember} />}
          {member && activeTab === 'contributions' && <ContributionsTab />}
          {member && activeTab === 'corrections' && <CorrectionsTab member={member} onApplied={refreshMember} />}
          {member && activeTab === 'feedback' && <FeedbackTab />}
        </div>
      </main>
    </div>
  )
}
