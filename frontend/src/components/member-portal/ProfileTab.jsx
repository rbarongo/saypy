import React, { useState } from 'react'
import { Bell, Phone, User } from 'react-feather'
import Alert from '../Alert'
import Badge from '../Badge'
import { membersApi } from '../members-core/api'

const CHANNELS = [
  { value: 'email', label: 'Email' },
  { value: 'sms', label: 'SMS' },
  { value: 'whatsapp', label: 'WhatsApp' },
  { value: 'physical', label: 'Physical / in-person' },
]

// Matches app.models.CommunicationCategory — lets a member pick a different
// channel per kind of message instead of one blanket setting for everything.
const CATEGORIES = [
  { value: 'general', label: 'General communications' },
  { value: 'offering_receipts', label: 'Offering receipts' },
  { value: 'church_announcements', label: 'Church announcements' },
]

export default function ProfileTab({ member, onSaved }) {
  const [preferences, setPreferences] = useState(() => {
    const byCategory = member.communication_preferences || {}
    const initial = {}
    CATEGORIES.forEach(c => { initial[c.value] = new Set(byCategory[c.value] || []) })
    return initial
  })
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [success, setSuccess] = useState('')

  function toggle(category, channel) {
    setPreferences(prev => {
      const next = new Set(prev[category])
      if (next.has(channel)) next.delete(channel); else next.add(channel)
      return { ...prev, [category]: next }
    })
  }

  async function save() {
    setSaving(true); setError(''); setSuccess('')
    try {
      const payload = {}
      CATEGORIES.forEach(c => { payload[c.value] = Array.from(preferences[c.value]) })
      await membersApi.updateMyPreferences(payload)
      setSuccess('Preferences saved')
      onSaved && onSaved()
    } catch (e) {
      setError(e.message || 'Failed to save preferences')
    } finally {
      setSaving(false)
    }
  }

  const rows = [
    ['Member #', member.member_number],
    ['First name', member.first_name],
    ['Middle name', member.middle_name || '—'],
    ['Last name', member.last_name],
    ['Gender', member.gender || '—'],
    ['Date of birth', member.date_of_birth || '—'],
    ['National ID', member.national_id || '—'],
    ['Nationality', member.nationality?.name || '—'],
    ['Household role', member.household_role?.name || '—'],
    ['Membership category', member.membership_category?.name || '—'],
    ['Family', member.family_name || '—'],
    ['Church role(s)', (member.church_role_names || []).join(', ') || '—'],
    ['Sabbath School group', member.sabbath_school_group_name || '—'],
    ['Organisation(s)', (member.organisation_names || []).join(', ') || '—'],
    ['Address', member.address || '—'],
    ['Occupation', member.occupation || '—'],
  ]

  return (
    <div>
      <div className='portal-section-title'>
        <span className='portal-section-icon'><User size={16} /></span>
        My Profile
        <Badge level={member.status?.code === 'active' ? 'success' : 'secondary'}>{member.status?.name || 'Unknown'}</Badge>
      </div>

      <div className='portal-card card--accent'>
        <div className='kv' style={{ gap: 2 }}>
          {rows.map(([label, value]) => (
            <div key={label} className='form-row' style={{ justifyContent: 'space-between', padding: '7px 0', borderBottom: '1px dashed var(--border)' }}>
              <span className='small' style={{ color: 'var(--text-muted)' }}>{label}</span>
              <strong>{value}</strong>
            </div>
          ))}
        </div>
      </div>

      <div className='portal-section-title' style={{ fontSize: 16 }}>
        <span className='portal-section-icon'><Phone size={15} /></span>
        Contact details
      </div>
      <div className='portal-card'>
        {(member.contacts || []).length === 0 ? (
          <p className='hint'>No contact details on file.</p>
        ) : (
          <ul className='small' style={{ margin: 0, paddingLeft: 18 }}>
            {member.contacts.map(c => <li key={c.id}>{c.contact_type?.name}: {c.contact}</li>)}
          </ul>
        )}
      </div>

      <div className='portal-section-title' style={{ fontSize: 16 }}>
        <span className='portal-section-icon'><Bell size={15} /></span>
        Communication preferences
      </div>
      <div className='portal-card'>
        <p className='small' style={{ marginBottom: 10 }}>Choose how you'd like to be reached, for each kind of communication.</p>
        {error && <div className='mb8'><Alert level='error' title='Could not save'>{error}</Alert></div>}
        {success && <div className='mb8'><Alert level='success' title='Saved'>{success}</Alert></div>}

        <div className='kv' style={{ gap: 12, marginBottom: 12 }}>
          {CATEGORIES.map(cat => (
            <div key={cat.value} className='subpanel'>
              <label className='small' style={{ display: 'block', marginBottom: 6, fontWeight: 700, color: 'var(--text)' }}>{cat.label}</label>
              <div className='form-row'>
                {CHANNELS.map(ch => (
                  <label key={ch.value} className='small' style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <input type='checkbox' checked={preferences[cat.value].has(ch.value)} onChange={() => toggle(cat.value, ch.value)} />
                    {ch.label}
                  </label>
                ))}
              </div>
            </div>
          ))}
        </div>

        <button type='button' className='btn btn-sm btn-primary' disabled={saving} onClick={save}>{saving ? 'Saving…' : 'Save preferences'}</button>
      </div>
    </div>
  )
}
