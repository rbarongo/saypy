import React, { useEffect, useState } from 'react'
import { toast } from 'react-toastify'
import { Clock, Edit3 } from 'react-feather'
import Alert from '../Alert'
import Badge from '../Badge'
import { membersApi } from '../members-core/api'

// Mirrors the server-side allow-list in member_service.CORRECTABLE_FIELDS —
// membership/status/church/family stay staff-only via the admin MemberForm.
const FIELDS = [
  { key: 'middle_name', label: 'Middle name', type: 'text' },
  { key: 'address', label: 'Address', type: 'text' },
  { key: 'occupation', label: 'Occupation', type: 'text' },
  { key: 'date_of_birth', label: 'Date of birth', type: 'date' },
  { key: 'national_id', label: 'National ID', type: 'text' },
  { key: 'nationality_id', label: 'Nationality', type: 'nationality' },
  { key: 'notes', label: 'Notes', type: 'textarea' },
]

const STATUS_LEVEL = { pending: 'warning', approved: 'success', rejected: 'danger' }

export default function CorrectionsTab({ member, onApplied }) {
  const [nationalities, setNationalities] = useState([])
  const [form, setForm] = useState(() => ({
    middle_name: member.middle_name || '',
    address: member.address || '',
    occupation: member.occupation || '',
    date_of_birth: member.date_of_birth || '',
    national_id: member.national_id || '',
    nationality_id: member.nationality_id || member.nationality?.id || '',
    notes: member.notes || '',
  }))
  const [reason, setReason] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [success, setSuccess] = useState('')
  const [requests, setRequests] = useState([])
  const [loadingRequests, setLoadingRequests] = useState(true)

  useEffect(() => { membersApi.nationalities.list().then(setNationalities).catch(() => {}) }, [])

  async function refreshRequests() {
    setLoadingRequests(true)
    try {
      const list = await membersApi.listMyCorrectionRequests()
      setRequests(list || [])
    } catch (e) {
      // non-fatal — leave whatever list we already had
    } finally {
      setLoadingRequests(false)
    }
  }

  useEffect(() => { refreshRequests() }, [])

  function currentValue(key) {
    if (key === 'nationality_id') return member.nationality_id || member.nationality?.id || ''
    return member[key] || ''
  }

  async function submit() {
    const changes = {}
    for (const f of FIELDS) {
      const before = String(currentValue(f.key) ?? '')
      const after = String(form[f.key] ?? '')
      if (before !== after) {
        changes[f.key] = f.key === 'nationality_id' ? (after === '' ? null : Number(after)) : (after === '' ? null : after)
      }
    }
    if (Object.keys(changes).length === 0) {
      const warnMsg = 'Change at least one field before submitting.'
      setError(warnMsg)
      toast.warning(warnMsg)
      return
    }
    setSaving(true); setError(''); setSuccess('')
    try {
      await membersApi.submitCorrectionRequest({ requested_changes: changes, reason: reason.trim() || null })
      const succMsg = 'Correction request submitted — a church administrator will review it.'
      setSuccess(succMsg)
      toast.success(succMsg)
      setReason('')
      await refreshRequests()
    } catch (e) {
      const errMsg = e.message || 'Failed to submit correction request'
      setError(errMsg)
      toast.error(errMsg)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div>
      <div className='portal-section-title'>
        <span className='portal-section-icon'><Edit3 size={15} /></span>
        Request corrections
      </div>
      <div className='portal-card card--accent'>
        <p className='small' style={{ marginBottom: 12 }}>
          Edit any of the fields below, then submit — changes are reviewed by a church administrator before they take effect.
        </p>
        {error && <div className='mb8'><Alert level='error' title='Could not submit'>{error}</Alert></div>}
        {success && <div className='mb8'><Alert level='success' title='Submitted'>{success}</Alert></div>}

        <div className='form-grid' style={{ gap: 12 }}>
          {FIELDS.map(f => (
            <div key={f.key}>
              <label className='small' style={{ display: 'block', marginBottom: 4 }}>{f.label}</label>
              {f.type === 'nationality' ? (
                <select className='form-select' value={form.nationality_id} onChange={e => setForm(p => ({ ...p, nationality_id: e.target.value }))}>
                  <option value=''>-- none --</option>
                  {nationalities.map(n => <option key={n.id} value={n.id}>{n.name}</option>)}
                </select>
              ) : f.type === 'textarea' ? (
                <textarea className='form-control' rows={3} value={form[f.key]} onChange={e => setForm(p => ({ ...p, [f.key]: e.target.value }))} />
              ) : (
                <input className='form-control' type={f.type} value={form[f.key]} onChange={e => setForm(p => ({ ...p, [f.key]: e.target.value }))} />
              )}
            </div>
          ))}
        </div>

        <div className='mt8'>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Reason (optional)</label>
          <textarea className='form-control' rows={2} value={reason} onChange={e => setReason(e.target.value)} placeholder='Why are you requesting this change?' />
        </div>

        <button type='button' className='btn btn-primary mt8' disabled={saving} onClick={submit}>{saving ? 'Submitting…' : 'Submit correction request'}</button>
      </div>

      <div className='portal-section-title' style={{ fontSize: 16 }}>
        <span className='portal-section-icon'><Clock size={14} /></span>
        My past requests
      </div>
      <div className='portal-card'>
        {loadingRequests && <p className='small'>Loading…</p>}
        {!loadingRequests && requests.length === 0 && <p className='hint'>No correction requests submitted yet.</p>}
        {!loadingRequests && requests.length > 0 && (
          <ul className='small' style={{ margin: 0, paddingLeft: 0, listStyle: 'none' }}>
            {requests.map(r => (
              <li key={r.id} className='form-row' style={{ justifyContent: 'space-between', padding: '8px 0', borderBottom: '1px dashed var(--border)' }}>
                <span>
                  {Object.keys(r.requested_changes || {}).join(', ')} — {new Date(r.created_at).toLocaleDateString()}
                  {r.status === 'rejected' && r.review_note && <span className='hint'> ({r.review_note})</span>}
                </span>
                <Badge level={STATUS_LEVEL[r.status] || 'secondary'}>{r.status}</Badge>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}
