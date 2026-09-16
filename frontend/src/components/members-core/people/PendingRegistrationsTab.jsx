import React, { useEffect, useState } from 'react'
import { Check, Copy, CheckCircle, Clock as ClockIcon, X } from 'react-feather'
import Alert from '../../Alert'
import Modal from '../../Modal'
import PageHeader from '../PageHeader'
import DataTable from '../DataTable'
import { fullName } from '../constants'
import { membersApi } from '../api'

/**
 * Review queue for assisted registrations submitted with "Submit for
 * review" (MemberForm's submit_for_review switch) — modeled on the
 * collections module's CollectionGroupsTab review-queue pattern.
 */
export default function PendingRegistrationsTab() {
  const [churches, setChurches] = useState([])
  const [churchFilter, setChurchFilter] = useState('')
  const [members, setMembers] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [busyId, setBusyId] = useState(null)
  const [rejectingId, setRejectingId] = useState(null)
  const [rejectReason, setRejectReason] = useState('')
  const [approvingMember, setApprovingMember] = useState(null)
  const [approvePassword, setApprovePassword] = useState('')
  const [copiedField, setCopiedField] = useState('')
  // Set from the approve-registration response right after it happens — the
  // plaintext password only ever exists here, shown once, then discarded.
  const [createdAccount, setCreatedAccount] = useState(null)

  const churchById = new Map(churches.map(c => [c.id, c]))

  useEffect(() => { membersApi.listChurches().then(setChurches).catch(() => {}) }, [])

  async function refresh() {
    setLoading(true); setError('')
    try {
      const res = await membersApi.listPendingRegistrations({ church_id: churchFilter, page_size: 200 })
      setMembers(res.items || [])
    } catch (e) {
      setError(e.message || 'Failed to load pending registrations')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [churchFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  async function submitApprove() {
    if (approvePassword && approvePassword.length < 6) { setError('Password must be at least 6 characters, or left blank to auto-generate one.'); return }
    setBusyId(approvingMember.id); setError('')
    try {
      const result = await membersApi.approveMemberRegistration(approvingMember.id, approvePassword.trim() || undefined)
      setApprovingMember(null); setApprovePassword('')
      if (result.created_username) {
        setCreatedAccount({ username: result.created_username, password: result.created_password })
      }
      await refresh()
    } catch (e) {
      setError(e.message || 'Approve failed')
    } finally {
      setBusyId(null)
    }
  }

  async function copyToClipboard(text, field) {
    try {
      await navigator.clipboard.writeText(text)
      setCopiedField(field)
      setTimeout(() => setCopiedField(f => (f === field ? '' : f)), 1500)
    } catch (e) { /* clipboard unavailable — the credentials are still visible to copy by hand */ }
  }

  async function submitReject() {
    if (!rejectReason.trim()) { setError('A reason is required to reject a registration'); return }
    setBusyId(rejectingId)
    try {
      await membersApi.rejectMemberRegistration(rejectingId, rejectReason.trim())
      setRejectingId(null); setRejectReason('')
      await refresh()
    } catch (e) {
      setError(e.message || 'Reject failed')
    } finally {
      setBusyId(null)
    }
  }

  const columns = [
    { key: 'name', label: 'Name', render: fullName, sortValue: fullName },
    { key: 'church', label: 'Church', sortable: false, render: m => churchById.get(m.church_id)?.name || m.church_id },
    { key: 'member_number', label: 'Member #' },
    { key: 'created_at', label: 'Submitted', render: m => new Date(m.created_at).toLocaleString() },
  ]

  return (
    <div>
      <PageHeader title='Member Management' subtitle='Pending Registrations' icon={ClockIcon} />

      <div className='toolbar mb-3'>
        <select className='form-select' style={{ maxWidth: 220 }} value={churchFilter} onChange={e => setChurchFilter(e.target.value)}>
          <option value=''>-- all churches --</option>
          {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <DataTable
        columns={columns}
        rows={members}
        loading={loading}
        emptyMessage='No pending registrations.'
        actions={m => (
          <div className='flex gap8'>
            <button type='button' className='btn btn-sm btn-primary' disabled={busyId === m.id} onClick={() => { setApprovingMember(m); setApprovePassword(''); setError('') }}>
              <Check size={14} /> Approve
            </button>
            <button type='button' className='btn btn-sm btn-danger' disabled={busyId === m.id} onClick={() => { setRejectingId(m.id); setRejectReason('') }}>
              <X size={14} /> Reject
            </button>
          </div>
        )}
      />

      {rejectingId && (
        <Modal title='Reject registration' onClose={() => setRejectingId(null)} maxWidth={420}>
          <p className='small mb8'>State the reason this registration is being rejected.</p>
          <textarea className='form-control' rows={3} value={rejectReason} onChange={e => setRejectReason(e.target.value)} placeholder='Reason *' />
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={() => setRejectingId(null)}>Cancel</button>
            <button type='button' className='btn btn-danger' disabled={busyId === rejectingId} onClick={submitReject}>
              {busyId === rejectingId ? 'Rejecting…' : 'Reject'}
            </button>
          </div>
        </Modal>
      )}

      {approvingMember && (
        <Modal title={`Approve ${fullName(approvingMember)}`} onClose={() => setApprovingMember(null)} maxWidth={420}>
          <p className='small mb8'>
            Approving creates their login account with the default <strong>Member</strong> role. Set a password for
            them now, or leave it blank to auto-generate a temporary one.
          </p>
          {error && <div className='mb8'><Alert level='error' title='Could not approve'>{error}</Alert></div>}
          <input
            type='text'
            className='form-control'
            value={approvePassword}
            onChange={e => setApprovePassword(e.target.value)}
            placeholder='Password (optional — min 6 characters)'
            autoComplete='new-password'
          />
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={() => setApprovingMember(null)}>Cancel</button>
            <button type='button' className='btn btn-primary' disabled={busyId === approvingMember.id} onClick={submitApprove}>
              {busyId === approvingMember.id ? 'Approving…' : 'Approve'}
            </button>
          </div>
        </Modal>
      )}

      {createdAccount && (
        <Modal title='Member registered' onClose={() => setCreatedAccount(null)} maxWidth={420}>
          <div className='flex gap8 mb8' style={{ alignItems: 'center' }}>
            <CheckCircle size={20} className='text-success' />
            <h5 className='card-title mb-0'>Account created</h5>
          </div>
          <p className='small mb8'>
            Share these credentials securely — the password is shown only this once and can't be recovered later, only reset.
          </p>
          <div className='kv' style={{ gap: 10 }}>
            <div className='field-chip' style={{ justifyContent: 'space-between' }}>
              <span><strong>Username:</strong> {createdAccount.username}</span>
              <button type='button' className='btn btn-sm btn-ghost' onClick={() => copyToClipboard(createdAccount.username, 'username')}>
                {copiedField === 'username' ? <><Check size={14} /> Copied</> : <><Copy size={14} /> Copy</>}
              </button>
            </div>
            <div className='field-chip' style={{ justifyContent: 'space-between' }}>
              <span><strong>Password:</strong> {createdAccount.password}</span>
              <button type='button' className='btn btn-sm btn-ghost' onClick={() => copyToClipboard(createdAccount.password, 'password')}>
                {copiedField === 'password' ? <><Check size={14} /> Copied</> : <><Copy size={14} /> Copy</>}
              </button>
            </div>
          </div>
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-primary' onClick={() => setCreatedAccount(null)}>Done</button>
          </div>
        </Modal>
      )}
    </div>
  )
}
