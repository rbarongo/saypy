import React, { useEffect, useState } from 'react'
import { Check, Edit3, X } from 'react-feather'
import Alert from '../../Alert'
import Modal from '../../Modal'
import PageHeader from '../PageHeader'
import DataTable from '../DataTable'
import { membersApi } from '../api'

/**
 * Staff review queue for member self-service correction requests
 * (POST /members/me/correction-requests) — modeled directly on
 * PendingRegistrationsTab's approve/reject pattern. Approving applies only
 * the allow-listed fields server-side (member_service.apply_correction_request);
 * this view just shows what was requested and lets staff decide.
 */
export default function CorrectionRequestsTab() {
  const [statusFilter, setStatusFilter] = useState('pending')
  const [requests, setRequests] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [busyId, setBusyId] = useState(null)
  const [rejectingId, setRejectingId] = useState(null)
  const [rejectReason, setRejectReason] = useState('')

  async function refresh() {
    setLoading(true); setError('')
    try {
      const list = await membersApi.listCorrectionRequests({ status: statusFilter || undefined })
      setRequests(list || [])
    } catch (e) {
      setError(e.message || 'Failed to load correction requests')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [statusFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  async function approve(r) {
    setBusyId(r.id); setError('')
    try {
      await membersApi.approveCorrectionRequest(r.id)
      await refresh()
    } catch (e) {
      setError(e.message || 'Approve failed')
    } finally {
      setBusyId(null)
    }
  }

  async function submitReject() {
    if (!rejectReason.trim()) { setError('A reason is required to reject a correction request'); return }
    setBusyId(rejectingId)
    try {
      await membersApi.rejectCorrectionRequest(rejectingId, rejectReason.trim())
      setRejectingId(null); setRejectReason('')
      await refresh()
    } catch (e) {
      setError(e.message || 'Reject failed')
    } finally {
      setBusyId(null)
    }
  }

  const columns = [
    { key: 'member_name', label: 'Member' },
    { key: 'requested_changes', label: 'Requested changes', sortable: false, render: r => Object.entries(r.requested_changes || {}).map(([k, v]) => `${k}: ${v ?? '(cleared)'}`).join('; ') },
    { key: 'reason', label: 'Reason', render: r => r.reason || '—' },
    { key: 'created_at', label: 'Submitted', render: r => new Date(r.created_at).toLocaleString() },
    { key: 'status', label: 'Status' },
  ]

  return (
    <div>
      <PageHeader title='Member Management' subtitle='Correction Requests' icon={Edit3} />

      <div className='toolbar mb-3'>
        <select className='form-select' style={{ maxWidth: 220 }} value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
          <option value=''>-- all statuses --</option>
          <option value='pending'>Pending</option>
          <option value='approved'>Approved</option>
          <option value='rejected'>Rejected</option>
        </select>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <DataTable
        columns={columns}
        rows={requests}
        loading={loading}
        emptyMessage='No correction requests.'
        actions={r => r.status === 'pending' ? (
          <div className='flex gap8'>
            <button type='button' className='btn btn-sm btn-primary' disabled={busyId === r.id} onClick={() => approve(r)}>
              <Check size={14} /> Approve
            </button>
            <button type='button' className='btn btn-sm btn-danger' disabled={busyId === r.id} onClick={() => { setRejectingId(r.id); setRejectReason('') }}>
              <X size={14} /> Reject
            </button>
          </div>
        ) : null}
      />

      {rejectingId && (
        <Modal title='Reject correction request' onClose={() => setRejectingId(null)} maxWidth={420}>
          <p className='small mb8'>State the reason this correction request is being rejected.</p>
          <textarea className='form-control' rows={3} value={rejectReason} onChange={e => setRejectReason(e.target.value)} placeholder='Reason *' />
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={() => setRejectingId(null)}>Cancel</button>
            <button type='button' className='btn btn-danger' disabled={busyId === rejectingId} onClick={submitReject}>
              {busyId === rejectingId ? 'Rejecting…' : 'Reject'}
            </button>
          </div>
        </Modal>
      )}
    </div>
  )
}
