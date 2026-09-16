import React, { useEffect, useState } from 'react'
import { Check, Cpu, X } from 'react-feather'
import Alert from '../../Alert'
import Badge from '../../Badge'
import Modal from '../../Modal'
import PageHeader from '../PageHeader'
import DataTable from '../DataTable'
import { membersApi } from '../api'
import useMyPermissions from '../../../hooks/useMyPermissions'

function collectionStatusLevel(status) {
  if (status === 'approved') return 'success'
  if (status === 'rejected') return 'danger'
  return 'warning'
}

function payerLabel(c) {
  return c.member_name || c.agent_payload?.payer || c.agent_payload?.name || '— unmatched —'
}

/**
 * Review queue for Collections submitted by the Digital Treasurer
 * receipt-reading agent (entry_method=agent, see POST /collections/agent-submit).
 * Each submission is its own standalone Collection (not a batch/group like
 * manual or import entries), so this reviews one at a time. Approve/Reject
 * are the exact same actions as any other collection — same audit trail,
 * same receipt issuance on approve — this view just surfaces the agent's
 * own assessment (agent_payload) prominently for the reviewer.
 */
export default function AgentSubmissionsTab() {
  const { can } = useMyPermissions()
  const [churches, setChurches] = useState([])
  const [churchFilter, setChurchFilter] = useState('')
  const [statusFilter, setStatusFilter] = useState('pending')
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const [reviewing, setReviewing] = useState(null)
  const [busy, setBusy] = useState(false)
  const [rejecting, setRejecting] = useState(false)
  const [rejectReason, setRejectReason] = useState('')
  const [detailError, setDetailError] = useState('')

  const churchById = new Map(churches.map(c => [c.id, c]))

  useEffect(() => { membersApi.listChurches().then(setChurches).catch(() => {}) }, [])

  async function refresh() {
    setLoading(true); setError('')
    try {
      const res = await membersApi.listCollections({ entry_method: 'agent', church_id: churchFilter, status: statusFilter, page_size: 100 })
      setItems(res.items || [])
    } catch (e) {
      setError(e.message || 'Failed to load agent submissions')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [churchFilter, statusFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  async function openReview(collection) {
    setDetailError('')
    try {
      setReviewing(await membersApi.getCollection(collection.id))
    } catch (e) {
      setReviewing(collection)
    }
  }

  async function approve() {
    if (!reviewing) return
    setBusy(true); setDetailError('')
    try {
      await membersApi.approveCollection(reviewing.id)
      setReviewing(null)
      await refresh()
    } catch (e) {
      setDetailError(e.message || 'Approve failed')
    } finally {
      setBusy(false)
    }
  }

  async function submitReject() {
    if (!rejectReason.trim()) { setDetailError('A reason is required to reject a submission'); return }
    setBusy(true); setDetailError('')
    try {
      await membersApi.rejectCollection(reviewing.id, rejectReason.trim())
      setRejecting(false); setRejectReason(''); setReviewing(null)
      await refresh()
    } catch (e) {
      setDetailError(e.message || 'Reject failed')
    } finally {
      setBusy(false)
    }
  }

  const columns = [
    { key: 'payer', label: 'Payer', sortable: false, render: payerLabel },
    { key: 'church', label: 'Church', sortable: false, render: c => churchById.get(c.church_id)?.name || c.church_id },
    { key: 'collection_date', label: 'Date' },
    { key: 'total_amount', label: 'Amount', render: c => Number(c.total_amount).toFixed(2) },
    { key: 'collection_source', label: 'Channel' },
    {
      key: 'agent_valid', label: 'Agent check', sortable: false, render: c => (
        c.agent_payload?.valid ? <Badge level='success'>OK</Badge> : <Badge level='danger'>Flagged</Badge>
      ),
    },
    { key: 'status', label: 'Status', sortable: false, render: c => <Badge level={collectionStatusLevel(c.status)}>{c.status}</Badge> },
  ]

  return (
    <div>
      <PageHeader title='Collections' subtitle='Digital Receipts (Agent Submissions)' icon={Cpu} />

      <div className='toolbar mb-3'>
        <select className='form-select' style={{ maxWidth: 220 }} value={churchFilter} onChange={e => setChurchFilter(e.target.value)}>
          <option value=''>-- all churches --</option>
          {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
        <select className='form-select' style={{ maxWidth: 220 }} value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
          <option value=''>-- all statuses --</option>
          <option value='pending'>Pending review</option>
          <option value='approved'>Approved</option>
          <option value='rejected'>Rejected</option>
        </select>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <DataTable
        columns={columns}
        rows={items}
        loading={loading}
        emptyMessage='No agent-submitted receipts yet.'
        actions={c => <button type='button' className='btn btn-sm btn-light' onClick={() => openReview(c)}>Review</button>}
      />

      {reviewing && (
        <Modal title='Review agent submission' onClose={() => setReviewing(null)} maxWidth={640}>
          {detailError && <div className='mb8'><Alert level='error' title='Error'>{detailError}</Alert></div>}

          <div className='kv' style={{ gap: 10 }}>
            <div className='flex gap8' style={{ justifyContent: 'space-between', flexWrap: 'wrap', alignItems: 'center' }}>
              <div>
                <strong>{payerLabel(reviewing)}</strong>
                {!reviewing.member_id && !reviewing.is_anonymous && <span className='hint' style={{ marginLeft: 6 }}>(not linked to a member record)</span>}
              </div>
              <Badge level={collectionStatusLevel(reviewing.status)}>{reviewing.status}</Badge>
            </div>

            {reviewing.agent_payload?.summary && (
              <p className='small' style={{ fontStyle: 'italic' }}>{reviewing.agent_payload.summary}</p>
            )}

            {(reviewing.agent_payload?.validation_issues || []).length > 0 && (
              <Alert level='warning' title='Agent flagged issues'>
                <ul style={{ margin: 0, paddingLeft: 18 }}>
                  {reviewing.agent_payload.validation_issues.map((issue, i) => <li key={i}>{issue}</li>)}
                </ul>
              </Alert>
            )}

            <div className='form-row' style={{ flexWrap: 'wrap', gap: 16 }}>
              <span className='field-chip'>Agent valid: {reviewing.agent_payload?.valid ? <Badge level='success'>Yes</Badge> : <Badge level='danger'>No</Badge>}</span>
              <span className='field-chip'>Description matches: {reviewing.agent_payload?.description_matches_receipt ? <Badge level='success'>Yes</Badge> : <Badge level='danger'>No</Badge>}</span>
              <span className='field-chip'>Reconciled: {reviewing.agent_payload?.reconciliation ? <Badge level='success'>Yes</Badge> : <Badge level='secondary'>No</Badge>}</span>
            </div>

            <div className='table-wrap'>
              <table>
                <thead><tr><th>Item</th><th className='text-right'>Amount</th></tr></thead>
                <tbody>
                  {(reviewing.items || []).map(i => (
                    <tr key={i.id}><td>{i.collection_type.name}</td><td className='text-right'>{Number(i.amount).toFixed(2)}</td></tr>
                  ))}
                  <tr className='row-total'><td>Total (recorded)</td><td className='text-right'>{Number(reviewing.total_amount).toFixed(2)}</td></tr>
                </tbody>
              </table>
            </div>

            <div className='kv' style={{ gap: 4 }}>
              <span className='small'><strong>Payment channel:</strong> {reviewing.collection_source}</span>
              <span className='small'><strong>Reference:</strong> {reviewing.payment_reference || '—'}</span>
              <span className='small'><strong>Date:</strong> {reviewing.collection_date}</span>
              <span className='small'><strong>Submitted via:</strong> {reviewing.agent_payload?.submission_channel || '—'}</span>
              <span className='small'><strong>Claimed payee:</strong> {reviewing.agent_payload?.payee || '—'}</span>
              <span className='small'><strong>Receipt # on document:</strong> {reviewing.agent_payload?.receipt_number || '—'}</span>
            </div>

            {reviewing.status === 'pending' && can('review_collections', reviewing.church_id) && (
              <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
                <button type='button' className='btn btn-danger' disabled={busy} onClick={() => { setRejecting(true); setRejectReason('') }}>
                  <X size={14} /> Reject
                </button>
                <button type='button' className='btn btn-primary' disabled={busy} onClick={approve}>
                  <Check size={14} /> Approve
                </button>
              </div>
            )}
          </div>
        </Modal>
      )}

      {rejecting && (
        <Modal title='Reject submission' onClose={() => setRejecting(false)} maxWidth={420}>
          <p className='small mb8'>State the reason this agent-submitted receipt is being rejected.</p>
          <textarea className='form-control' rows={3} value={rejectReason} onChange={e => setRejectReason(e.target.value)} placeholder='Reason *' />
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={() => setRejecting(false)}>Cancel</button>
            <button type='button' className='btn btn-danger' disabled={busy} onClick={submitReject}>
              {busy ? 'Rejecting…' : 'Reject'}
            </button>
          </div>
        </Modal>
      )}
    </div>
  )
}
