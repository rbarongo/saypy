import React, { useEffect, useState } from 'react'
import { Check, CheckSquare, X } from 'react-feather'
import Alert from '../../Alert'
import Badge from '../../Badge'
import Modal from '../../Modal'
import PageHeader from '../PageHeader'
import DataTable from '../DataTable'
import { membersApi } from '../api'
import CollectionReceiptView from './CollectionReceiptView'
import useMyPermissions from '../../../hooks/useMyPermissions'

function groupStatusLevel(status) { return status === 'reviewed' ? 'success' : 'warning' }
function collectionStatusLevel(status) {
  if (status === 'approved') return 'success'
  if (status === 'rejected') return 'danger'
  return 'warning'
}

/**
 * Review queue: a different person than the initiator opens a group, then
 * approves or rejects each collection inside it (or approves everything
 * still pending in one click). Approval issues a receipt server-side.
 */
export default function CollectionGroupsTab() {
  const { can } = useMyPermissions()
  const [churches, setChurches] = useState([])
  const [churchFilter, setChurchFilter] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [groups, setGroups] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const [openGroupId, setOpenGroupId] = useState(null)
  const [groupDetail, setGroupDetail] = useState(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState('')
  const [busyCollectionId, setBusyCollectionId] = useState(null)
  const [rejectingId, setRejectingId] = useState(null)
  const [rejectReason, setRejectReason] = useState('')
  const [receiptForCollectionId, setReceiptForCollectionId] = useState(null)

  const churchById = new Map(churches.map(c => [c.id, c]))

  useEffect(() => { membersApi.listChurches().then(setChurches).catch(() => {}) }, [])

  async function refresh() {
    setLoading(true); setError('')
    try {
      setGroups(await membersApi.listCollectionGroups({ church_id: churchFilter, status: statusFilter }))
    } catch (e) {
      setError(e.message || 'Failed to load collection groups')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [churchFilter, statusFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  async function openGroup(id) {
    setOpenGroupId(id); setGroupDetail(null); setDetailLoading(true); setDetailError('')
    try {
      setGroupDetail(await membersApi.getCollectionGroup(id))
    } catch (e) {
      setDetailError(e.message || 'Failed to load group')
    } finally {
      setDetailLoading(false)
    }
  }

  async function reloadDetail() {
    if (!openGroupId) return
    try { setGroupDetail(await membersApi.getCollectionGroup(openGroupId)) } catch (e) { /* keep previous */ }
    await refresh()
  }

  async function approve(collectionId) {
    setBusyCollectionId(collectionId)
    try {
      await membersApi.approveCollection(collectionId)
      await reloadDetail()
    } catch (e) {
      setDetailError(e.message || 'Approve failed')
    } finally {
      setBusyCollectionId(null)
    }
  }

  async function submitReject() {
    if (!rejectReason.trim()) { setDetailError('A reason is required to reject a collection'); return }
    setBusyCollectionId(rejectingId)
    try {
      await membersApi.rejectCollection(rejectingId, rejectReason.trim())
      setRejectingId(null); setRejectReason('')
      await reloadDetail()
    } catch (e) {
      setDetailError(e.message || 'Reject failed')
    } finally {
      setBusyCollectionId(null)
    }
  }

  async function approveAll() {
    if (!openGroupId) return
    setBusyCollectionId('__all__')
    try {
      await membersApi.approveAllPendingInGroup(openGroupId)
      await reloadDetail()
    } catch (e) {
      setDetailError(e.message || 'Bulk approve failed')
    } finally {
      setBusyCollectionId(null)
    }
  }

  const columns = [
    { key: 'code', label: 'Group' },
    { key: 'name', label: 'Name', render: g => g.name || '—' },
    { key: 'church', label: 'Church', sortable: false, render: g => churchById.get(g.church_id)?.name || g.church_id },
    { key: 'initiator_username', label: 'Initiator' },
    { key: 'initiated_at', label: 'Initiated', render: g => new Date(g.initiated_at).toLocaleString() },
    { key: 'collection_count', label: 'Collections' },
    { key: 'total_amount', label: 'Total', render: g => Number(g.total_amount).toFixed(2) },
    { key: 'status', label: 'Status', sortable: false, render: g => <Badge level={groupStatusLevel(g.status)}>{g.status}</Badge> },
  ]

  return (
    <div>
      <PageHeader title='Collections' subtitle='Review Groups' icon={CheckSquare} />

      <div className='toolbar mb-3'>
        <select className='form-select' style={{ maxWidth: 220 }} value={churchFilter} onChange={e => setChurchFilter(e.target.value)}>
          <option value=''>-- all churches --</option>
          {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
        <select className='form-select' style={{ maxWidth: 220 }} value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
          <option value=''>-- all statuses --</option>
          <option value='pending'>Pending review</option>
          <option value='reviewed'>Reviewed</option>
        </select>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <DataTable
        columns={columns}
        rows={groups}
        loading={loading}
        emptyMessage='No collection groups yet.'
        actions={g => <button type='button' className='btn btn-sm btn-light' onClick={() => openGroup(g.id)}>Review</button>}
      />

      {openGroupId && (
        <Modal title={groupDetail ? `${groupDetail.code}${groupDetail.name ? ' — ' + groupDetail.name : ''}` : 'Group'} onClose={() => { setOpenGroupId(null); setGroupDetail(null) }} maxWidth={860}>
          {detailError && <div className='mb8'><Alert level='error' title='Error'>{detailError}</Alert></div>}
          {detailLoading && <p className='small'>Loading…</p>}
          {groupDetail && (
            <div>
              <div className='flex gap8 mb8' style={{ justifyContent: 'space-between', alignItems: 'center' }}>
                <p className='small mb-0'>
                  Initiated by <strong>{groupDetail.initiator_username}</strong> on {new Date(groupDetail.initiated_at).toLocaleString()}
                  {' · '}
                  <Badge level={groupStatusLevel(groupDetail.status)}>{groupDetail.status}</Badge>
                </p>
                {groupDetail.status === 'pending' && can('review_collections', groupDetail.church_id) && (
                  <button type='button' className='btn btn-sm btn-primary' disabled={busyCollectionId === '__all__'} onClick={approveAll}>
                    {busyCollectionId === '__all__' ? 'Approving…' : 'Approve all pending'}
                  </button>
                )}
              </div>

              <div className='table-wrap'>
                <table>
                  <thead><tr><th>Member</th><th>Items</th><th className='text-right'>Total</th><th>Status</th><th>Actions</th></tr></thead>
                  <tbody>
                    {groupDetail.collections.length === 0 && <tr><td colSpan={5} className='small'>No collections in this group.</td></tr>}
                    {groupDetail.collections.map(c => (
                      <tr key={c.id}>
                        <td>{c.member_name}</td>
                        <td>{(c.items || []).map(i => `${i.collection_type.name}: ${Number(i.amount).toFixed(2)}`).join(', ')}</td>
                        <td className='text-right'>{Number(c.total_amount).toFixed(2)}</td>
                        <td>
                          <Badge level={collectionStatusLevel(c.status)}>{c.status}</Badge>
                          {c.status === 'rejected' && c.rejection_reason && <div className='hint'>{c.rejection_reason}</div>}
                        </td>
                        <td>
                          {c.status === 'pending' && can('review_collections', groupDetail.church_id) && (
                            <div className='flex gap8'>
                              <button type='button' className='btn btn-sm btn-primary' disabled={busyCollectionId === c.id} onClick={() => approve(c.id)}>
                                <Check size={14} /> Approve
                              </button>
                              <button type='button' className='btn btn-sm btn-danger' disabled={busyCollectionId === c.id} onClick={() => { setRejectingId(c.id); setRejectReason('') }}>
                                <X size={14} /> Reject
                              </button>
                            </div>
                          )}
                          {c.status === 'approved' && c.receipt_number && (
                            <button type='button' className='btn btn-sm btn-ghost' onClick={() => setReceiptForCollectionId(c.id)}>Receipt</button>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </Modal>
      )}

      {rejectingId && (
        <Modal title='Reject collection' onClose={() => setRejectingId(null)} maxWidth={420}>
          <p className='small mb8'>State the reason this collection is being rejected.</p>
          <textarea className='form-control' rows={3} value={rejectReason} onChange={e => setRejectReason(e.target.value)} placeholder='Reason *' />
          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-ghost' onClick={() => setRejectingId(null)}>Cancel</button>
            <button type='button' className='btn btn-danger' disabled={busyCollectionId === rejectingId} onClick={submitReject}>
              {busyCollectionId === rejectingId ? 'Rejecting…' : 'Reject'}
            </button>
          </div>
        </Modal>
      )}

      {receiptForCollectionId && (
        <CollectionReceiptView collectionId={receiptForCollectionId} onClose={() => setReceiptForCollectionId(null)} />
      )}
    </div>
  )
}
