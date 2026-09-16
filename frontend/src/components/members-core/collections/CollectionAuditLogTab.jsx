import React, { useEffect, useState } from 'react'
import { Shield } from 'react-feather'
import Alert from '../../Alert'
import DataTable from '../DataTable'
import PageHeader from '../PageHeader'
import { membersApi } from '../api'
import useMyPermissions from '../../../hooks/useMyPermissions'

const FETCH_SIZE = 200

const ACTION_LABELS = {
  collection_created: 'Created',
  collection_approved: 'Approved',
  collection_rejected: 'Rejected',
  correction_requested: 'Correction requested',
  correction_approved: 'Correction approved',
  correction_rejected: 'Correction rejected',
  group_approved_all: 'Group approved (bulk)',
  receipt_issued: 'Receipt issued',
  receipt_reprinted: 'Receipt reprinted',
  receipt_cancelled: 'Receipt cancelled',
  receipt_corrected: 'Receipt corrected',
  collection_verified: 'Payment verified',
  collection_flagged: 'Payment flagged',
  reconciliation_matched: 'Reconciliation matched',
  period_closed: 'Financial period closed',
  period_reopened: 'Financial period reopened',
}

/** Cross-collection auditor view — read-only, church-scoped, no write
 * endpoint exists anywhere for this data (see collection_audit.py). */
export default function CollectionAuditLogTab() {
  const { can } = useMyPermissions()
  const [churches, setChurches] = useState([])
  const [churchFilter, setChurchFilter] = useState('')
  const [actionFilter, setActionFilter] = useState('')
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const churchById = new Map(churches.map(c => [c.id, c]))
  const canView = can('view_treasury_audit')

  useEffect(() => { membersApi.listChurches().then(setChurches).catch(() => {}) }, [])

  async function refresh() {
    setLoading(true); setError('')
    try {
      const res = await membersApi.getCollectionAuditLogs({ church_id: churchFilter, action: actionFilter, page_size: FETCH_SIZE })
      setItems(res.items || [])
    } catch (e) {
      setError(e.message || 'Failed to load audit log')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { if (canView) refresh() }, [churchFilter, actionFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  const columns = [
    { key: 'created_at', label: 'When', render: e => new Date(e.created_at).toLocaleString() },
    { key: 'church', label: 'Church', sortable: false, render: e => churchById.get(e.church_id)?.name || e.church_id },
    { key: 'action', label: 'Action', render: e => ACTION_LABELS[e.action] || e.action },
    { key: 'actor_username', label: 'By', render: e => e.actor_username || 'system' },
    { key: 'source', label: 'Source', render: e => e.source || '—' },
    { key: 'reason', label: 'Reason', render: e => e.reason || '—' },
    { key: 'collection_id', label: 'Collection', render: e => e.collection_id ?? '—' },
    { key: 'receipt_id', label: 'Receipt', render: e => e.receipt_id ?? '—' },
  ]

  if (!canView) {
    return (
      <div>
        <PageHeader title='Collections' subtitle='Audit Log' icon={Shield} />
        <Alert level='error' title='Access restricted'>You don't have permission to view the treasury audit log.</Alert>
      </div>
    )
  }

  return (
    <div>
      <PageHeader title='Collections' subtitle='Audit Log' />

      <div className='toolbar mb-3'>
        <select className='form-select' style={{ maxWidth: 220 }} value={churchFilter} onChange={e => { setChurchFilter(e.target.value); setPage(1) }}>
          <option value=''>-- all churches --</option>
          {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
        <select className='form-select' style={{ maxWidth: 260 }} value={actionFilter} onChange={e => { setActionFilter(e.target.value); setPage(1) }}>
          <option value=''>-- all actions --</option>
          {Object.entries(ACTION_LABELS).map(([code, label]) => <option key={code} value={code}>{label}</option>)}
        </select>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <DataTable
        columns={columns}
        rows={items}
        loading={loading}
        emptyMessage='No audit entries recorded yet.'
        pageSize={20}
      />
    </div>
  )
}
