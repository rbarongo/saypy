import React, { useEffect, useMemo, useState } from 'react'
import { CreditCard, Printer } from 'react-feather'
import Alert from '../Alert'
import Badge from '../Badge'
import DataTable from '../members-core/DataTable'
import CollectionReceiptView from '../members-core/collections/CollectionReceiptView'
import { membersApi } from '../members-core/api'

const STATUS_LEVEL = { pending: 'warning', approved: 'success', rejected: 'danger' }

export default function ContributionsTab() {
  const [collections, setCollections] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [receiptId, setReceiptId] = useState(null)

  useEffect(() => {
    membersApi.getMyCollections({ page_size: 200 })
      .then(res => setCollections(res.items || []))
      .catch(e => setError(e.message || 'Failed to load your contributions'))
      .finally(() => setLoading(false))
  }, [])

  const stats = useMemo(() => {
    const approved = collections.filter(c => c.status === 'approved')
    const total = approved.reduce((s, c) => s + Number(c.total_amount || 0), 0)
    const thisYear = approved.filter(c => new Date(c.collection_date).getFullYear() === new Date().getFullYear())
    const yearTotal = thisYear.reduce((s, c) => s + Number(c.total_amount || 0), 0)
    return { total, count: approved.length, yearTotal }
  }, [collections])

  const columns = [
    { key: 'collection_date', label: 'Date' },
    { key: 'items', label: 'Items', sortable: false, render: c => (c.items || []).map(i => i.collection_type.name).join(', ') },
    { key: 'total_amount', label: 'Total', render: c => Number(c.total_amount).toFixed(2) },
    { key: 'status', label: 'Status', render: c => <Badge level={STATUS_LEVEL[c.status] || 'secondary'}>{c.status}</Badge> },
  ]

  return (
    <div>
      <div className='portal-section-title'>
        <span className='portal-section-icon'><CreditCard size={16} /></span>
        My Contributions
      </div>

      <div className='portal-stat-row'>
        <div className='portal-stat'>
          <span className='portal-stat-label'>Total given (all time)</span>
          <span className='portal-stat-value'>{stats.total.toFixed(2)}</span>
        </div>
        <div className='portal-stat'>
          <span className='portal-stat-label'>This year</span>
          <span className='portal-stat-value'>{stats.yearTotal.toFixed(2)}</span>
        </div>
        <div className='portal-stat'>
          <span className='portal-stat-label'>Approved records</span>
          <span className='portal-stat-value'>{stats.count}</span>
        </div>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <div className='portal-card'>
        <DataTable
          columns={columns}
          rows={collections}
          loading={loading}
          emptyMessage='No contributions recorded yet.'
          actions={c => c.status === 'approved' && c.receipt_number ? (
            <button type='button' className='btn btn-sm btn-ghost' onClick={() => setReceiptId(c.id)}>
              <Printer size={14} /> Receipt
            </button>
          ) : null}
        />
      </div>
      {receiptId && <CollectionReceiptView collectionId={receiptId} onClose={() => setReceiptId(null)} />}
    </div>
  )
}
