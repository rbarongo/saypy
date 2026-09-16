import React, { useEffect, useState } from 'react'
import Alert from '../../Alert'
import Modal from '../../Modal'
import { membersApi } from '../api'

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

function formatValue(v) {
  if (v === null || v === undefined) return null
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

/** Fetches and displays the append-only audit trail for one collection —
 * same fetch-and-display shape as CollectionReceiptView.jsx. No edit/delete
 * action exists anywhere for these rows; that's what makes them tamper-evident. */
export default function CollectionAuditTrailView({ collectionId, onClose }) {
  const [entries, setEntries] = useState(null)
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false
    membersApi.getCollectionAuditLog(collectionId)
      .then(rows => { if (!cancelled) setEntries(rows) })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load audit trail') })
    return () => { cancelled = true }
  }, [collectionId])

  return (
    <Modal title='Audit trail' onClose={onClose} maxWidth={640}>
      {error && <Alert level='error' title='Could not load audit trail'>{error}</Alert>}
      {!error && !entries && <p className='small'>Loading…</p>}
      {entries && entries.length === 0 && <p className='hint'>No audit entries recorded yet.</p>}
      {entries && entries.length > 0 && (
        <ul className='small' style={{ margin: 0, paddingLeft: 0, listStyle: 'none' }}>
          {entries.map(e => (
            <li key={e.id} style={{ padding: '8px 0', borderBottom: '1px solid var(--border)' }}>
              <div className='flex gap8' style={{ justifyContent: 'space-between' }}>
                <strong>{ACTION_LABELS[e.action] || e.action}</strong>
                <span className='hint'>{new Date(e.created_at).toLocaleString()}</span>
              </div>
              <div className='hint'>By {e.actor_username || 'system'}{e.source ? ` · via ${e.source}` : ''}</div>
              {e.reason && <div>{e.reason}</div>}
              {(e.before_value || e.after_value) && (
                <div className='hint'>
                  {e.before_value && <span>Before: {formatValue(e.before_value)} </span>}
                  {e.after_value && <span>After: {formatValue(e.after_value)}</span>}
                </div>
              )}
            </li>
          ))}
        </ul>
      )}
      <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
        <button type='button' className='btn btn-ghost' onClick={onClose}>Close</button>
      </div>
    </Modal>
  )
}
