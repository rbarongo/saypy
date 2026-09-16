import React, { useEffect, useState } from 'react'
import Alert from '../../Alert'
import Modal from '../../Modal'
import { membersApi } from '../api'
import { ACTION_LABELS } from '../actionLabels'

/** "View record history where authorised" — a member's MemberAuditLog rows. */
export default function MemberHistoryModal({ member, onClose }) {
  const [history, setHistory] = useState(null)
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false
    membersApi.getMemberHistory(member.id)
      .then(rows => { if (!cancelled) setHistory(rows) })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load history') })
    return () => { cancelled = true }
  }, [member.id])

  return (
    <Modal title={`History — ${member.first_name} ${member.last_name}`} onClose={onClose} maxWidth={560}>
      {error && <Alert level='error' title='Could not load history'>{error}</Alert>}
      {!error && !history && <p className='small'>Loading…</p>}
      {history && (
        <div className='table-wrap'>
          <table>
            <thead><tr><th>When</th><th>Action</th><th>By</th><th>Details</th></tr></thead>
            <tbody>
              {history.length === 0 && <tr><td colSpan={4} className='small'>No history recorded yet.</td></tr>}
              {history.map(row => (
                <tr key={row.id}>
                  <td>{new Date(row.created_at).toLocaleString()}</td>
                  <td>{ACTION_LABELS[row.action] || row.action}</td>
                  <td>{row.actor_username || '—'}</td>
                  <td>{row.details || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
        <button type='button' className='btn btn-ghost' onClick={onClose}>Close</button>
      </div>
    </Modal>
  )
}
