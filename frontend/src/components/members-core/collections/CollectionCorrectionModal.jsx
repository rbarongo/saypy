import React, { useEffect, useState } from 'react'
import Alert from '../../Alert'
import Badge from '../../Badge'
import Modal from '../../Modal'
import { membersApi } from '../api'
import useMyPermissions from '../../../hooks/useMyPermissions'

const KIND_OPTIONS = [
  { value: 'amount_adjustment', label: 'Amount adjustment' },
  { value: 'reversal', label: 'Reversal (zero out)' },
]

function correctionStatusLevel(status) {
  if (status === 'approved') return 'success'
  if (status === 'rejected') return 'danger'
  return 'warning'
}

/**
 * The only path to change an already-approved collection — maker-checker
 * enforced server-side (the requester can never also decide their own
 * request); this view mirrors that by hiding the decide buttons for one's
 * own pending request rather than letting a click 403 pointlessly.
 */
export default function CollectionCorrectionModal({ collection, onClose, onSaved }) {
  const { can } = useMyPermissions()
  const currentUserId = membersApi.getStoredUser()?.id

  const [kind, setKind] = useState('amount_adjustment')
  const [reason, setReason] = useState('')
  const [items, setItems] = useState(() => (collection.items || []).map(i => ({ collection_type_id: i.collection_type_id, name: i.collection_type.name, amount: i.amount })))
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [corrections, setCorrections] = useState([])
  const [loadingList, setLoadingList] = useState(true)
  const [decidingId, setDecidingId] = useState(null)
  const [decisionNotes, setDecisionNotes] = useState('')

  async function refresh() {
    setLoadingList(true)
    try { setCorrections(await membersApi.listCollectionCorrections(collection.id)) }
    catch (e) { /* non-fatal — the request form still works even if history fails to load */ }
    finally { setLoadingList(false) }
  }

  useEffect(() => { refresh() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  function updateAmount(idx, value) {
    setItems(prev => prev.map((it, i) => i === idx ? { ...it, amount: value } : it))
  }

  const hasPending = corrections.some(c => c.status === 'pending')
  const canDecide = can('review_collections', collection.church_id)
  const canRequest = can('correct_collections', collection.church_id)

  async function submit() {
    if (!reason.trim()) { setError('A reason is required'); return }
    const proposed_items = kind === 'reversal' ? [] : items.filter(i => Number(i.amount) > 0).map(i => ({ collection_type_id: i.collection_type_id, amount: Number(i.amount) }))
    if (kind === 'amount_adjustment' && proposed_items.length === 0) { setError('At least one item must have a positive amount'); return }
    setError(''); setSaving(true)
    try {
      await membersApi.requestCollectionCorrection(collection.id, { kind, reason: reason.trim(), proposed_items })
      setReason('')
      await refresh()
      if (onSaved) onSaved()
    } catch (e) {
      setError(e.message || 'Failed to submit correction')
    } finally {
      setSaving(false)
    }
  }

  async function decide(id, approve) {
    setDecidingId(id); setError('')
    try {
      await membersApi.decideCollectionCorrection(id, approve, decisionNotes || null)
      setDecisionNotes('')
      await refresh()
      if (onSaved) onSaved()
    } catch (e) {
      setError(e.message || 'Failed to decide correction')
    } finally {
      setDecidingId(null)
    }
  }

  return (
    <Modal title={`Correct collection — ${collection.member_name}`} onClose={onClose} maxWidth={640}>
      {error && <div className='mb8'><Alert level='error' title='Error'>{error}</Alert></div>}

      {collection.status !== 'approved' ? (
        <p className='small'>Only an approved collection can have a correction requested.</p>
      ) : !canRequest ? (
        <p className='small'>You don't have permission to request a correction on this collection.</p>
      ) : hasPending ? (
        <Alert level='warning' title='A correction is already pending'>Wait for it to be decided before requesting another.</Alert>
      ) : (
        <div className='kv mb8' style={{ gap: 10 }}>
          <div>
            <label className='small' style={{ display: 'block', marginBottom: 4 }}>Kind</label>
            <select className='form-select' value={kind} onChange={e => setKind(e.target.value)}>
              {KIND_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </div>

          {kind === 'amount_adjustment' && (
            <div>
              <label className='small' style={{ display: 'block', marginBottom: 4 }}>Corrected amounts</label>
              {items.map((it, idx) => (
                <div key={it.collection_type_id} className='form-row mb8' style={{ alignItems: 'center' }}>
                  <span className='small' style={{ minWidth: 140 }}>{it.name}</span>
                  <input type='number' min='0' step='0.01' className='form-control' value={it.amount} onChange={e => updateAmount(idx, e.target.value)} />
                </div>
              ))}
            </div>
          )}

          <div>
            <label className='small' style={{ display: 'block', marginBottom: 4 }}>Reason *</label>
            <textarea className='form-control' rows={2} value={reason} onChange={e => setReason(e.target.value)} />
          </div>

          <button type='button' className='btn btn-primary' disabled={saving} onClick={submit}>{saving ? 'Submitting…' : 'Submit correction request'}</button>
        </div>
      )}

      <h5 className='card-title mt8' style={{ fontSize: 13 }}>Past requests</h5>
      {loadingList && <p className='small'>Loading…</p>}
      {!loadingList && corrections.length === 0 && <p className='hint'>No corrections requested yet.</p>}
      {!loadingList && corrections.map(c => (
        <div key={c.id} className='field-chip mb8' style={{ display: 'block' }}>
          <div className='flex gap8' style={{ justifyContent: 'space-between' }}>
            <span>{c.kind === 'reversal' ? 'Reversal' : 'Amount adjustment'} — requested by {c.requested_by_username}</span>
            <Badge level={correctionStatusLevel(c.status)}>{c.status}</Badge>
          </div>
          <div className='hint'>{c.reason}</div>
          {c.decision_notes && <div className='hint'>Decision: {c.decision_notes}</div>}
          {c.status === 'pending' && canDecide && c.requested_by_user_id === currentUserId && (
            <div className='hint mt8'>You requested this — a different authorised user must decide it.</div>
          )}
          {c.status === 'pending' && canDecide && c.requested_by_user_id !== currentUserId && (
            <div className='mt8'>
              <input className='form-control form-control-sm mb8' placeholder='Decision notes (optional)' value={decisionNotes} onChange={e => setDecisionNotes(e.target.value)} />
              <div className='flex gap8'>
                <button type='button' className='btn btn-sm btn-primary' disabled={decidingId === c.id} onClick={() => decide(c.id, true)}>Approve</button>
                <button type='button' className='btn btn-sm btn-danger' disabled={decidingId === c.id} onClick={() => decide(c.id, false)}>Reject</button>
              </div>
            </div>
          )}
        </div>
      ))}

      <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
        <button type='button' className='btn btn-ghost' onClick={onClose}>Close</button>
      </div>
    </Modal>
  )
}
