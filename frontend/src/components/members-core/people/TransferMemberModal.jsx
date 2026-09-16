import React, { useState } from 'react'
import Alert from '../../Alert'
import Modal from '../../Modal'
import { FloatingInput, FloatingSelect } from '../FloatingField'
import { membersApi } from '../api'

function todayStr() { return new Date().toISOString().slice(0, 10) }

/** Transfer-out (ToR Registration: "Transfer-in and transfer-out") — moves
 * the member to the 'transferred' status and records where/when/why. */
export default function TransferMemberModal({ member, churches, onClose, onTransferred }) {
  const [toChurchId, setToChurchId] = useState('')
  const [transferDate, setTransferDate] = useState(todayStr())
  const [reason, setReason] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')

  const otherChurches = (churches || []).filter(c => c.id !== member.church_id)

  async function submit() {
    if (!toChurchId) { setError('Choose the destination church'); return }
    setSaving(true); setError('')
    try {
      await membersApi.transferMemberOut(member.id, {
        to_church_id: Number(toChurchId),
        transfer_date: transferDate,
        reason: reason || null,
      })
      onTransferred()
    } catch (e) {
      setError(e.message || 'Transfer failed')
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal title={`Transfer out — ${member.first_name} ${member.last_name}`} onClose={onClose} maxWidth={480}>
      {error && <div className='mb8'><Alert level='error' title='Could not transfer'>{error}</Alert></div>}
      <div className='kv' style={{ gap: 10 }}>
        <FloatingSelect label='Destination church *' value={toChurchId} onChange={e => setToChurchId(e.target.value)}>
          <option value=''>-- church --</option>
          {otherChurches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </FloatingSelect>
        <FloatingInput label='Transfer date *' type='date' value={transferDate} onChange={e => setTransferDate(e.target.value)} />
        <FloatingInput label='Reason' value={reason} onChange={e => setReason(e.target.value)} />
      </div>
      <p className='hint'>
        Sets this member's status to "Transferred" and records the destination church, date, and reason — their record
        stays in this church's history rather than being deleted or moved.
      </p>
      <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
        <button type='button' className='btn btn-ghost' onClick={onClose}>Cancel</button>
        <button type='button' className='btn btn-primary' disabled={saving} onClick={submit}>{saving ? 'Transferring…' : 'Transfer out'}</button>
      </div>
    </Modal>
  )
}
