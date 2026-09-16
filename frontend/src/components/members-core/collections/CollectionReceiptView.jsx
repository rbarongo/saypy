import React, { useEffect, useState } from 'react'
import { jsPDF } from 'jspdf'
import autoTable from 'jspdf-autotable'
import { Printer } from 'react-feather'
import Alert from '../../Alert'
import Badge from '../../Badge'
import Modal from '../../Modal'
import { membersApi } from '../api'
import useMyPermissions from '../../../hooks/useMyPermissions'

function statusLevel(status) {
  if (status === 'cancelled') return 'danger'
  if (status === 'corrected') return 'secondary'
  if (status === 'reprinted') return 'warning'
  return 'success'
}

/**
 * Fetches and displays the receipt for an approved collection, with a
 * client-side PDF export — same division of labor as the old app (no
 * backend PDF generation; jsPDF/jspdf-autotable already a dependency here).
 */
export default function CollectionReceiptView({ collectionId, onClose }) {
  const { can } = useMyPermissions()
  const [receipt, setReceipt] = useState(null)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [cancelling, setCancelling] = useState(false)
  const [cancelReason, setCancelReason] = useState('')

  async function load() {
    try { setReceipt(await membersApi.getReceipt(collectionId)) }
    catch (e) { setError(e.message || 'Failed to load receipt') }
  }

  useEffect(() => { load() }, [collectionId]) // eslint-disable-line react-hooks/exhaustive-deps

  function downloadPdf() {
    if (!receipt) return
    const doc = new jsPDF()
    doc.setFontSize(16)
    doc.text('Collection Receipt', 14, 18)
    doc.setFontSize(11)
    doc.text(`Receipt No: ${receipt.receipt_number}`, 14, 28)
    doc.text(`Member: ${receipt.member_name || ''}`, 14, 35)
    doc.text(`Date: ${receipt.collection.collection_date}`, 14, 42)
    doc.text(`Issued by: ${receipt.issued_by_username || ''} on ${new Date(receipt.issued_at).toLocaleString()}`, 14, 49)

    autoTable(doc, {
      startY: 56,
      head: [['Collection type', 'Amount']],
      body: (receipt.collection.items || []).map(i => [i.collection_type.name, Number(i.amount).toFixed(2)]),
      foot: [['Total', Number(receipt.total_amount).toFixed(2)]],
    })

    doc.save(`${receipt.receipt_number}.pdf`)
  }

  async function reprint() {
    setBusy(true); setError('')
    try {
      await membersApi.reprintReceipt(receipt.id)
      await load()
    } catch (e) {
      setError(e.message || 'Reprint failed')
    } finally {
      setBusy(false)
    }
  }

  async function submitCancel() {
    if (!cancelReason.trim()) { setError('A reason is required to cancel a receipt'); return }
    setBusy(true); setError('')
    try {
      await membersApi.cancelReceipt(receipt.id, cancelReason.trim())
      setCancelling(false); setCancelReason('')
      await load()
    } catch (e) {
      setError(e.message || 'Cancel failed')
    } finally {
      setBusy(false)
    }
  }

  const canManage = receipt ? can('manage_receipts', receipt.church_id) : false

  return (
    <Modal title='Receipt' onClose={onClose} maxWidth={520}>
      {error && <div className='mb8'><Alert level='error' title='Could not load receipt'>{error}</Alert></div>}
      {!error && !receipt && <p className='small'>Loading…</p>}
      {receipt && (
        <div>
          <div className='kv' style={{ gap: 8 }}>
            <div className='field-chip' style={{ justifyContent: 'space-between' }}>
              <span><strong>Receipt No:</strong>&nbsp;{receipt.receipt_number}</span>
              <Badge level={statusLevel(receipt.status)}>{receipt.status}</Badge>
            </div>
            <div className='field-chip'><strong>Member:</strong>&nbsp;{receipt.member_name || 'Anonymous'}</div>
            <div className='field-chip'><strong>Date:</strong>&nbsp;{receipt.collection.collection_date}</div>
            <div className='field-chip'><strong>Issued by:</strong>&nbsp;{receipt.issued_by_username} on {new Date(receipt.issued_at).toLocaleString()}</div>
            {receipt.reprint_count > 0 && <div className='field-chip'><strong>Reprinted:</strong>&nbsp;{receipt.reprint_count} time(s)</div>}
            {receipt.status === 'cancelled' && receipt.cancelled_reason && (
              <div className='field-chip'><strong>Cancelled:</strong>&nbsp;{receipt.cancelled_reason} (by {receipt.cancelled_by_username})</div>
            )}
            {receipt.superseded_by_receipt_id && (
              <div className='field-chip'><strong>Superseded by receipt #{receipt.superseded_by_receipt_id}</strong> — see the collection's current receipt for the corrected amount.</div>
            )}
          </div>

          <div className='table-wrap mt8'>
            <table>
              <thead><tr><th>Collection type</th><th className='text-right'>Amount</th></tr></thead>
              <tbody>
                {(receipt.collection.items || []).map(i => (
                  <tr key={i.id}><td>{i.collection_type.name}</td><td className='text-right'>{Number(i.amount).toFixed(2)}</td></tr>
                ))}
                <tr className='row-total'><td>Total</td><td className='text-right'>{Number(receipt.total_amount).toFixed(2)}</td></tr>
              </tbody>
            </table>
          </div>

          {cancelling && (
            <div className='card mt8'>
              <p className='small mb8'>State the reason this receipt is being cancelled.</p>
              <textarea className='form-control' rows={2} value={cancelReason} onChange={e => setCancelReason(e.target.value)} placeholder='Reason *' />
              <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
                <button type='button' className='btn btn-ghost' onClick={() => setCancelling(false)}>Back</button>
                <button type='button' className='btn btn-danger' disabled={busy} onClick={submitCancel}>{busy ? 'Cancelling…' : 'Confirm cancel'}</button>
              </div>
            </div>
          )}

          <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end', flexWrap: 'wrap' }}>
            {canManage && receipt.status !== 'cancelled' && !cancelling && (
              <>
                <button type='button' className='btn btn-ghost' disabled={busy} onClick={reprint}>Reprint</button>
                <button type='button' className='btn btn-outline-danger' disabled={busy} onClick={() => setCancelling(true)}>Cancel receipt</button>
              </>
            )}
            <button type='button' className='btn btn-ghost' onClick={onClose}>Close</button>
            <button type='button' className='btn btn-primary' onClick={downloadPdf}><Printer size={16} /> Download PDF</button>
          </div>
        </div>
      )}
    </Modal>
  )
}
