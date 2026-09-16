import React, { useEffect, useState } from 'react'
import { toast } from 'react-toastify'
import { MessageSquare } from 'react-feather'
import Alert from '../Alert'
import Badge from '../Badge'
import { membersApi } from '../members-core/api'

const STATUS_LEVEL = { open: 'warning', reviewed: 'success' }

export default function FeedbackTab() {
  const [subject, setSubject] = useState('')
  const [message, setMessage] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [success, setSuccess] = useState('')
  const [items, setItems] = useState([])
  const [loading, setLoading] = useState(true)

  async function refresh() {
    setLoading(true)
    try {
      const list = await membersApi.listMyFeedback()
      setItems(list || [])
    } catch (e) {
      // non-fatal
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [])

  async function submit() {
    if (!subject.trim() || !message.trim()) {
      const err = 'Subject and message are both required.'
      setError(err)
      toast.warning(err)
      return
    }
    setSaving(true); setError(''); setSuccess('')
    try {
      await membersApi.submitFeedback({ subject: subject.trim(), message: message.trim() })
      setSubject(''); setMessage('')
      const succMsg = 'Thank you — your feedback has been submitted.'
      setSuccess(succMsg)
      toast.success(succMsg)
      await refresh()
    } catch (e) {
      const errMsg = e.message || 'Failed to submit feedback'
      setError(errMsg)
      toast.error(errMsg)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div>
      <div className='portal-section-title'>
        <span className='portal-section-icon'><MessageSquare size={15} /></span>
        Submit Feedback &amp; Suggestions
      </div>
      <div className='portal-card card--accent'>
        {error && <div className='mb8'><Alert level='error' title='Could not submit'>{error}</Alert></div>}
        {success && <div className='mb8'><Alert level='success' title='Submitted'>{success}</Alert></div>}
        <div className='kv' style={{ gap: 10 }}>
          <input className='form-control' placeholder='Subject' value={subject} onChange={e => setSubject(e.target.value)} maxLength={200} />
          <textarea className='form-control' rows={4} placeholder='Your feedback or suggestion' value={message} onChange={e => setMessage(e.target.value)} maxLength={2000} />
          <button type='button' className='btn btn-primary' disabled={saving} onClick={submit}>{saving ? 'Submitting…' : 'Submit'}</button>
        </div>
      </div>

      <div className='portal-section-title' style={{ fontSize: 16 }}>My past submissions</div>
      <div className='portal-card'>
        {loading && <p className='small'>Loading…</p>}
        {!loading && items.length === 0 && <p className='hint'>No feedback submitted yet.</p>}
        {!loading && items.length > 0 && (
          <ul className='small' style={{ margin: 0, paddingLeft: 0, listStyle: 'none' }}>
            {items.map(f => (
              <li key={f.id} style={{ padding: '10px 0', borderBottom: '1px dashed var(--border)' }}>
                <div className='form-row' style={{ justifyContent: 'space-between' }}>
                  <strong>{f.subject}</strong>
                  <Badge level={STATUS_LEVEL[f.status] || 'secondary'}>{f.status}</Badge>
                </div>
                <div className='hint'>{new Date(f.created_at).toLocaleString()}</div>
                <p style={{ margin: '4px 0 0' }}>{f.message}</p>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}
