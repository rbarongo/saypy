import React, { useEffect, useState } from 'react'
import Alert from './Alert'
import { membersApi } from './members-core/api'

// Pre-login page — AdminKit/Bootstrap CSS isn't attached yet (only toggled
// on `!!user` in App.jsx, same as LoginScreen), so this deliberately uses
// plain inputs/`.card`/`.btn` (styles.css's always-loaded styling) rather
// than the Bootstrap-dependent FloatingField components used post-login.

function ChurchMark() {
  return (
    <svg viewBox='0 0 24 24' fill='none' xmlns='http://www.w3.org/2000/svg'>
      <path d='M12 2v3' stroke='#fff' strokeWidth='1.6' strokeLinecap='round' />
      <path d='M10.5 3.5h3' stroke='#fff' strokeWidth='1.6' strokeLinecap='round' />
      <path d='M12 5l8 6v10H4V11l8-6Z' stroke='#fff' strokeWidth='1.6' strokeLinejoin='round' />
      <path d='M9.5 21v-6a2.5 2.5 0 0 1 5 0v6' stroke='#fff' strokeWidth='1.6' strokeLinejoin='round' />
    </svg>
  )
}

const EMPTY_FORM = {
  church_id: '', first_name: '', middle_name: '', last_name: '', gender: '', date_of_birth: '',
  household_role_id: '', nationality_id: '', address: '', occupation: '',
}
const EMPTY_CONTACT_DRAFT = { contact_type_id: '', contact: '' }

export default function SelfRegistrationPage() {
  const [options, setOptions] = useState(null)
  const [optionsError, setOptionsError] = useState('')
  const [form, setForm] = useState(EMPTY_FORM)
  const [contacts, setContacts] = useState([])
  const [contactDraft, setContactDraft] = useState(EMPTY_CONTACT_DRAFT)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [duplicateCandidates, setDuplicateCandidates] = useState([])
  const [submitted, setSubmitted] = useState(false)

  useEffect(() => {
    membersApi.getSelfRegisterOptions()
      .then(setOptions)
      .catch(e => setOptionsError(e.message || 'Failed to load the registration form'))
  }, [])

  function set(field, value) { setForm(prev => ({ ...prev, [field]: value })) }

  function addContact() {
    if (!contactDraft.contact_type_id || !contactDraft.contact) return
    setContacts(prev => [...prev, { ...contactDraft, contact_type_id: Number(contactDraft.contact_type_id) }])
    setContactDraft(EMPTY_CONTACT_DRAFT)
  }
  function removeContact(idx) { setContacts(prev => prev.filter((_, i) => i !== idx)) }

  async function submit(force) {
    if (!form.church_id || !form.first_name.trim() || !form.last_name.trim() || !form.household_role_id) {
      setError('Please fill in church, first name, last name, and household role.')
      return
    }
    setSaving(true); setError(''); setDuplicateCandidates([])
    try {
      await membersApi.selfRegister({
        ...form,
        church_id: Number(form.church_id),
        household_role_id: Number(form.household_role_id),
        nationality_id: form.nationality_id === '' ? null : Number(form.nationality_id),
        date_of_birth: form.date_of_birth || null,
        contacts,
        force_despite_duplicates: force,
      })
      setSubmitted(true)
    } catch (e) {
      let candidates = null
      try { candidates = JSON.parse(e.message)?.candidates } catch (_) { /* not a structured duplicate error */ }
      if (candidates && candidates.length > 0) {
        setDuplicateCandidates(candidates)
      } else {
        setError(e.message || 'Registration failed')
      }
    } finally {
      setSaving(false)
    }
  }

  const contactTypeName = id => options?.contact_types.find(ct => ct.id === id)?.name || id

  return (
    <div className='selfreg-page'>
      <div style={{ width: '100%', maxWidth: 720, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
        <div className='selfreg-banner' style={{ borderRadius: 'var(--radius-lg) var(--radius-lg) 0 0' }}>
          <div className='selfreg-banner-mark'><ChurchMark /></div>
          <h1>Join the family</h1>
          <p>Register yourself as a church member in just a few minutes — a church administrator will review it before it's active.</p>
        </div>

        <div className='selfreg-card'>
          {submitted ? (
            <div style={{ textAlign: 'center' }}>
              <h3 className='card-title' style={{ fontSize: 18 }}>Registration submitted 🎉</h3>
              <Alert level='success' title='Awaiting review'>
                Thank you — your registration has been submitted and is pending review by a church administrator.
                You'll be able to sign in once it's approved.
              </Alert>
              <a className='btn btn-primary mt8' href='/'>Back to sign in</a>
            </div>
          ) : (
            <>
              {optionsError && <Alert level='error' title='Could not load form'>{optionsError}</Alert>}
              {error && <div className='mb8'><Alert level='error' title='Could not submit'>{error}</Alert></div>}

              {duplicateCandidates.length > 0 && (
                <div className='mb8'>
                  <Alert level='warning' title='Possible existing registration found'>
                    <ul className='mb8'>
                      {duplicateCandidates.map(c => (
                        <li key={c.member_id}>{c.member_name} — matched by {c.match_reason.replace(/_/g, ' ')}</li>
                      ))}
                    </ul>
                    <p className='small mb8'>If this isn't you, you can still submit — a church administrator will review it either way.</p>
                    <button type='button' className='btn btn-sm btn-primary' disabled={saving} onClick={() => submit(true)}>Submit anyway</button>
                  </Alert>
                </div>
              )}

              {options && (
                <div className='kv' style={{ gap: 12 }}>
                  <div className='selfreg-section-label'>Church</div>
                  <div>
                    <label style={{ display: 'block', marginBottom: 4 }}>Church *</label>
                    <select value={form.church_id} onChange={e => set('church_id', e.target.value)} style={{ width: '100%' }}>
                      <option value=''>-- select your church --</option>
                      {options.churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
                    </select>
                  </div>

                  <div className='selfreg-section-label'>Personal details</div>
                  <div className='form-row'>
                    <input placeholder='First name *' value={form.first_name} onChange={e => set('first_name', e.target.value)} />
                    <input placeholder='Middle name' value={form.middle_name} onChange={e => set('middle_name', e.target.value)} />
                    <input placeholder='Last name *' value={form.last_name} onChange={e => set('last_name', e.target.value)} />
                  </div>

                  <div className='form-row'>
                    <select value={form.gender} onChange={e => set('gender', e.target.value)}>
                      <option value=''>-- gender --</option>
                      <option value='male'>Male</option>
                      <option value='female'>Female</option>
                      <option value='other'>Other</option>
                    </select>
                    <input type='date' value={form.date_of_birth} onChange={e => set('date_of_birth', e.target.value)} title='Date of birth' />
                    <select value={form.nationality_id} onChange={e => set('nationality_id', e.target.value)}>
                      <option value=''>-- nationality --</option>
                      {options.nationalities.map(n => <option key={n.id} value={n.id}>{n.name}</option>)}
                    </select>
                  </div>

                  <select value={form.household_role_id} onChange={e => set('household_role_id', e.target.value)}>
                    <option value=''>-- household role * --</option>
                    {options.household_roles.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
                  </select>

                  <div className='form-row'>
                    <input placeholder='Address' value={form.address} onChange={e => set('address', e.target.value)} />
                    <input placeholder='Occupation' value={form.occupation} onChange={e => set('occupation', e.target.value)} />
                  </div>

                  <div className='selfreg-section-label'>Contact details</div>
                  <div className='subpanel'>
                    <div className='form-row mb8'>
                      <select value={contactDraft.contact_type_id} onChange={e => setContactDraft(prev => ({ ...prev, contact_type_id: e.target.value }))}>
                        <option value=''>-- type --</option>
                        {options.contact_types.map(ct => <option key={ct.id} value={ct.id}>{ct.name}</option>)}
                      </select>
                      <input placeholder='Value (phone, email, ...)' value={contactDraft.contact} onChange={e => setContactDraft(prev => ({ ...prev, contact: e.target.value }))} />
                      <button type='button' className='btn btn-sm' onClick={addContact}>+ Add</button>
                    </div>
                    {contacts.length === 0 ? (
                      <p className='hint'>No contacts added yet.</p>
                    ) : (
                      <ul className='small' style={{ margin: 0, paddingLeft: 18 }}>
                        {contacts.map((c, i) => (
                          <li key={i}>
                            {contactTypeName(c.contact_type_id)}: {c.contact}
                            <button type='button' className='link-button' style={{ marginLeft: 8 }} onClick={() => removeContact(i)}>Remove</button>
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>

                  <button type='button' className='btn btn-primary mt8' disabled={saving} onClick={() => submit(false)}>
                    {saving ? 'Submitting…' : 'Submit registration'}
                  </button>
                </div>
              )}

              <p className='small' style={{ marginTop: 16, textAlign: 'center' }}>Already registered? <a href='/'>Sign in</a> instead.</p>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
