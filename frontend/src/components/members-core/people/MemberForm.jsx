import React, { useEffect, useState } from 'react'
import { CheckCircle, Copy, Check } from 'react-feather'
import Modal from '../../Modal'
import Alert from '../../Alert'
import { membersApi } from '../api'
import { GENDERS, fullName } from '../constants'
import { FloatingInput, FloatingSelect, FloatingTextarea, FormSwitch } from '../FloatingField'

const EMPTY = {
  first_name: '', middle_name: '', last_name: '',
  gender: '', date_of_birth: '',
  household_role_id: '', membership_category_id: '', status_id: '',
  church_id: '',
  nationality_id: '', address: '', occupation: '', join_date: '', notes: '',
  sabbath_school_group_id: '', organisation_ids: [], church_role_ids: [], preferred_channels: [],
}

const CHANNEL_OPTIONS = [
  { value: 'email', label: 'Email' },
  { value: 'sms', label: 'SMS' },
  { value: 'whatsapp', label: 'WhatsApp' },
  { value: 'physical', label: 'Physical' },
]

const EMPTY_CONTACT_DRAFT = { contact_type_id: '', contact: '', contact_init: false }
const LEADER_SELF = '__self__'

/**
 * Create/edit form for a member, including their contacts and (optionally)
 * their family — either attach to an existing family or create a new one
 * inline, defaulting its leader to the member being saved.
 */
export default function MemberForm({ open, onClose, editingMember, churches, referenceData, defaultChurchId, onSaved, embedded = false }) {
  const [form, setForm] = useState(EMPTY)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')

  const [families, setFamilies] = useState([])
  const [churchMembers, setChurchMembers] = useState([])
  const [familyLoadError, setFamilyLoadError] = useState('')

  // Ministry & Groups (ToR Member Profile associations) — church-scoped,
  // fetched alongside families/churchMembers whenever the church changes.
  const [organisations, setOrganisations] = useState([])
  const [sabbathSchoolGroups, setSabbathSchoolGroups] = useState([])
  const [churchRoles, setChurchRoles] = useState([])

  // Assisted registration review (ToR "Registration review and approval") —
  // create-only; an existing member's registration_status is changed via
  // the dedicated Pending Registrations tab, not by re-editing this form.
  const [submitForReview, setSubmitForReview] = useState(false)

  // Probable-duplicate warning (ToR "prevention of probable duplicate
  // records") — a pre-check before the real submit; "Save anyway" resends
  // with force_despite_duplicates so the backend's own re-check doesn't
  // block it a second time.
  const [duplicateCandidates, setDuplicateCandidates] = useState([])
  const [forceDuplicates, setForceDuplicates] = useState(false)
  const [checkingDuplicates, setCheckingDuplicates] = useState(false)

  // Login account auto-provisioned for a newly-created member (see backend's
  // provision_member_user_account) — the temporary password only exists in
  // this one response, so it's held here just long enough to show the admin.
  const [createdAccount, setCreatedAccount] = useState(null)
  const [copiedField, setCopiedField] = useState('')

  // Family attachment: 'none' | 'existing' | 'new'
  const [familyMode, setFamilyMode] = useState('none')
  const [existingFamilyId, setExistingFamilyId] = useState('')
  const [newFamily, setNewFamily] = useState({ name: '', alias: '', leader: LEADER_SELF })

  // Contacts: for a brand-new member these are staged locally and sent with
  // the create request; for an existing member each action hits the API
  // immediately (there's already a member_id to attach to).
  const [draftContacts, setDraftContacts] = useState([])
  const [contactDraft, setContactDraft] = useState(EMPTY_CONTACT_DRAFT)
  const [contactBusy, setContactBusy] = useState(false)
  const [contactError, setContactError] = useState('')

  useEffect(() => {
    if (!open) return
    if (editingMember) {
      setForm({
        first_name: editingMember.first_name || '',
        middle_name: editingMember.middle_name || '',
        last_name: editingMember.last_name || '',
        gender: editingMember.gender || '',
        date_of_birth: editingMember.date_of_birth || '',
        household_role_id: editingMember.household_role_id ?? '',
        membership_category_id: editingMember.membership_category_id ?? '',
        status_id: editingMember.status_id ?? '',
        church_id: editingMember.church_id ?? '',
        nationality_id: editingMember.nationality_id ?? '',
        address: editingMember.address || '',
        occupation: editingMember.occupation || '',
        join_date: editingMember.join_date || '',
        notes: editingMember.notes || '',
        sabbath_school_group_id: editingMember.sabbath_school_group_id ?? '',
        organisation_ids: editingMember.organisation_ids || [],
        church_role_ids: editingMember.church_role_ids || [],
        preferred_channels: editingMember.preferred_channels || [],
      })
      setDraftContacts([])
      if (editingMember.family_id) {
        setFamilyMode('existing')
        setExistingFamilyId(String(editingMember.family_id))
      } else {
        setFamilyMode('none')
        setExistingFamilyId('')
      }
    } else {
      const today = new Date()
      const yyyy = today.getFullYear()
      const mm = String(today.getMonth() + 1).padStart(2, '0')
      const dd = String(today.getDate()).padStart(2, '0')
      const currentDateStr = `${yyyy}-${mm}-${dd}`
      setForm({ ...EMPTY, church_id: defaultChurchId ?? '', join_date: currentDateStr })
      setDraftContacts([])
      setFamilyMode('none')
      setExistingFamilyId('')
    }
    setNewFamily({ name: '', alias: '', leader: LEADER_SELF })
    setError(''); setContactError(''); setContactDraft(EMPTY_CONTACT_DRAFT)
    setCreatedAccount(null); setCopiedField('')
    setSubmitForReview(false); setDuplicateCandidates([]); setForceDuplicates(false)
  }, [open, editingMember, defaultChurchId])

  useEffect(() => {
    if (!open || !form.church_id) {
      setFamilies([]); setChurchMembers([]); setFamilyLoadError('')
      setOrganisations([]); setSabbathSchoolGroups([]); setChurchRoles([])
      return
    }
    let cancelled = false
    setFamilyLoadError('')
    membersApi.listFamilies({ church_id: form.church_id, page_size: 200 })
      .then(list => { if (!cancelled) setFamilies(list) })
      .catch(e => { if (!cancelled) { setFamilies([]); setFamilyLoadError(e.message || 'Failed to load families') } })
    membersApi.listMembers({ church_id: form.church_id, page_size: 200 })
      .then(res => { if (!cancelled) setChurchMembers((res.items || []).filter(m => !editingMember || m.id !== editingMember.id)) })
      .catch(() => { if (!cancelled) setChurchMembers([]) })
    membersApi.organisations.list({ church_id: form.church_id })
      .then(list => { if (!cancelled) setOrganisations(list.filter(o => o.status === 'active')) })
      .catch(() => { if (!cancelled) setOrganisations([]) })
    membersApi.sabbathSchoolGroups.list({ church_id: form.church_id })
      .then(list => { if (!cancelled) setSabbathSchoolGroups(list.filter(g => g.status === 'active')) })
      .catch(() => { if (!cancelled) setSabbathSchoolGroups([]) })
    membersApi.churchRoles.list({ church_id: form.church_id })
      .then(list => { if (!cancelled) setChurchRoles(list.filter(r => r.status === 'active')) })
      .catch(() => { if (!cancelled) setChurchRoles([]) })
    return () => { cancelled = true }
  }, [open, form.church_id, editingMember])

  if (!open) return null

  function set(field, value) { setForm(prev => ({ ...prev, [field]: value })) }

  function stageContact() {
    if (!contactDraft.contact_type_id || !contactDraft.contact) { setContactError('Choose a contact type and enter a value'); return }
    setDraftContacts(prev => [...prev, { ...contactDraft, contact_type_id: Number(contactDraft.contact_type_id) }])
    setContactDraft(EMPTY_CONTACT_DRAFT)
    setContactError('')
  }

  function unstageContact(index) {
    setDraftContacts(prev => prev.filter((_, i) => i !== index))
  }

  async function addContactToExisting() {
    if (!contactDraft.contact_type_id || !contactDraft.contact) { setContactError('Choose a contact type and enter a value'); return }
    setContactBusy(true); setContactError('')
    try {
      await membersApi.addMemberContact(editingMember.id, { ...contactDraft, contact_type_id: Number(contactDraft.contact_type_id) })
      setContactDraft(EMPTY_CONTACT_DRAFT)
      onSaved(editingMember.id, { keepOpen: true })
    } catch (e) {
      setContactError(e.message || 'Failed to add contact')
    } finally {
      setContactBusy(false)
    }
  }

  async function removeExistingContact(contactId) {
    setContactBusy(true); setContactError('')
    try {
      await membersApi.deleteMemberContact(editingMember.id, contactId)
      onSaved(editingMember.id, { keepOpen: true })
    } catch (e) {
      setContactError(e.message || 'Failed to remove contact')
    } finally {
      setContactBusy(false)
    }
  }

  async function markPrimary(contact) {
    setContactBusy(true); setContactError('')
    try {
      await membersApi.updateMemberContact(editingMember.id, contact.id, { contact_init: true })
      onSaved(editingMember.id, { keepOpen: true })
    } catch (e) {
      setContactError(e.message || 'Failed to update contact')
    } finally {
      setContactBusy(false)
    }
  }

  function buildMemberPayload() {
    const payload = { ...form }
    payload.church_id = Number(payload.church_id)
    payload.household_role_id = Number(payload.household_role_id)
    payload.membership_category_id = Number(payload.membership_category_id)
    payload.status_id = Number(payload.status_id)
    payload.nationality_id = payload.nationality_id === '' ? null : Number(payload.nationality_id)
    payload.sabbath_school_group_id = payload.sabbath_school_group_id === '' ? null : Number(payload.sabbath_school_group_id)
      ;['middle_name', 'gender', 'date_of_birth', 'address', 'occupation', 'join_date', 'notes'].forEach(k => {
        if (payload[k] === '') payload[k] = null
      })
    return payload
  }

  function toggleInList(field, id) {
    setForm(prev => {
      const list = prev[field] || []
      const next = list.includes(id) ? list.filter(x => x !== id) : [...list, id]
      return { ...prev, [field]: next }
    })
  }

  function toggleChannel(channel) {
    setForm(prev => {
      const list = prev.preferred_channels || []
      const next = list.includes(channel) ? list.filter(c => c !== channel) : [...list, channel]
      return { ...prev, preferred_channels: next }
    })
  }

  /** Resolves familyMode into a concrete family_id, creating a new family
   * first if needed (its leader defaults to `selfId` — the member being
   * saved — unless a different existing member was picked). */
  async function resolveFamilyId(selfId) {
    if (familyMode === 'none') return null
    if (familyMode === 'existing') return existingFamilyId ? Number(existingFamilyId) : null
    if (!newFamily.name.trim()) throw new Error('New family needs a name')
    const leaderId = newFamily.leader === LEADER_SELF ? selfId : Number(newFamily.leader)
    const createdFamily = await membersApi.createFamily({
      name: newFamily.name,
      alias: newFamily.alias || null,
      church_id: Number(form.church_id),
      leader_id: leaderId,
    })
    return createdFamily.id
  }

  async function save(force = forceDuplicates) {
    setSaving(true); setError(''); setDuplicateCandidates([])
    try {
      const payload = buildMemberPayload()

      if (editingMember) {
        payload.family_id = await resolveFamilyId(editingMember.id)
        await membersApi.updateMember(editingMember.id, payload)
        onSaved()
      } else {
        payload.family_id = familyMode === 'existing' ? (existingFamilyId ? Number(existingFamilyId) : null) : null
        payload.contacts = draftContacts
        payload.submit_for_review = submitForReview
        payload.force_despite_duplicates = force
        const created = await membersApi.createMember(payload)
        if (familyMode === 'new') {
          const familyId = await resolveFamilyId(created.id)
          await membersApi.updateMember(created.id, { family_id: familyId })
        }
        if (created.created_username) {
          // Hold the modal open one more beat to show the one-time login
          // credentials instead of closing straight through to onSaved.
          setCreatedAccount({ id: created.id, username: created.created_username, password: created.created_password })
        } else {
          onSaved(created.id)
        }
      }
    } catch (e) {
      setError(e.message || 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  /** Runs the probable-duplicate pre-check (new members only) before the
   * real submit — shows a warning + "Save anyway" instead of saving
   * straight through when a likely match is found. Best-effort: if the
   * pre-check itself fails, falls through to the real save, which
   * re-validates server-side anyway (see api's create_member). */
  async function attemptSave() {
    if (editingMember || forceDuplicates) { await save(); return }
    setCheckingDuplicates(true); setError('')
    try {
      const res = await membersApi.checkMemberDuplicates({
        church_id: Number(form.church_id),
        first_name: form.first_name,
        last_name: form.last_name,
        date_of_birth: form.date_of_birth || null,
      })
      if (res.candidates && res.candidates.length > 0) {
        setDuplicateCandidates(res.candidates)
        return
      }
    } catch (e) { /* best-effort — fall through to save() */ } finally {
      setCheckingDuplicates(false)
    }
    await save()
  }

  function saveAnyway() {
    setForceDuplicates(true)
    save(true)
  }

  async function copyToClipboard(text, field) {
    try {
      await navigator.clipboard.writeText(text)
      setCopiedField(field)
      setTimeout(() => setCopiedField(f => (f === field ? '' : f)), 1500)
    } catch (e) { /* clipboard unavailable — the credentials are still visible to copy by hand */ }
  }

  function finishAfterAccountReveal() {
    const id = createdAccount?.id
    setCreatedAccount(null)
    onSaved(id)
  }

  const contactTypeById = new Map((referenceData.contactTypes || []).map(c => [c.id, c]))

  const body = (
    <>
      {error && <div className='mb8'><Alert level='error' title='Could not save'>{error}</Alert></div>}

      {duplicateCandidates.length > 0 && (
        <div className='mb8'>
          <Alert level='warning' title='Possible duplicate member(s) found'>
            <ul className='mb8'>
              {duplicateCandidates.map(c => (
                <li key={c.member_id}>{c.member_name} — matched by {c.match_reason.replace(/_/g, ' ')}</li>
              ))}
            </ul>
            <button type='button' className='btn btn-sm btn-primary' onClick={saveAnyway}>Save anyway</button>
          </Alert>
        </div>
      )}

      <div className='card mb-1'><div className='card-body'>
        <h5 className='card-title mb-1'>Personal Details</h5>
        <div className='form-grid'>
          <FloatingInput label='First name *' value={form.first_name} onChange={e => set('first_name', e.target.value)} />
          <FloatingInput label='Middle name' value={form.middle_name} onChange={e => set('middle_name', e.target.value)} />
          <FloatingInput label='Last name *' value={form.last_name} onChange={e => set('last_name', e.target.value)} />

          <FloatingSelect label='Gender' value={form.gender} onChange={e => set('gender', e.target.value)}>
            <option value=''>-- gender --</option>
            {GENDERS.map(g => <option key={g.value} value={g.value}>{g.label}</option>)}
          </FloatingSelect>
          <FloatingInput label='Date of birth' type='date' value={form.date_of_birth || ''} onChange={e => set('date_of_birth', e.target.value)} />
          <FloatingSelect label='Nationality' value={form.nationality_id} onChange={e => set('nationality_id', e.target.value)}>
            <option value=''>-- nationality --</option>
            {referenceData.nationalities.map(n => <option key={n.id} value={n.id}>{n.name}</option>)}
          </FloatingSelect>
          <FloatingSelect label='Household role *' value={form.household_role_id} onChange={e => set('household_role_id', e.target.value)}>
            <option value=''>-- role --</option>
            {referenceData.householdRoles.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
          </FloatingSelect>
          <FloatingInput label='Address' value={form.address} onChange={e => set('address', e.target.value)} style={{ gridColumn: 'span 2' }} />
          <FloatingInput label='Occupation' value={form.occupation} onChange={e => set('occupation', e.target.value)} />

        </div>
      </div></div>

      <div className='card mb-1'><div className='card-body'>
        <h5 className='card-title mb-1'>Church & Membership</h5>
        <div className='form-grid'>
          <FloatingSelect label='Church *' value={form.church_id} onChange={e => set('church_id', e.target.value)}>
            <option value=''>-- church --</option>
            {churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
          </FloatingSelect>
          <FloatingSelect label='Membership category *' value={form.membership_category_id} onChange={e => set('membership_category_id', e.target.value)}>
            <option value=''>-- category --</option>
            {referenceData.membershipCategories.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
          </FloatingSelect>
          <FloatingSelect label='Status *' value={form.status_id} onChange={e => set('status_id', e.target.value)}>
            <option value=''>-- status --</option>
            {referenceData.memberStatuses.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
          </FloatingSelect>
          {editingMember && (
            <FloatingInput label='Join date' type='date' value={form.join_date || ''} onChange={e => set('join_date', e.target.value)} />
          )}
        </div>
      </div></div>

      <div className='card mb-1'><div className='card-body'>
        <h5 className='card-title mb-1'>Ministry & Groups</h5>
        <div className='form-grid mb8'>
          <FloatingSelect label='Sabbath School group' value={form.sabbath_school_group_id} onChange={e => set('sabbath_school_group_id', e.target.value)}>
            <option value=''>-- none --</option>
            {sabbathSchoolGroups.map(g => <option key={g.id} value={g.id}>{g.name}</option>)}
          </FloatingSelect>
        </div>
        <div className='row'>
          <div className='col-md-6'>
            <label className='small' style={{ display: 'block', marginBottom: 6 }}>Organisations / Ministries</label>
            {organisations.length === 0 && <p className='hint mb-0'>No organisations configured for this church yet.</p>}
            {organisations.map(o => (
              <FormSwitch
                key={o.id}
                label={o.name}
                checked={form.organisation_ids.includes(o.id)}
                onChange={() => toggleInList('organisation_ids', o.id)}
              />
            ))}
          </div>
          <div className='col-md-6'>
            <label className='small' style={{ display: 'block', marginBottom: 6 }}>Church Roles</label>
            {churchRoles.length === 0 && <p className='hint mb-0'>No church roles configured for this church yet.</p>}
            {churchRoles.map(r => (
              <FormSwitch
                key={r.id}
                label={r.name}
                checked={form.church_role_ids.includes(r.id)}
                onChange={() => toggleInList('church_role_ids', r.id)}
              />
            ))}
          </div>
        </div>
      </div></div>

      {/* <div className='card mb-1'><div className='card-body'>
        <h5 className='card-title mb-1'>Contact & Location Details</h5>
        <FloatingTextarea label='Notes' value={form.notes} onChange={e => set('notes', e.target.value)} />
      </div></div> */}

      <div className='card mb-1'><div className='card-body'>
        <h5 className='card-title'>Family</h5>
        {familyLoadError && <div className='mb8'><Alert level='error' title='Could not load families'>{familyLoadError}</Alert></div>}
        <FormSwitch
          label='Add this member to a family'
          checked={familyMode !== 'none'}
          onChange={e => setFamilyMode(e.target.checked ? 'existing' : 'none')}
        />

        {familyMode !== 'none' && (
          <div className='mt8'>
            <div className='form-grid'>
              <FloatingSelect label='Family setup' value={familyMode} onChange={e => setFamilyMode(e.target.value)}>
                <option value='existing'>Select existing family</option>
                <option value='new'>Create a new family</option>
              </FloatingSelect>

              {familyMode === 'existing' && (
                <>
                  <FloatingSelect label='Family *' value={existingFamilyId} onChange={e => setExistingFamilyId(e.target.value)}>
                    <option value=''>-- choose a family --</option>
                    {families.map(f => <option key={f.id} value={f.id}>{f.name}</option>)}
                  </FloatingSelect>
                  {!familyLoadError && families.length === 0 && (
                    <p className='small text-muted mb-0'>No families exist yet for this church — switch to "Create a new family" instead.</p>
                  )}
                </>
              )}

              {familyMode === 'new' && (
                <>
                  <FloatingInput label='Family name *' value={newFamily.name} onChange={e => setNewFamily(prev => ({ ...prev, name: e.target.value }))} />
                  <FloatingInput label='Alias' value={newFamily.alias} onChange={e => setNewFamily(prev => ({ ...prev, alias: e.target.value }))} />
                </>
              )}
            </div>
          </div>
        )}
      </div></div>

      <div className='card mb-1'><div className='card-body'>
        <h5 className='card-title'>Contacts</h5>
        {contactError && <div className='mb8'><Alert level='error' title='Contact error'>{contactError}</Alert></div>}

        <div className='mb8'>
          <label className='small' style={{ display: 'block', marginBottom: 6 }}>Preferred communication channels (general)</label>
          <p className='hint' style={{ marginTop: -2, marginBottom: 6 }}>The member can further refine this per communication type (offering receipts, church announcements) from their own portal.</p>
          <div className='flex gap8' style={{ flexWrap: 'wrap' }}>
            {CHANNEL_OPTIONS.map(ch => (
              <FormSwitch
                key={ch.value}
                label={ch.label}
                checked={form.preferred_channels.includes(ch.value)}
                onChange={() => toggleChannel(ch.value)}
              />
            ))}
          </div>
        </div>

        <div className='form-row mb8'>
          <FloatingSelect label='Type' value={contactDraft.contact_type_id} onChange={e => setContactDraft(prev => ({ ...prev, contact_type_id: e.target.value }))} style={{ minWidth: 160 }}>
            <option value=''>-- type --</option>
            {referenceData.contactTypes.map(ct => <option key={ct.id} value={ct.id}>{ct.name}</option>)}
          </FloatingSelect>
          <FloatingInput label='Value (phone, email, ...)' value={contactDraft.contact} onChange={e => setContactDraft(prev => ({ ...prev, contact: e.target.value }))} />
          <FormSwitch
            label='Primary'
            checked={contactDraft.contact_init}
            onChange={e => setContactDraft(prev => ({ ...prev, contact_init: e.target.checked }))}
          />
          <button
            type='button' className='btn btn-sm'
            disabled={contactBusy}
            onClick={editingMember ? addContactToExisting : stageContact}
          >
            + Add
          </button>
        </div>

        <div className='table-wrap'>
          <table>
            <thead><tr><th>Type</th><th>Value</th><th>Primary</th><th>Actions</th></tr></thead>
            <tbody>
              {(editingMember ? (editingMember.contacts || []) : draftContacts).length === 0 && (
                <tr><td colSpan={4} className='small'>No contacts yet.</td></tr>
              )}
              {editingMember && (editingMember.contacts || []).map(c => (
                <tr key={c.id}>
                  <td>{c.contact_type.name}</td>
                  <td>{c.contact}</td>
                  <td>{c.contact_init ? <span className='badge text-bg-success'>primary</span> : (
                    <button type='button' className='btn btn-sm btn-ghost' disabled={contactBusy} onClick={() => markPrimary(c)}>Make primary</button>
                  )}</td>
                  <td><button type='button' className='btn btn-sm btn-danger' disabled={contactBusy} onClick={() => removeExistingContact(c.id)}>Remove</button></td>
                </tr>
              ))}
              {!editingMember && draftContacts.map((c, i) => (
                <tr key={i}>
                  <td>{contactTypeById.get(c.contact_type_id)?.name || c.contact_type_id}</td>
                  <td>{c.contact}</td>
                  <td>{c.contact_init ? <span className='badge text-bg-success'>primary</span> : '—'}</td>
                  <td><button type='button' className='btn btn-sm btn-danger' onClick={() => unstageContact(i)}>Remove</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div></div>

      {!editingMember && (
        <div className='card mb-1'><div className='card-body'>
          <FormSwitch
            label='Submit for review (assisted registration)'
            checked={submitForReview}
            onChange={e => setSubmitForReview(e.target.checked)}
          />
          {submitForReview && (
            <p className='hint mb-0'>
              This member is saved as pending — a reviewer with the Review Members permission must approve it
              (Member Management → Pending Registrations) before their record is active and their login account is created.
            </p>
          )}
        </div></div>
      )}

      <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
        {onClose && <button type='button' className='btn btn-ghost' onClick={onClose}>Cancel</button>}
        <button type='button' className='btn btn-primary' disabled={saving || checkingDuplicates} onClick={attemptSave}>
          {checkingDuplicates ? 'Checking…' : saving ? 'Saving…' : (editingMember ? 'Save changes' : 'Register member')}
        </button>
      </div>
    </>
  )

  const accountReveal = (
    <div>
      <div className='flex gap8 mb8' style={{ alignItems: 'center' }}>
        <CheckCircle size={20} className='text-success' />
        <h5 className='card-title mb-0'>Member registered</h5>
      </div>
      <p className='small mb8'>
        A login account was created for them with the default <strong>Member</strong> role. Share these credentials
        securely — the password is shown only this once and can't be recovered later, only reset.
      </p>
      <div className='kv' style={{ gap: 10 }}>
        <div className='field-chip' style={{ justifyContent: 'space-between' }}>
          <span><strong>Username:</strong> {createdAccount?.username}</span>
          <button type='button' className='btn btn-sm btn-ghost' onClick={() => copyToClipboard(createdAccount?.username, 'username')}>
            {copiedField === 'username' ? <><Check size={14} /> Copied</> : <><Copy size={14} /> Copy</>}
          </button>
        </div>
        <div className='field-chip' style={{ justifyContent: 'space-between' }}>
          <span><strong>Temporary password:</strong> {createdAccount?.password}</span>
          <button type='button' className='btn btn-sm btn-ghost' onClick={() => copyToClipboard(createdAccount?.password, 'password')}>
            {copiedField === 'password' ? <><Check size={14} /> Copied</> : <><Copy size={14} /> Copy</>}
          </button>
        </div>
      </div>
      <div className='flex gap8 mt8' style={{ justifyContent: 'flex-end' }}>
        <button type='button' className='btn btn-primary' onClick={finishAfterAccountReveal}>Done</button>
      </div>
    </div>
  )

  if (embedded) {
    return <div className='card'><div className='card-body'>{createdAccount ? accountReveal : body}</div></div>
  }

  return (
    <Modal title={createdAccount ? 'Member registered' : (editingMember ? `Edit ${fullName(editingMember)}` : 'New Member')} onClose={onClose} maxWidth={820}>
      {createdAccount ? accountReveal : body}
    </Modal>
  )
}
