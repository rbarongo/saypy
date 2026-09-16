import React from 'react'
import { ALL_ACTIONS, ACTION_LABELS, DEFAULT_MOVEMENT_ACTIONS } from '../actionLabels'

const GAP_OPTIONS = [
  { value: '', label: '-- all members --' },
  { value: 'missing_phone', label: 'Missing phone' },
  { value: 'missing_email', label: 'Missing email' },
  { value: 'missing_both', label: 'Missing both' },
  { value: 'has_all', label: 'Has phone & email' },
]

/** Renders one control per filter token — see reportsConfig.js for the
 * token list each report declares. `referenceData` is useReferenceData()'s
 * output (churches/categories/roles/statuses); `scopedLookups` holds the
 * church-scoped lookups (organisations/church roles/SS groups) the parent
 * refetches whenever the selected church changes. */
export default function ReportFilters({ tokens, values, onChange, referenceData, scopedLookups }) {
  function set(key, value) { onChange({ ...values, [key]: value }) }

  function toggleAction(action) {
    const current = new Set((values.actions ? values.actions.split(',') : DEFAULT_MOVEMENT_ACTIONS))
    if (current.has(action)) current.delete(action); else current.add(action)
    set('actions', Array.from(current).join(','))
  }

  return (
    <div className='form-row' style={{ alignItems: 'flex-end' }}>
      {tokens.includes('church') && (
        <div>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Church</label>
          <select className='form-select' value={values.church_id || ''} onChange={e => set('church_id', e.target.value)}>
            <option value=''>-- all accessible churches --</option>
            {(referenceData.churches || []).map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
          </select>
        </div>
      )}

      {tokens.includes('status') && (
        <div>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Status</label>
          <select className='form-select' value={values.status_id || ''} onChange={e => set('status_id', e.target.value)}>
            <option value=''>-- any --</option>
            {(referenceData.memberStatuses || []).map(s => <option key={s.id} value={s.id}>{s.name}</option>)}
          </select>
        </div>
      )}

      {tokens.includes('membershipCategory') && (
        <div>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Membership Category</label>
          <select className='form-select' value={values.membership_category_id || ''} onChange={e => set('membership_category_id', e.target.value)}>
            <option value=''>-- any --</option>
            {(referenceData.membershipCategories || []).map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
          </select>
        </div>
      )}

      {tokens.includes('householdRole') && (
        <div>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Household Role</label>
          <select className='form-select' value={values.household_role_id || ''} onChange={e => set('household_role_id', e.target.value)}>
            <option value=''>-- any --</option>
            {(referenceData.householdRoles || []).map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
          </select>
        </div>
      )}

      {tokens.includes('sabbathSchoolGroup') && (
        <div>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Sabbath School Group</label>
          <select className='form-select' value={values.sabbath_school_group_id ?? ''} onChange={e => set('sabbath_school_group_id', e.target.value)}>
            <option value=''>-- any --</option>
            <option value='0'>— none assigned —</option>
            {(scopedLookups.sabbathSchoolGroups || []).map(g => <option key={g.id} value={g.id}>{g.name}</option>)}
          </select>
        </div>
      )}

      {tokens.includes('organisation') && (
        <div>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Organisation</label>
          <select className='form-select' value={values.organisation_id || ''} onChange={e => set('organisation_id', e.target.value)}>
            <option value=''>-- any --</option>
            {(scopedLookups.organisations || []).map(o => <option key={o.id} value={o.id}>{o.name}</option>)}
          </select>
        </div>
      )}

      {tokens.includes('churchRole') && (
        <div>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Church Role</label>
          <select className='form-select' value={values.church_role_id || ''} onChange={e => set('church_role_id', e.target.value)}>
            <option value=''>-- any --</option>
            {(scopedLookups.churchRoles || []).map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
          </select>
        </div>
      )}

      {tokens.includes('gap') && (
        <div>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Contact gap</label>
          <select className='form-select' value={values.gap || ''} onChange={e => set('gap', e.target.value)}>
            {GAP_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
          </select>
        </div>
      )}

      {tokens.includes('dateRange') && (
        <>
          <div>
            <label className='small' style={{ display: 'block', marginBottom: 4 }}>From</label>
            <input type='date' className='form-control' value={values.date_from || ''} onChange={e => set('date_from', e.target.value)} />
          </div>
          <div>
            <label className='small' style={{ display: 'block', marginBottom: 4 }}>To</label>
            <input type='date' className='form-control' value={values.date_to || ''} onChange={e => set('date_to', e.target.value)} />
          </div>
        </>
      )}

      {tokens.includes('search') && (
        <div>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Search name</label>
          <input className='form-control' value={values.q || ''} onChange={e => set('q', e.target.value)} placeholder='Name or national ID' />
        </div>
      )}

      {tokens.includes('actions') && (
        <div style={{ width: '100%' }}>
          <label className='small' style={{ display: 'block', marginBottom: 4 }}>Actions to include</label>
          <div className='flex gap8' style={{ flexWrap: 'wrap' }}>
            {ALL_ACTIONS.map(a => (
              <label key={a} className='small' style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <input
                  type='checkbox'
                  checked={(values.actions ? values.actions.split(',') : DEFAULT_MOVEMENT_ACTIONS).includes(a)}
                  onChange={() => toggleAction(a)}
                />
                {ACTION_LABELS[a]}
              </label>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
