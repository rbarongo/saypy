// Gender is kept as a small fixed enum (mirrors app/models.py's `Gender`) —
// everything else that used to be a hardcoded list (household role,
// membership category, status, nationality, contact type) is now real,
// admin-editable reference data fetched from the API.
export const GENDERS = [
  { value: 'male', label: 'Male' },
  { value: 'female', label: 'Female' },
  { value: 'other', label: 'Other' },
]

export function fullName(m){
  if(!m) return ''
  return [m.first_name, m.middle_name, m.last_name].filter(Boolean).join(' ')
}

export function statusBadgeLevel(statusCode){
  switch(statusCode){
    case 'active': return 'success'
    case 'inactive': return 'warning'
    case 'transferred': return 'info'
    case 'deceased': return 'danger'
    default: return 'info'
  }
}

export function byId(list){
  return new Map((list || []).map(item => [item.id, item]))
}
