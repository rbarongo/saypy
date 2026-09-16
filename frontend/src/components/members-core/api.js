// Fetch client for api/ (port 8001) — covers auth, churches, reference-data
// lookups, families, members (+contacts), and the RBAC resources (roles,
// permissions, users, user-roles). Independent from the main app's
// `authFetch`/token in App.jsx, though the two now share one login session
// (see CoreApp.jsx's `sharedToken` prop) since both hit the same backend.

const DEFAULT_BASE = typeof window !== 'undefined'
  ? `${window.location.protocol}//${window.location.hostname}:8001`
  : 'http://localhost:8001'
const API_BASE = (import.meta.env.VITE_MEMBERS_API_BASE_URL || DEFAULT_BASE).trim().replace(/\/$/, '')

const TOKEN_KEY = 'members_api_token'
const USER_KEY = 'members_api_user'

function getToken(){
  return localStorage.getItem(TOKEN_KEY) || ''
}

function getStoredUser(){
  try{ return JSON.parse(localStorage.getItem(USER_KEY) || 'null') }catch(e){ return null }
}

function setSession(token, user){
  if(token) localStorage.setItem(TOKEN_KEY, token); else localStorage.removeItem(TOKEN_KEY)
  if(user) localStorage.setItem(USER_KEY, JSON.stringify(user)); else localStorage.removeItem(USER_KEY)
}

function toQueryString(params){
  const usp = new URLSearchParams()
  Object.entries(params || {}).forEach(([key, value]) => {
    if(value !== undefined && value !== null && value !== '') usp.set(key, value)
  })
  const s = usp.toString()
  return s ? `?${s}` : ''
}

async function request(path, { method = 'GET', body, auth = true } = {}){
  const headers = { 'Content-Type': 'application/json' }
  const token = auth ? getToken() : null
  if(token) headers['Authorization'] = `Bearer ${token}`
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })

  if(res.status === 401 && auth){
    setSession(null, null)
    // Only a *real, previously-valid* session expiring/being rejected counts
    // as a session-expired event — tell App.jsx so it drops `user` to null
    // too, which is what actually takes the sidebar/admin shell off screen
    // in favor of the plain LoginScreen (this module's own session store is
    // separate from App.jsx's top-level auth state).
    if(token && typeof window !== 'undefined'){
      window.dispatchEvent(new CustomEvent('saypy:session-expired'))
    }
  }

  const raw = await res.text()
  let data = null
  if(raw){
    try{ data = JSON.parse(raw) }catch(e){ data = raw }
  }

  if(!res.ok){
    const detail = data && data.detail
      ? (typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail))
      : (res.statusText || 'Request failed')
    throw new Error(detail)
  }
  return data
}

// Multipart variant of `request()` for the one endpoint that takes a file
// (collections import inspect) — can't set Content-Type: application/json
// there, the browser needs to set its own multipart boundary header.
async function requestForm(path, formData){
  const headers = {}
  const token = getToken()
  if(token) headers['Authorization'] = `Bearer ${token}`
  const res = await fetch(`${API_BASE}${path}`, { method: 'POST', headers, body: formData })
  if(res.status === 401) setSession(null, null)
  const raw = await res.text()
  let data = null
  if(raw){ try{ data = JSON.parse(raw) }catch(e){ data = raw } }
  if(!res.ok){
    const detail = data && data.detail
      ? (typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail))
      : (res.statusText || 'Request failed')
    throw new Error(detail)
  }
  return data
}

// Authenticated binary download (a plain <a href> can't carry the Bearer
// token — there's no cookie session in this app) — used for the collection
// evidence file, which is never served from a public/unauthenticated route.
async function requestBlob(path){
  const headers = {}
  const token = getToken()
  if(token) headers['Authorization'] = `Bearer ${token}`
  const res = await fetch(`${API_BASE}${path}`, { headers })
  if(res.status === 401) setSession(null, null)
  if(!res.ok){
    let detail = res.statusText || 'Request failed'
    try{ const data = await res.json(); if(data && data.detail) detail = data.detail }catch(e){ /* not JSON */ }
    throw new Error(detail)
  }
  return res.blob()
}

// A {code, name} lookup resource only ever needs this shape of CRUD — reused
// for all 5 reference-data resources instead of writing each out by hand.
function buildLookupApi(path){
  return {
    list: () => request(path),
    create: (payload) => request(path, { method: 'POST', body: payload }),
    update: (id, payload) => request(`${path}/${id}`, { method: 'PUT', body: payload }),
    remove: (id) => request(`${path}/${id}`, { method: 'DELETE' }),
  }
}

// Same shape as `buildLookupApi`, plus a church_id filter on list — for the
// church-scoped lookups (collection types, organisations, SS groups, church
// roles) that also carry a `status` field.
function buildChurchScopedLookupApi(path){
  return {
    list: (params = {}) => request(`${path}${toQueryString(params)}`),
    create: (payload) => request(path, { method: 'POST', body: payload }),
    update: (id, payload) => request(`${path}/${id}`, { method: 'PUT', body: payload }),
    remove: (id) => request(`${path}/${id}`, { method: 'DELETE' }),
  }
}

export const membersApi = {
  API_BASE,
  getToken,
  getStoredUser,
  setSession,

  login: (username, password) => request('/auth/login', { method: 'POST', body: { username, password }, auth: false }),
  me: () => request('/auth/me'),

  getStatsOverview: () => request('/stats/overview'),

  // ---- Reference data --------------------------------------------------
  householdRoles: buildLookupApi('/household-roles'),
  membershipCategories: buildLookupApi('/membership-categories'),
  memberStatuses: buildLookupApi('/member-statuses'),
  contactTypes: buildLookupApi('/contact-types'),
  nationalities: buildLookupApi('/nationalities'),

  // ---- Churches ----------------------------------------------------------
  listChurches: () => request('/churches'),
  createChurch: (payload) => request('/churches', { method: 'POST', body: payload }),
  updateChurch: (id, payload) => request(`/churches/${id}`, { method: 'PUT', body: payload }),
  deleteChurch: (id) => request(`/churches/${id}`, { method: 'DELETE' }),

  // ---- Families ------------------------------------------------------
  listFamilies: (params = {}) => request(`/families${toQueryString(params)}`),
  createFamily: (payload) => request('/families', { method: 'POST', body: payload }),
  getFamily: (id) => request(`/families/${id}`),
  updateFamily: (id, payload) => request(`/families/${id}`, { method: 'PUT', body: payload }),
  deleteFamily: (id) => request(`/families/${id}`, { method: 'DELETE' }),
  getFamilyRoster: (id) => request(`/families/${id}/members`),

  // ---- Members + contacts ---------------------------------------------
  listMembers: (params = {}) => request(`/members${toQueryString(params)}`),
  createMember: (payload) => request('/members', { method: 'POST', body: payload }),
  getMember: (id) => request(`/members/${id}`),
  updateMember: (id, payload) => request(`/members/${id}`, { method: 'PUT', body: payload }),
  deleteMember: (id) => request(`/members/${id}`, { method: 'DELETE' }),
  listMemberContacts: (memberId) => request(`/members/${memberId}/contacts`),
  addMemberContact: (memberId, payload) => request(`/members/${memberId}/contacts`, { method: 'POST', body: payload }),
  updateMemberContact: (memberId, contactId, payload) => request(`/members/${memberId}/contacts/${contactId}`, { method: 'PUT', body: payload }),
  deleteMemberContact: (memberId, contactId) => request(`/members/${memberId}/contacts/${contactId}`, { method: 'DELETE' }),

  // ---- RBAC: roles + permissions -----------------------------------------
  listRoles: () => request('/roles'),
  createRole: (payload) => request('/roles', { method: 'POST', body: payload }),
  updateRole: (id, payload) => request(`/roles/${id}`, { method: 'PUT', body: payload }),
  deleteRole: (id) => request(`/roles/${id}`, { method: 'DELETE' }),
  getRolePermissions: (roleId) => request(`/roles/${roleId}/permissions`),
  setRolePermission: (roleId, permissionId, isAllowed) => request(`/roles/${roleId}/permissions/${permissionId}`, { method: 'PUT', body: { is_allowed: isAllowed } }),

  listPermissions: () => request('/permissions'),
  createPermission: (payload) => request('/permissions', { method: 'POST', body: payload }),
  updatePermission: (id, payload) => request(`/permissions/${id}`, { method: 'PUT', body: payload }),
  deletePermission: (id) => request(`/permissions/${id}`, { method: 'DELETE' }),

  // ---- RBAC: users + user-roles -------------------------------------
  listUsers: () => request('/users'),
  createUser: (payload) => request('/users', { method: 'POST', body: payload }),
  updateUser: (id, payload) => request(`/users/${id}`, { method: 'PUT', body: payload }),
  resetUserPassword: (id, newPassword) => request(`/users/${id}/reset-password`, { method: 'POST', body: { new_password: newPassword } }),
  listUserRoles: (userId) => request(`/users/${userId}/roles`),
  addUserRole: (userId, payload) => request(`/users/${userId}/roles`, { method: 'POST', body: payload }),
  updateUserRole: (userId, userRoleId, payload) => request(`/users/${userId}/roles/${userRoleId}`, { method: 'PUT', body: payload }),
  removeUserRole: (userId, userRoleId) => request(`/users/${userId}/roles/${userRoleId}`, { method: 'DELETE' }),

  // ---- Collection types --------------------------------------------------
  listCollectionTypes: (params = {}) => request(`/collection-types${toQueryString(params)}`),
  createCollectionType: (payload) => request('/collection-types', { method: 'POST', body: payload }),
  updateCollectionType: (id, payload) => request(`/collection-types/${id}`, { method: 'PUT', body: payload }),
  deleteCollectionType: (id) => request(`/collection-types/${id}`, { method: 'DELETE' }),

  // ---- Church-scoped member lookups (ToR: organisations/ministries,
  // Sabbath School groups, church/ministry roles) — same {code, name,
  // church_id, status} shape as collection types.
  organisations: buildChurchScopedLookupApi('/organisations'),
  sabbathSchoolGroups: buildChurchScopedLookupApi('/sabbath-school-groups'),
  churchRoles: buildChurchScopedLookupApi('/church-roles'),

  // ---- Member registration workflow, duplicates, transfer, history ------
  checkMemberDuplicates: (payload) => request('/members/check-duplicates', { method: 'POST', body: payload }),
  listPendingRegistrations: (params = {}) => request(`/members/pending-registrations${toQueryString(params)}`),
  approveMemberRegistration: (id, password) => request(`/members/${id}/approve-registration`, { method: 'POST', body: password ? { password } : undefined }),
  rejectMemberRegistration: (id, reason) => request(`/members/${id}/reject-registration`, { method: 'POST', body: { reason } }),
  transferMemberOut: (id, payload) => request(`/members/${id}/transfer-out`, { method: 'POST', body: payload }),
  getMemberHistory: (id) => request(`/members/${id}/history`),

  // ---- Collections + import wizard ---------------------------------------
  listCollections: (params = {}) => request(`/collections${toQueryString(params)}`),
  createCollection: (payload) => request('/collections', { method: 'POST', body: payload }),
  getCollection: (id) => request(`/collections/${id}`),
  approveCollection: (id) => request(`/collections/${id}/approve`, { method: 'POST' }),
  rejectCollection: (id, reason) => request(`/collections/${id}/reject`, { method: 'POST', body: { reason } }),
  getReceipt: (collectionId) => request(`/collections/${collectionId}/receipt`),

  inspectImportFile: (file) => { const fd = new FormData(); fd.append('file', file); return requestForm('/collections/import/inspect', fd) },
  resolveImportMembers: (payload) => request('/collections/import/resolve-members', { method: 'POST', body: payload }),
  commitImport: (payload) => request('/collections/import/commit', { method: 'POST', body: payload }),

  // ---- Collection groups (review queue) ----------------------------------
  listCollectionGroups: (params = {}) => request(`/collection-groups${toQueryString(params)}`),
  getCollectionGroup: (id) => request(`/collection-groups/${id}`),
  approveAllPendingInGroup: (id) => request(`/collection-groups/${id}/approve-all`, { method: 'POST' }),

  // ---- Treasury: corrections / audit / evidence / receipts ---------------
  requestCollectionCorrection: (collectionId, payload) => request(`/collections/${collectionId}/corrections`, { method: 'POST', body: payload }),
  listCollectionCorrections: (collectionId) => request(`/collections/${collectionId}/corrections`),
  decideCollectionCorrection: (correctionId, approve, decisionNotes) =>
    request(`/collections/corrections/${correctionId}/decide`, { method: 'POST', body: { approve, decision_notes: decisionNotes || null } }),
  uploadCollectionEvidence: (collectionId, file) => { const fd = new FormData(); fd.append('file', file); return requestForm(`/collections/${collectionId}/evidence`, fd) },
  getCollectionEvidenceBlob: (collectionId) => requestBlob(`/collections/${collectionId}/evidence`),
  getCollectionAuditLog: (collectionId) => request(`/collections/${collectionId}/audit-log`),
  getCollectionAuditLogs: (params = {}) => request(`/collection-audit-logs${toQueryString(params)}`),
  getReceiptById: (id) => request(`/receipts/${id}`),
  getReceiptBatch: (ids) => request(`/receipts/batch?ids=${ids.join(',')}`),
  reprintReceipt: (id) => request(`/receipts/${id}/reprint`, { method: 'POST' }),
  cancelReceipt: (id, reason) => request(`/receipts/${id}/cancel`, { method: 'POST', body: { reason } }),
  listReceiptSeries: (params = {}) => request(`/receipt-series${toQueryString(params)}`),
  createReceiptSeries: (payload) => request('/receipt-series', { method: 'POST', body: payload }),
  updateReceiptSeries: (id, payload) => request(`/receipt-series/${id}`, { method: 'PATCH', body: payload }),

  // ---- Current user's own RBAC scopes -------------------------------------
  getMyPermissions: () => request('/auth/me/permissions'),

  // ---- Self-service portal: public self-registration --------------------
  getSelfRegisterOptions: () => request('/members/self-register/options', { auth: false }),
  selfRegister: (payload) => request('/members/self-register', { method: 'POST', body: payload, auth: false }),

  // ---- Self-service portal: authenticated /me endpoints ------------------
  getMyMember: () => request('/members/me'),
  updateMyPreferences: (preferences) => request('/members/me/preferences', { method: 'PUT', body: { preferences } }),
  submitCorrectionRequest: (payload) => request('/members/me/correction-requests', { method: 'POST', body: payload }),
  listMyCorrectionRequests: () => request('/members/me/correction-requests'),
  submitFeedback: (payload) => request('/members/me/feedback', { method: 'POST', body: payload }),
  listMyFeedback: () => request('/members/me/feedback'),
  getMyCollections: (params = {}) => request(`/collections/mine${toQueryString(params)}`),

  // ---- Staff review: correction requests + feedback ----------------------
  listCorrectionRequests: (params = {}) => request(`/members/correction-requests${toQueryString(params)}`),
  approveCorrectionRequest: (id) => request(`/members/correction-requests/${id}/approve`, { method: 'POST' }),
  rejectCorrectionRequest: (id, reason) => request(`/members/correction-requests/${id}/reject`, { method: 'POST', body: { reason } }),
  listFeedback: (params = {}) => request(`/feedback${toQueryString(params)}`),
  markFeedbackReviewed: (id) => request(`/feedback/${id}/mark-reviewed`, { method: 'POST' }),

  // ---- Member management reports ------------------------------------------
  reports: {
    membershipRegister: (params = {}) => request(`/reports/membership-register${toQueryString(params)}`),
    membershipMovements: (params = {}) => request(`/reports/membership-movements${toQueryString(params)}`),
    membershipGrowth: (params = {}) => request(`/reports/membership-growth${toQueryString(params)}`),
    sabbathSchoolMembership: (params = {}) => request(`/reports/sabbath-school-membership${toQueryString(params)}`),
    ministryParticipation: (params = {}) => request(`/reports/ministry-participation${toQueryString(params)}`),
    demographicSummary: (params = {}) => request(`/reports/demographic-summary${toQueryString(params)}`),
    contactCompleteness: (params = {}) => request(`/reports/contact-completeness${toQueryString(params)}`),
  },
}
