import React, { useState, useEffect } from 'react'
import './styles.css'

const LOCAL_API_BASE = 'http://localhost:8000'
const API_BASE = (import.meta.env.VITE_API_BASE_URL || '').trim().replace(/\/$/, '')

function rewriteApiUrl(input){
  // Default to HTTP for API calls; backend commonly runs on plain HTTP:8000.
  const base = API_BASE || (typeof window !== 'undefined' ? `http://${window.location.hostname}:8000` : LOCAL_API_BASE)
  if(typeof input === 'string' && input.startsWith(LOCAL_API_BASE)){
    return `${base}${input.slice(LOCAL_API_BASE.length)}`
  }
  if(typeof Request !== 'undefined' && input instanceof Request && input.url.startsWith(LOCAL_API_BASE)){
    return new Request(`${base}${input.url.slice(LOCAL_API_BASE.length)}`, input)
  }
  return input
}

if(typeof window !== 'undefined' && !window.__saypyFetchPatched){
  const nativeFetch = window.fetch.bind(window)
  window.fetch = (input, init) => nativeFetch(rewriteApiUrl(input), init)
  window.__saypyFetchPatched = true
}

// Single-file frontend with Login, RBAC, and pages for Admin/Members/Collections/Reports
export default function App(){
  const [page, setPage] = useState('dashboard') // default landing
  const [token, setToken] = useState(localStorage.getItem('token') || '')
  const [user, setUser] = useState(JSON.parse(localStorage.getItem('user')||'null'))
  const [status, setStatus] = useState('')
  const [rolesCatalog, setRolesCatalog] = useState([])
  const currentUserChurchId = user && user.church != null ? Number(user.church) : null

  function canAccessChurch(churchId){
    if(churchId === null || churchId === undefined || churchId === '') return true
    if(currentUserChurchId === null || Number.isNaN(currentUserChurchId)) return true
    return Number(churchId) === currentUserChurchId
  }

  function denyRestrictedChurchAccess(context){
    setStatus(`Restricted information: you cannot access ${context} for another church`)
  }

  function passwordPolicyMessage(password){
    const p = String(password || '')
    if(p.length < 8) return 'Password must be at least 8 characters long'
    if(!/[A-Za-z]/.test(p)) return 'Password must include at least one letter'
    if(!/[0-9]/.test(p)) return 'Password must include at least one number'
    if(!/[^A-Za-z0-9]/.test(p)) return 'Password must include at least one special character'
    return ''
  }
  const PASSWORD_HINT = 'Password: minimum 8 characters, include at least 1 letter, 1 number, and 1 special character.'
  function roleLabel(role){
    const found = (rolesCatalog || []).find(r => String(r.role || '').toLowerCase() === String(role || '').toLowerCase())
    if(found && found.display_name) return String(found.display_name)
    const map = {
      system_admin: 'System Admin',
      admin: 'Church Admin',
      treasurer: 'Treasurer',
      data_steward: 'Data Steward',
      uploader: 'Uploader',
      viewer: 'Viewer',
    }
    return map[String(role||'').toLowerCase()] || String(role || '')
  }

  function roleOptionsForCurrentUser(){
    const dynamicRoles = (rolesCatalog || []).map(r => String(r.role || '').toLowerCase()).filter(Boolean)
    if(dynamicRoles.length){
      const me = String(user?.role || '').toLowerCase()
      if(me === 'system_admin') return dynamicRoles
      return dynamicRoles.filter(r => r !== 'system_admin')
    }
    const me = String(user?.role || '').toLowerCase()
    if(me === 'system_admin'){
      return ['admin','treasurer','data_steward','uploader','viewer','system_admin']
    }
    return ['admin','treasurer','data_steward','uploader','viewer']
  }

  // ----- Shared data -----
  const [churches, setChurches] = useState([])
  const [appName, setAppName] = useState('Church Offerings — Admin Console')  // Custom app name for the church

  useEffect(()=>{
    fetchChurches()
  }, [])

  useEffect(()=>{
    if(user && isAdmin()) fetchRoles()
  }, [user, token])

  // Fetch app_name when user logs in and has a church assigned
  useEffect(()=>{
    if(user && currentUserChurchId){
      fetchAppName()
    }else if(!user){
      setAppName('Church Offerings — Admin Console')
    }
  }, [user, currentUserChurchId])

  useEffect(()=>{
    if(token){ localStorage.setItem('token', token) }
    else { localStorage.removeItem('token') }
    if(user){ localStorage.setItem('user', JSON.stringify(user)) }
    else { localStorage.removeItem('user') }
  }, [token, user])

  function authHeaders(extra={}){
    const h = { ...extra }
    if(token) h['Authorization'] = `Bearer ${token}`
    return h
  }

  async function fetchChurches(){
    try{ const res = await fetch('http://localhost:8000/churches'); const data = await res.json(); setChurches(data) }catch(e){}
  }

  async function fetchRoles(){
    try{
      const res = await fetch('http://localhost:8000/roles', { headers: authHeaders() })
      const data = await res.json().catch(()=>[])
      if(res.ok && Array.isArray(data)) setRolesCatalog(data)
    }catch(e){
      // keep fallback hardcoded roles
    }
  }

  async function fetchAppName(){
    try{
      const churchId = currentUserChurchId || (user && user.church)
      if(!churchId) return
      const res = await fetch(`http://localhost:8000/churches/${churchId}/app_name`, { headers: authHeaders() })
      const data = await res.json().catch(()=> ({}))
      if(res.ok && data.app_name){
        setAppName(data.app_name)
      }
    }catch(e){
      // Silently fail; keep default app name
    }
  }

  // ----- Login / token management -----
  const [loginUser, setLoginUser] = useState('')
  const [loginPass, setLoginPass] = useState('')
  async function doLogin(){
    try{
      const res = await fetch('http://localhost:8000/users/login',{method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({username: loginUser, password: loginPass})})
      const data = await res.json()
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setToken(data.token)
      setUser(data.user)
      setStatus('Logged in')
      setPage('dashboard')
    }catch(e){ setStatus('Login failed: '+e.message) }
  }
  function logout(){ setToken(''); setUser(null); setStatus('Logged out'); }

  // ----- Admin: Settings -----
  const [editingAppName, setEditingAppName] = useState('')
  const [showEditAppName, setShowEditAppName] = useState(false)
  const [localCollectionCodes, setLocalCollectionCodes] = useState([])
  const [newCodeColumn, setNewCodeColumn] = useState('')
  const [newCodeLabel, setNewCodeLabel] = useState('')
  const [editingCodeId, setEditingCodeId] = useState(null)
  const [editCodeForm, setEditCodeForm] = useState({column_name:'', code:''})
  const [editingRole, setEditingRole] = useState(null)
  const [newRole, setNewRole] = useState({
    role: '',
    display_name: '',
    can_view_dashboard: true,
    can_manage_users: false,
    can_manage_members: false,
    can_manage_collections: false,
    can_view_reports: false,
    can_manage_settings: false,
    can_manage_roles: false,
  })

  async function submitAppName(){
    try{
      if(!currentUserChurchId){ setStatus('Error: Unable to determine church'); return }
      const url = `http://localhost:8000/churches/${currentUserChurchId}/app_name`
      console.log('Updating app_name at:', url, 'with:', {app_name: editingAppName})
      const res = await fetch(url, {
        method: 'PUT',
        headers: {...authHeaders(), 'Content-Type':'application/json'},
        body: JSON.stringify({app_name: editingAppName})
      })
      console.log('Response status:', res.status, res.statusText)
      const data = await res.json().catch(()=>({}))
      console.log('Response data:', data)
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setAppName(editingAppName)
      setShowEditAppName(false)
      setStatus('App name updated successfully')
    }catch(e){ 
      console.error('submitAppName error:', e)
      setStatus('Update app name failed: '+e.message) 
    }
  }

  async function createLocalCode(){
    try{
      if(!currentUserChurchId){ setStatus('Error: Unable to determine church'); return }
      if(!newCodeColumn.trim()){ setStatus('Error: Column name is required'); return }
      if(!newCodeLabel.trim()){ setStatus('Error: Code label is required'); return }
      const res = await fetch(`http://localhost:8000/churches/${currentUserChurchId}/collection_codes`, {
        method: 'POST',
        headers: {...authHeaders(), 'Content-Type':'application/json'},
        body: JSON.stringify({column_name: newCodeColumn, code: newCodeLabel})
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setNewCodeColumn('')
      setNewCodeLabel('')
      fetchLocalCodes()
      setStatus('Code created successfully')
    }catch(e){ setStatus('Create code failed: '+e.message) }
  }

  async function updateLocalCode(){
    try{
      if(!currentUserChurchId){ setStatus('Error: Unable to determine church'); return }
      if(!editingCodeId){ setStatus('Error: No code selected'); return }
      const res = await fetch(`http://localhost:8000/churches/${currentUserChurchId}/collection_codes/${editingCodeId}`, {
        method: 'PUT',
        headers: {...authHeaders(), 'Content-Type':'application/json'},
        body: JSON.stringify({column_name: editCodeForm.column_name, code: editCodeForm.code})
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setEditingCodeId(null)
      setEditCodeForm({column_name:'', code:''})
      fetchLocalCodes()
      setStatus('Code updated successfully')
    }catch(e){ setStatus('Update code failed: '+e.message) }
  }

  async function deleteLocalCode(codeId){
    try{
      if(!currentUserChurchId){ setStatus('Error: Unable to determine church'); return }
      if(!window.confirm('Are you sure you want to delete this code?')) return
      const res = await fetch(`http://localhost:8000/churches/${currentUserChurchId}/collection_codes/${codeId}`, {
        method: 'DELETE',
        headers: authHeaders()
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      fetchLocalCodes()
      setStatus('Code deleted successfully')
    }catch(e){ setStatus('Delete code failed: '+e.message) }
  }

  async function fetchLocalCodes(){
    try{
      if(!currentUserChurchId){ return }
      // Filter codes where church == currentUserChurchId from the full collection_codes list
      const res = await fetch('http://localhost:8000/collection_codes', { headers: authHeaders() })
      const data = await res.json().catch(()=>[])
      if(res.ok){
        const localCodes = data.filter(c=>Number(c.church)===Number(currentUserChurchId))
        setLocalCollectionCodes(localCodes)
      }
    }catch(e){ /* silently fail */ }
  }

  async function createRolePolicy(){
    try{
      if(String(user?.role || '').toLowerCase() !== 'system_admin'){
        setStatus('Only system admin can define roles')
        return
      }
      const roleName = String(newRole.role || '').trim().toLowerCase()
      if(!roleName){ setStatus('Role key is required'); return }
      const res = await fetch('http://localhost:8000/roles', {
        method: 'POST',
        headers: {...authHeaders(), 'Content-Type':'application/json'},
        body: JSON.stringify({...newRole, role: roleName})
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus('Role created')
      setNewRole({
        role: '',
        display_name: '',
        can_view_dashboard: true,
        can_manage_users: false,
        can_manage_members: false,
        can_manage_collections: false,
        can_view_reports: false,
        can_manage_settings: false,
        can_manage_roles: false,
      })
      await fetchRoles()
    }catch(e){ setStatus('Create role failed: ' + e.message) }
  }

  async function saveRolePolicy(){
    try{
      if(!editingRole){ return }
      if(String(user?.role || '').toLowerCase() !== 'system_admin'){
        setStatus('Only system admin can update roles')
        return
      }
      const roleName = String(editingRole.role || '').trim().toLowerCase()
      const res = await fetch(`http://localhost:8000/roles/${encodeURIComponent(roleName)}`, {
        method: 'PUT',
        headers: {...authHeaders(), 'Content-Type':'application/json'},
        body: JSON.stringify(editingRole)
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus('Role updated')
      setEditingRole(null)
      await fetchRoles()
    }catch(e){ setStatus('Update role failed: ' + e.message) }
  }

  async function removeRolePolicy(roleName){
    try{
      if(String(user?.role || '').toLowerCase() !== 'system_admin'){
        setStatus('Only system admin can delete roles')
        return
      }
      if(!window.confirm(`Delete role ${roleName}?`)) return
      const res = await fetch(`http://localhost:8000/roles/${encodeURIComponent(roleName)}`, {
        method: 'DELETE',
        headers: authHeaders(),
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus('Role deleted')
      await fetchRoles()
    }catch(e){ setStatus('Delete role failed: ' + e.message) }
  }

  useEffect(()=>{
    if(page==='settings' && currentUserChurchId){
      setEditingAppName(appName)
      fetchLocalCodes()
    }
  }, [page, currentUserChurchId])

  // ----- Admin: users -----
  const [usersList, setUsersList] = useState([])
  const [usersMaxRows, setUsersMaxRows] = useState(30)
  const [usersSearchField, setUsersSearchField] = useState('all')
  const [usersSearchText, setUsersSearchText] = useState('')
  const [editingUser, setEditingUser] = useState(null)
  const [userEditForm, setUserEditForm] = useState({ username:'', first_name:'', middle_name:'', last_name:'', email:'', phone:'', role:'uploader', church:'' })
  const [resetUser, setResetUser] = useState(null)
  const [resetPassword, setResetPassword] = useState('')
  const [showOwnPasswordForm, setShowOwnPasswordForm] = useState(false)
  const [ownCurrentPassword, setOwnCurrentPassword] = useState('')
  const [ownNewPassword, setOwnNewPassword] = useState('')
  const [ownConfirmPassword, setOwnConfirmPassword] = useState('')
  async function fetchUsers(){
    try{
      const res = await fetch('http://localhost:8000/users', { headers: authHeaders() })
      const data = await res.json().catch(()=> ({}))
      if(!res.ok){
        const detail = (data && data.detail) ? String(data.detail) : `HTTP ${res.status}`
        if(res.status === 401){
          logout()
          throw new Error('Session expired. Please login again. ' + detail)
        }
        throw new Error(detail)
      }
      setUsersList(data)
    }catch(e){ setStatus('Fetch users failed: '+e.message) }
  }
  async function createUser(username, password, church, role, firstName='', middleName='', lastName='', email='', phone=''){
    try{
      if(!canAccessChurch(church)){ denyRestrictedChurchAccess('user creation'); return }
      const policyMsg = passwordPolicyMessage(password)
      if(policyMsg){ setStatus('Create user failed: ' + policyMsg); return }
      const body = {
        username,
        password,
        church,
        role,
        first_name: firstName || null,
        middle_name: middleName || null,
        last_name: lastName || null,
        email: email || null,
        phone: phone || null,
      }
      const res = await fetch('http://localhost:8000/users/register', { method:'POST', headers: authHeaders({'Content-Type':'application/json'}), body: JSON.stringify(body) })
      const data = await res.json()
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setStatus('User created')
      await fetchUsers()
    }catch(e){ setStatus('Create user failed: '+e.message) }
  }

  function beginEditUser(u){
    if(!u) return
    setEditingUser(u)
    setUserEditForm({
      username: u.username || '',
      first_name: u.first_name || '',
      middle_name: u.middle_name || '',
      last_name: u.last_name || '',
      email: u.email || '',
      phone: u.phone || '',
      role: u.role || 'uploader',
      church: u.church ?? ''
    })
  }

  async function saveUserEdit(){
    if(!editingUser) return
    try{
      if(!canAccessChurch(userEditForm.church)){ denyRestrictedChurchAccess('user update'); return }
      const payload = {
        username: userEditForm.username,
        first_name: userEditForm.first_name || null,
        middle_name: userEditForm.middle_name || null,
        last_name: userEditForm.last_name || null,
        email: userEditForm.email || null,
        phone: userEditForm.phone || null,
        church: userEditForm.church === '' ? null : Number(userEditForm.church),
        role: userEditForm.role || 'uploader',
        password: ''
      }
      const res = await fetch(`http://localhost:8000/users/${editingUser.id}`, {
        method:'PUT',
        headers: authHeaders({'Content-Type':'application/json'}),
        body: JSON.stringify(payload)
      })
      const data = await res.json()
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setStatus('User updated')
      setEditingUser(null)
      await fetchUsers()
    }catch(e){ setStatus('Update user failed: ' + e.message) }
  }

  async function submitResetPassword(){
    if(!resetUser) return
    try{
      const policyMsg = passwordPolicyMessage(resetPassword)
      if(policyMsg){
        setStatus('Password reset failed: ' + policyMsg)
        return
      }
      if(!canAccessChurch(resetUser.church)){ denyRestrictedChurchAccess('password reset'); return }
      const payload = {
        username: resetUser.username,
        first_name: resetUser.first_name || null,
        middle_name: resetUser.middle_name || null,
        last_name: resetUser.last_name || null,
        email: resetUser.email || null,
        phone: resetUser.phone || null,
        church: resetUser.church ?? null,
        role: resetUser.role || 'uploader',
        password: resetPassword
      }
      const res = await fetch(`http://localhost:8000/users/${resetUser.id}`, {
        method:'PUT',
        headers: authHeaders({'Content-Type':'application/json'}),
        body: JSON.stringify(payload)
      })
      const data = await res.json()
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setStatus(`Password reset for ${resetUser.username}`)
      setResetUser(null)
      setResetPassword('')
      await fetchUsers()
    }catch(e){ setStatus('Password reset failed: ' + e.message) }
  }

  async function submitOwnPasswordReset(){
    try{
      if(!ownCurrentPassword || !ownNewPassword){
        setStatus('Password change failed: current and new password are required')
        return
      }
      const policyMsg = passwordPolicyMessage(ownNewPassword)
      if(policyMsg){
        setStatus('Password change failed: ' + policyMsg)
        return
      }
      if(ownNewPassword !== ownConfirmPassword){
        setStatus('Password change failed: password confirmation does not match')
        return
      }
      const res = await fetch('http://localhost:8000/users/me/reset-password', {
        method:'POST',
        headers: authHeaders({'Content-Type':'application/json'}),
        body: JSON.stringify({ current_password: ownCurrentPassword, new_password: ownNewPassword })
      })
      const data = await res.json().catch(()=> ({}))
      if(!res.ok) throw new Error(data.detail || JSON.stringify(data) || `HTTP ${res.status}`)
      setStatus('Password changed successfully')
      setShowOwnPasswordForm(false)
      setOwnCurrentPassword('')
      setOwnNewPassword('')
      setOwnConfirmPassword('')
    }catch(e){
      setStatus('Password change failed: ' + e.message)
    }
  }

  // ----- Members (list + update) -----
  const [members, setMembers] = useState([])
  const [membersMaxRows, setMembersMaxRows] = useState(30)
  const [membersSearchField, setMembersSearchField] = useState('all')
  const [membersSearchText, setMembersSearchText] = useState('')
  const [membersQ, setMembersQ] = useState('')
  async function fetchMembers(q=''){
    try{
      const url = q? `http://localhost:8000/members?q=${encodeURIComponent(q)}` : 'http://localhost:8000/members'
      const res = await fetch(url, { headers: authHeaders() })
      const data = await res.json()
      setMembers(data)
      if(data && data.length && (!membersFields || membersFields.length===0)){
        // derive fields from first row, exclude internal timestamps
        const keys = Object.keys(data[0]).filter(k=> k !== 'created_at' && k !== 'id')
        setMembersFields(keys)
      }
    }catch(e){ setStatus('Failed to load members: '+e.message) }
  }
  async function updateMember(id, payload){
    try{
      const res = await fetch(`http://localhost:8000/members/${id}`, { method:'PUT', headers: authHeaders({'Content-Type':'application/json'}), body: JSON.stringify(payload) })
      if(!res.ok) throw new Error('Failed')
      setStatus('Member updated')
      await fetchMembers()
    }catch(e){ setStatus('Update failed: '+e.message) }
  }

  // Members UI local state (was missing and caused runtime errors)
  const [showMemberForm, setShowMemberForm] = useState(false)
  const [editingMember, setEditingMember] = useState(null)
  const [memberForm, setMemberForm] = useState({})
  const [membersFields, setMembersFields] = useState([])
  const [showAllMemberCols, setShowAllMemberCols] = useState(false)
  const [showCollectionsTable, setShowCollectionsTable] = useState(false)
  const [membersCollections, setMembersCollections] = useState([])
  const [membersCollectionsFields, setMembersCollectionsFields] = useState([])
  const [showCollectionsInReports, setShowCollectionsInReports] = useState(false)
  const [mcFilterText, setMcFilterText] = useState('')
  const [mcSearchField, setMcSearchField] = useState('all')
  const [mcFilterCode, setMcFilterCode] = useState('')
  const [mcFrom, setMcFrom] = useState('')
  const [mcTo, setMcTo] = useState('')
  const [mcSortKey, setMcSortKey] = useState('id')
  const [mcSortDir, setMcSortDir] = useState('desc')
  const [mcPage, setMcPage] = useState(1)
  const [mcPageSize, setMcPageSize] = useState(30)
  const [editingCollection, setEditingCollection] = useState(null)

  // Load members when navigating to Members page
  useEffect(()=>{
    if(page==='members') fetchMembers('')
  }, [page])

  // Load members_collections when navigating to the Members Collections page
  useEffect(()=>{
    if(page==='members_collections'){
      setStatus('Loading collections...')
      fetchMembersCollections().then(()=>{
        setStatus('')
        if(!membersCollections || membersCollections.length===0) setStatus('No collection rows found')
      }).catch(()=>{})
    }
  }, [page])

  // Global error handlers to avoid uncaught errors causing dev overlay crashes
  useEffect(()=>{
    const onErr = (e)=>{
      console.error('Global error', e);
      try{ setStatus('Error: '+ (e && e.message ? e.message : String(e)) ) }catch(_){ }
    }
    const onRej = (e)=>{
      console.error('Unhandled rejection', e);
      try{ setStatus('Unhandled rejection: '+ (e && e.reason ? (e.reason.message||String(e.reason)) : String(e)) ) }catch(_){ }
    }
    window.addEventListener('error', onErr)
    window.addEventListener('unhandledrejection', onRej)
    return ()=>{ window.removeEventListener('error', onErr); window.removeEventListener('unhandledrejection', onRej) }
  }, [])

  // ----- Collections / Upload UI (reuse existing flow) -----
  // We'll keep the original upload flow components but scoped under Collections page
  // State for upload flow:
  const [step, setStep] = useState(1)
  const [file, setFile] = useState(null)
  const [headers, setHeaders] = useState([])
  const [preview, setPreview] = useState([])
  const [fullPreview, setFullPreview] = useState([])
  const [mapping, setMapping] = useState({})
  const [s1Column, setS1Column] = useState(null)
  const [collectionCodes, setCollectionCodes] = useState([])
  const [membersLocal, setMembersLocal] = useState([])
  const [selectedChurch, setSelectedChurch] = useState(churches[0]?.id || '')
  const [selectedDate, setSelectedDate] = useState('')
  const [uploaderName, setUploaderName] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [apiKeyStatus, setApiKeyStatus] = useState('')
  const [mappedPreview, setMappedPreview] = useState([])
  const [validationErrors, setValidationErrors] = useState([])

  useEffect(()=>{ fetchCodes(); fetchMembersLocal() }, [])
  useEffect(()=>{ if(step===3||step===2) recomputeMappedPreview() }, [step, mapping, fullPreview, selectedChurch, selectedDate, uploaderName])

  // Helper: human-friendly label for a column using `collectionCodes` mapping
  function labelForColumn(col){
    if(!col) return '';
    const found = (collectionCodes||[]).find(c=> c.column_name === col || c.code === col);
    if(found && found.code) return found.code;
    const human = { s1: 'Sno', s2: 'Date', s3: 'Serial', s4: 'Name' }[col];
    return human || col;
  }

  // Helper: display value for a table cell; for `collection_code` show the column_name when available
  function displayCellValue(k, row){
    if(!row) return '';
    if(k === 'collection_code'){
      const raw = row.collection_code;
      const found = (collectionCodes||[]).find(c=> c.column_name === raw || c.code === raw);
      if(found) return found.column_name || found.code || raw;
      return raw || '';
    }
    if(k === 'verified'){
      return row && row.__verified? 'Yes':'No';
    }
    const v = row[k];
    return v===null||v===undefined? '': String(v);
  }

  function authFetch(url, opts={}){
    const h = opts.headers? {...opts.headers} : {}
    if(token) h['Authorization'] = `Bearer ${token}`
    return fetch(url, {...opts, headers: h})
  }

  async function fetchCodes(){ try{ const res = await fetch('http://localhost:8000/collection_codes'); const data = await res.json(); setCollectionCodes(data) }catch(e){} }
  async function fetchMembersLocal(){ try{ const res = await fetch('http://localhost:8000/members'); const data = await res.json(); setMembersLocal(data) }catch(e){} }

  async function fetchMembersCollections(){
    try{
      const res = await authFetch('http://localhost:8000/reports/members_collections')
      const data = await res.json()
      if(!res.ok){ setStatus('Failed to load collections: '+(data.detail||JSON.stringify(data))); setMembersCollections([]); return }
      // attach helper metadata per row (verified flag and suggestions)
      const enriched = (Array.isArray(data)? data : []).map(r=> ({...r, __verified: false, __suggestions: []}))
      setMembersCollections(enriched)
      if(enriched && enriched.length && (!membersCollectionsFields || membersCollectionsFields.length===0)){
        const keys = Object.keys(enriched[0]).filter(k=> k !== 'added_at' && !k.startsWith('__'))
        if(!keys.includes('verified')) keys.push('verified')
        setMembersCollectionsFields(keys)
      }
    }catch(e){ setStatus('Failed to load collections: '+e.message); setMembersCollections([]) }
  }

  // Simple Levenshtein distance for fuzzy matching
  function levenshtein(a, b){
    if(!a) a=''; if(!b) b=''; a = String(a).toLowerCase(); b = String(b).toLowerCase();
    const m = a.length, n = b.length; const dp = Array.from({length: m+1}, ()=> Array(n+1).fill(0));
    for(let i=0;i<=m;i++) dp[i][0]=i; for(let j=0;j<=n;j++) dp[0][j]=j;
    for(let i=1;i<=m;i++) for(let j=1;j<=n;j++) dp[i][j] = a[i-1]===b[j-1]? dp[i-1][j-1] : Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1]+1);
    return dp[m][n];
  }

  // Verify names in current membersCollections: fetch /members?q=name and compute fuzzy suggestions
  async function verifyNames(){
    if(!Array.isArray(membersCollections) || membersCollections.length===0) return;
    setStatus('Verifying names...')
    const updated = [];
    for(const r of membersCollections){
      const name = r.s4 || r.s4 || r['s4'] || '';
      if(!name){ updated.push({...r, __verified:false, __suggestions: []}); continue }
      try{
        const res = await fetch(`http://localhost:8000/members?q=${encodeURIComponent(name)}`)
        const cand = await res.json();
        const scored = (Array.isArray(cand)? cand: []).map(c=> ({...c, _score: levenshtein(name, c.MEMBER_NAME || c.MEMBER_NAME || '')})).sort((a,b)=> a._score - b._score).slice(0,6)
        const verified = scored.length>0 && scored[0]._score <= Math.max(2, Math.floor((name.length||1)*0.25));
        updated.push({...r, __verified: verified, __suggestions: scored})
      }catch(e){ updated.push({...r, __verified:false, __suggestions: []}) }
    }
    setMembersCollections(updated)
    setStatus('Verification complete')
  }

  function getOrderedCodes(codes){ if(!codes||!codes.length) return []; const filtered = codes.filter(c=> c.code && String(c.code).toUpperCase() !== 'UNUSED'); const priority = ['Sno','Jina','Zaka','Sadaka']; const out=[]; const used=new Set(); priority.forEach(p=>{ const found = filtered.find(c=> String(c.code).toLowerCase()===p.toLowerCase()); if(found){ out.push(found); used.add(found.column_name) } }); const rest = filtered.filter(c=> !used.has(c.column_name)).sort((a,b)=> (a.code||'').toString().localeCompare((b.code||'').toString())); return out.concat(rest) }

  async function uploadFile(){
    if(!file) return;
    const fd = new FormData(); fd.append('batch', file, file.name);
    setStatus('Uploading file...');
    try{
      const headers = {};
      if(apiKey) headers['X-API-KEY']=apiKey;
      const res = await authFetch('http://localhost:8000/upload/headers',{method:'POST', body: fd, headers});
      const data = await res.json();
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
      setHeaders(data.headers);
      setFullPreview(data.full_preview||[]);
      // use full preview as working preview so all rows are editable
      setPreview(data.full_preview||[]);
      setS1Column(data.s1_column||null);
      const m={};
      data.headers.forEach(h=> m[h]= data.suggestions && data.suggestions[h] ? data.suggestions[h] : '');
      setMapping(m);
      recomputeMappedPreview(m);
      setStatus('File uploaded');
      setStep(2);
    }catch(e){ setStatus('Upload failed: '+e.message) }
  }

  function coerceValue(key, val){
    if(val===null||val===undefined||val==='') return null;
    const sString = new Set(['s4','s10','s11','s12','source','collection_code','notes']);
    const sNumeric = new Set(['s3','s5','s6','s7','s8','s9','s13']);
    const sval = String(val).trim();
    if(key==='s2'){ const d=new Date(sval); if(!isNaN(d)) return d.toISOString(); return sval }
    if(key==='s1') return sval;
    if(sString.has(key)) return sval;
    if(sNumeric.has(key) || key.startsWith('c') || key.startsWith('l')){ const n=Number(sval.replace(/[^0-9.\-]/g,'')); return isNaN(n)? null: n }
    return typeof val==='number' ? val : sval;
  }

  function recomputeMappedPreview(mappingToUse = mapping){
    const rows = (fullPreview||[]).map(r=>{
      const out = {collection_code:'import'};
      if(selectedChurch) out.church = selectedChurch;
      if(selectedDate) out.s2 = new Date(selectedDate).toISOString();
      if(uploaderName) out.source = uploaderName;
      Object.keys(r).forEach(h=>{
        const mapped = mappingToUse[h];
        if(mapped){ out[mapped] = coerceValue(mapped, r[h]) }
      });
      try{
        const s2val = out.s2; let s2dt=null;
        if(typeof s2val==='string'){ const d=new Date(s2val); if(!isNaN(d)) s2dt=d }
        else if(s2val instanceof Date) s2dt=s2val;
        const s3val = out.s3; const s3int = s3val!=null? Number(s3val) : null;
        const church_id = out.church || selectedChurch || null;
        if(s2dt && s3int!=null && !out.s1){ const ymd = s2dt.toISOString().slice(0,10).replace(/-/g,''); const cidn = String(church_id||'').padStart(3,'0'); const s3n = String(Number(s3int)).padStart(3,'0'); out.s1 = `${ymd}${cidn}${s3n}` }
      }catch(e){}
      return out
    })
    setMappedPreview(rows);
    setValidationErrors([]);
    return rows
  }

  async function validateRows(rows){ try{ const headers = {'Content-Type':'application/json'}; if(apiKey) headers['X-API-KEY']=apiKey; const res = await authFetch('http://localhost:8000/members_collections/validate',{method:'POST', headers, body: JSON.stringify(rows)}); const data = await res.json(); if(!res.ok){ if(res.status===422 && data && data.detail && data.detail.validation_errors){ if(data.detail.rows) setMappedPreview(data.detail.rows); return data.detail.validation_errors || [] } throw new Error(data.detail||JSON.stringify(data)) } if(data.rows) setMappedPreview(data.rows); return data.validation_errors || [] }catch(err){ console.warn('Validation error', err); return [{error: err.message}] } }

  async function submitMapped(){
    // ensure mappedPreview is up-to-date and try extraction
    const rows = mappedPreview.length? mappedPreview : recomputeMappedPreview(mapping);
    const rows2 = recomputeMappedPreview(mapping, rows);
    // client-side pre-validation for missing required fields
    const pre = preValidateRows(rows2);
    if(pre && pre.length){ setValidationErrors(pre); setStep(4); setStatus('Pre-validation failed'); return }
    setStatus('Validating rows before submit...');
    const val = await validateRows(rows2);
    if(val && val.length){ setStatus('Validation failed'); setValidationErrors(val); setPage('collections'); setStep(4); return }
    setStatus('Submitting mapped rows...');
    try{
      const headers = {'Content-Type':'application/json'}; if(apiKey) headers['X-API-KEY']=apiKey;
      const res = await authFetch('http://localhost:8000/members_collections/bulk',{method:'POST', headers, body: JSON.stringify(rows2)});
      const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
      setStatus(`Inserted ${data.inserted} rows`); setStep(5)
    }catch(err){ setStatus('Submit failed: '+err.message) }
  }

  // ----- Reports -----
  const [reportRows, setReportRows] = useState([])
  const [reportFrom, setReportFrom] = useState('')
  const [reportTo, setReportTo] = useState('')
  const [aggRows, setAggRows] = useState([])
  const [reportMaxRows, setReportMaxRows] = useState(30)
  const [reportCollectionsMaxRows, setReportCollectionsMaxRows] = useState(30)
  const [dashboardStats, setDashboardStats] = useState({ members: null, users: null, loading: false })

  async function fetchDashboardStats(){
    setDashboardStats(prev => ({ ...prev, loading: true }))
    try{
      const [membersRes, usersRes] = await Promise.all([
        authFetch('http://localhost:8000/members'),
        authFetch('http://localhost:8000/users'),
      ])

      const membersData = await membersRes.json().catch(()=>[])
      const usersData = await usersRes.json().catch(()=>[])

      setDashboardStats({
        members: Array.isArray(membersData) ? membersData.length : 0,
        users: usersRes.ok && Array.isArray(usersData) ? usersData.length : null,
        loading: false,
      })
    }catch(e){
      setDashboardStats({ members: null, users: null, loading: false })
      setStatus('Failed to load dashboard: ' + e.message)
    }
  }

  useEffect(()=>{
    if(user && page === 'dashboard') fetchDashboardStats()
  }, [page, user, token])

  async function fetchMembersCollectionReport(){
    try{
      let url = 'http://localhost:8000/reports/members_collections';
      if(reportFrom && reportTo) url += `?start_date=${encodeURIComponent(reportFrom)}&end_date=${encodeURIComponent(reportTo)}`;
      const res = await authFetch(url);
      const data = await res.json();
      if(!res.ok){ setStatus('Report failed: '+(data.detail||data.error||JSON.stringify(data))); setReportRows([]); return }
      if(!Array.isArray(data)){ setStatus('Report returned unexpected response'); setReportRows([]); return }
      setReportRows(data)
    }catch(e){ setStatus('Report failed: '+e.message); setReportRows([]) }
  }

  function aggregateByCollectionCode(){
    const rows = reportRows || [];
    const map = {};
    rows.forEach(r=>{
      const code = r.collection_code || 'unknown';
      if(!map[code]) map[code] = {collection_code: code, count:0, s5:0, s6:0, s7:0};
      map[code].count += 1;
      map[code].s5 += Number(r.s5||0);
      map[code].s6 += Number(r.s6||0);
      map[code].s7 += Number(r.s7||0);
    })
    setAggRows(Object.values(map));
  }

  // ----- Simple helpers for role checks -----
  function isAdmin(){
    if(!user) return false
    const role = String(user.role || '').toLowerCase()
    const uname = String(user.username || '').toLowerCase()
    return role === 'admin' || role === 'system_admin' || uname === 'saypy_admin'
  }
  function isUploader(){ return user && user.role === 'uploader' }

  function formatNumber(value){
    if(value === null || value === undefined || Number.isNaN(Number(value))) return '-'
    return new Intl.NumberFormat().format(Number(value))
  }

  function currentRolePolicy(){
    const role = String(user?.role || '').toLowerCase()
    return (rolesCatalog || []).find(r => String(r.role || '').toLowerCase() === role) || null
  }

  function hasRoleRight(flag, fallback=false){
    if(String(user?.role || '').toLowerCase() === 'system_admin') return true
    const rp = currentRolePolicy()
    if(!rp) return fallback
    return !!rp[flag]
  }

  function displayUserName(u){
    const first = String(u?.first_name || '').trim()
    const last = String(u?.last_name || '').trim()
    const full = [first, last].filter(Boolean).join(' ')
    if(full) return full
    return String(u?.username || '')
  }

  const filteredUsers = (usersList || []).filter((u)=>{
    const q = String(usersSearchText || '').trim().toLowerCase()
    if(!q) return true
    const fullName = [u?.first_name, u?.middle_name, u?.last_name].filter(Boolean).join(' ')
    const churchName = (churches||[]).find(c=> Number(c.id)===Number(u?.church))?.name || u?.church || ''
    const bag = {
      username: u?.username || '',
      name: fullName,
      email: u?.email || '',
      phone: u?.phone || '',
      role: roleLabel(u?.role || ''),
      church: String(churchName || ''),
      all: [u?.id, u?.username, fullName, u?.email, u?.phone, roleLabel(u?.role || ''), churchName].join(' '),
    }
    return String(bag[usersSearchField] ?? bag.all).toLowerCase().includes(q)
  })

  const filteredMembers = (members || []).filter((m)=>{
    const q = String(membersSearchText || '').trim().toLowerCase()
    if(!q) return true
    const churchName = (churches||[]).find(c=> Number(c.id)===Number(m?.church))?.name || m?.church || ''
    const bag = {
      name: m?.MEMBER_NAME || '',
      member_id: m?.MEMBER_ID ?? '',
      phone: m?.PHONE || '',
      church: String(churchName || ''),
      sno: m?.sno ?? '',
      all: [m?.sno, m?.MEMBER_NAME, m?.MEMBER_ID, m?.PHONE, churchName].join(' '),
    }
    return String(bag[membersSearchField] ?? bag.all).toLowerCase().includes(q)
  })

  // If not authenticated, show a focused login screen (no menu)
  if(!user){
    return (
      <div style={{fontFamily:'Arial',padding:18}}>
        <h2>Church Offerings — Login</h2>
        <div style={{marginTop:12}}>
          <input placeholder='username' value={loginUser} onChange={e=>setLoginUser(e.target.value)} />
          <input placeholder='password' type='password' value={loginPass} onChange={e=>setLoginPass(e.target.value)} />
          <button onClick={doLogin}>Login</button>
          <div style={{marginTop:8,color:'#666'}}>{status}</div>
        </div>
      </div>
    )
  }

  return (
    <div style={{fontFamily:'Arial',padding:18}}>
      <header style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
        <h2>{appName}</h2>
        <div>
          <div>
            <strong>{displayUserName(user)}</strong> ({user.role})
            <button style={{marginLeft:8}} onClick={()=> setShowOwnPasswordForm(s=>!s)}>{showOwnPasswordForm ? 'Cancel Password Change' : 'Change Password'}</button>
            <button style={{marginLeft:8}} onClick={logout}>Logout</button>
          </div>
        </div>
      </header>

      {showOwnPasswordForm && (
        <div style={{marginTop:10,border:'1px solid #ddd',padding:10,borderRadius:6}}>
          <h4 style={{marginTop:0}}>Change My Password</h4>
          <div style={{display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
            <input type='password' placeholder='Current password' value={ownCurrentPassword} onChange={e=>setOwnCurrentPassword(e.target.value)} />
            <input type='password' placeholder='New password' value={ownNewPassword} onChange={e=>setOwnNewPassword(e.target.value)} />
            <input type='password' placeholder='Confirm new password' value={ownConfirmPassword} onChange={e=>setOwnConfirmPassword(e.target.value)} />
            <button onClick={submitOwnPasswordReset}>Update Password</button>
          </div>
          <div style={{marginTop:6,fontSize:12,color:'#666'}}>{PASSWORD_HINT}</div>
        </div>
      )}

      <div style={{display:'grid',gridTemplateColumns:'220px 1fr',gap:16,marginTop:12}}>
        <aside style={{border:'1px solid #eee',borderRadius:8,padding:10,alignSelf:'start'}}>
          <div style={{fontWeight:700,marginBottom:10}}>Menu</div>
          <div style={{display:'flex',flexDirection:'column',gap:8}}>
            {hasRoleRight('can_view_dashboard', true) && <button onClick={()=>setPage('dashboard')} style={{textAlign:'left',background:page==='dashboard' ? '#e8f0fe' : '#fff'}}>Dashboard</button>}
            {hasRoleRight('can_manage_collections', true) && <button onClick={()=>setPage('collections')} style={{textAlign:'left',background:page==='collections' ? '#e8f0fe' : '#fff'}}>Collections</button>}
            {hasRoleRight('can_manage_members', true) && <button onClick={()=>setPage('members')} style={{textAlign:'left',background:page==='members' ? '#e8f0fe' : '#fff'}}>Members</button>}
            {hasRoleRight('can_manage_collections', true) && <button onClick={()=>setPage('members_collections')} style={{textAlign:'left',background:page==='members_collections' ? '#e8f0fe' : '#fff'}}>Members Collections</button>}
            {hasRoleRight('can_view_reports', true) && <button onClick={()=>setPage('reports')} style={{textAlign:'left',background:page==='reports' ? '#e8f0fe' : '#fff'}}>Reports</button>}
            {hasRoleRight('can_manage_users', isAdmin()) && <button onClick={()=>{ setPage('admin'); fetchUsers() }} style={{textAlign:'left',background:page==='admin' ? '#e8f0fe' : '#fff'}}>Admin</button>}
            {hasRoleRight('can_manage_settings', isAdmin()) && <button onClick={()=>setPage('settings')} style={{textAlign:'left',background:page==='settings' ? '#e8f0fe' : '#fff'}}>Settings</button>}
          </div>
        </aside>

        <main style={{borderTop:'1px solid #eee', paddingTop:12}}>
          <div style={{marginBottom:12,color:'#666'}}>{status}</div>
        {page==='dashboard' && (
          <div>
            <h3>Dashboard</h3>
            <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit, minmax(220px, 1fr))',gap:12}}>
              <div style={{border:'1px solid #d6e4ff',borderRadius:12,padding:18,background:'#f8fbff',boxShadow:'0 1px 3px rgba(14,30,64,0.08)'}}>
                <div style={{fontSize:12,color:'#48608a',fontWeight:600,letterSpacing:'0.04em'}}>TOTAL MEMBERS</div>
                <div style={{fontSize:42,fontWeight:800,lineHeight:1.1,marginTop:8,color:'#0f2d5c'}}>{dashboardStats.loading ? '...' : formatNumber(dashboardStats.members)}</div>
              </div>
              <div style={{border:'1px solid #d6e4ff',borderRadius:12,padding:18,background:'#f8fbff',boxShadow:'0 1px 3px rgba(14,30,64,0.08)'}}>
                <div style={{fontSize:12,color:'#48608a',fontWeight:600,letterSpacing:'0.04em'}}>TOTAL USERS</div>
                <div style={{fontSize:42,fontWeight:800,lineHeight:1.1,marginTop:8,color:'#0f2d5c'}}>{dashboardStats.loading ? '...' : formatNumber(dashboardStats.users)}</div>
              </div>
            </div>
          </div>
        )}
        {page==='admin' && (
          <div>
            <h3>Admin — Manage Users</h3>
            <div style={{marginBottom:8}}>
              <button onClick={fetchUsers}>Refresh users</button>
            </div>
            <div>
              <h4>Create user</h4>
              <CreateUserForm onCreate={createUser} churches={churches} scopedChurchId={currentUserChurchId} canAccessChurch={canAccessChurch} onRestrictedChurchAttempt={denyRestrictedChurchAccess} roleOptions={roleOptionsForCurrentUser()} roleLabel={roleLabel} />
            </div>
            <div style={{marginTop:12}}>
              <h4>Users</h4>
              <div style={{marginBottom:8,display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                <label>Search</label>
                <select value={usersSearchField} onChange={e=>setUsersSearchField(e.target.value)}>
                  <option value='all'>All fields</option>
                  <option value='username'>Username</option>
                  <option value='name'>Name</option>
                  <option value='email'>Email</option>
                  <option value='phone'>Phone</option>
                  <option value='role'>Role</option>
                  <option value='church'>Church</option>
                </select>
                <input placeholder='Search users...' value={usersSearchText} onChange={e=>setUsersSearchText(e.target.value)} />
                <label>Max rows</label>
                <select value={usersMaxRows} onChange={e=>setUsersMaxRows(Number(e.target.value))}>
                  {[10,30,50,100].map(n=> <option key={n} value={n}>{n}</option>)}
                </select>
                <span style={{color:'#666'}}>Showing {Math.min(filteredUsers.length, usersMaxRows)} of {filteredUsers.length}</span>
              </div>
              <table border={1} cellPadding={6} style={{borderCollapse:'collapse'}}>
                <thead><tr><th>ID</th><th>Username</th><th>Name</th><th>Email</th><th>Phone</th><th>Role</th><th>Church</th><th>Actions</th></tr></thead>
                <tbody>
                  {filteredUsers.slice(0, usersMaxRows).map(u=> {
                    const chName = (churches||[]).find(c=> Number(c.id)===Number(u.church))?.name || u.church
                    const fullName = [u.first_name, u.middle_name, u.last_name].filter(Boolean).join(' ')
                    return (
                      <tr key={u.id}>
                        <td>{u.id}</td>
                        <td>{u.username}</td>
                        <td>{fullName || '-'}</td>
                        <td>{u.email || '-'}</td>
                        <td>{u.phone || '-'}</td>
                        <td>{roleLabel(u.role)}</td>
                        <td>{chName}</td>
                        <td>
                          <button onClick={()=>beginEditUser(u)}>Edit</button>
                          <button style={{marginLeft:8}} onClick={()=>{ setResetUser(u); setResetPassword('') }}>Reset Password</button>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>

              {editingUser && (
                <div style={{marginTop:12,border:'1px solid #ddd',padding:10,borderRadius:6}}>
                  <h4>Edit User: {editingUser.username}</h4>
                  <div style={{display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                    <input value={userEditForm.username} placeholder='username' onChange={e=>setUserEditForm(prev=>({...prev, username:e.target.value}))} />
                    <input value={userEditForm.first_name||''} placeholder='first name' onChange={e=>setUserEditForm(prev=>({...prev, first_name:e.target.value}))} />
                    <input value={userEditForm.middle_name||''} placeholder='middle name' onChange={e=>setUserEditForm(prev=>({...prev, middle_name:e.target.value}))} />
                    <input value={userEditForm.last_name||''} placeholder='last name' onChange={e=>setUserEditForm(prev=>({...prev, last_name:e.target.value}))} />
                    <input value={userEditForm.email||''} placeholder='email' onChange={e=>setUserEditForm(prev=>({...prev, email:e.target.value}))} />
                    <input value={userEditForm.phone||''} placeholder='phone' onChange={e=>setUserEditForm(prev=>({...prev, phone:e.target.value}))} />
                    <select value={userEditForm.role} onChange={e=>setUserEditForm(prev=>({...prev, role:e.target.value}))}>
                      {roleOptionsForCurrentUser().map(r => <option key={r} value={r}>{roleLabel(r)}</option>)}
                    </select>
                    <select value={userEditForm.church ?? ''} onChange={e=>{
                      const v = e.target.value
                      if(!canAccessChurch(v)){ denyRestrictedChurchAccess('user management'); return }
                      setUserEditForm(prev=>({...prev, church:v}))
                    }}>
                      <option value=''>-- church --</option>
                      {(churches||[]).map(ch=> <option key={ch.id} value={ch.id} disabled={!canAccessChurch(ch.id)}>{ch.name}</option>)}
                    </select>
                    <button onClick={saveUserEdit}>Save</button>
                    <button onClick={()=>setEditingUser(null)}>Cancel</button>
                  </div>
                </div>
              )}

              {resetUser && (
                <div style={{marginTop:12,border:'1px solid #ddd',padding:10,borderRadius:6}}>
                  <h4>Reset Password: {resetUser.username}</h4>
                  <div style={{display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                    <input type='password' placeholder='new password' value={resetPassword} onChange={e=>setResetPassword(e.target.value)} />
                    <button onClick={submitResetPassword}>Set New Password</button>
                    <button onClick={()=>{ setResetUser(null); setResetPassword('') }}>Cancel</button>
                  </div>
                  <div style={{marginTop:6,fontSize:12,color:'#666'}}>{PASSWORD_HINT}</div>
                </div>
              )}
            </div>
          </div>
        )}

        {page==='settings' && (
          <div>
            <h3>Settings</h3>
            {!currentUserChurchId && <div style={{backgroundColor:'#fff3cd',padding:10,marginBottom:12,borderRadius:6}}>⚠️ You do not have a church assigned. Only local church admins can access settings.</div>}
            
            <div style={{marginTop:12,border:'1px solid #ddd',padding:10,borderRadius:6}}>
              <h4>Application Name</h4>
              {!showEditAppName ? (
                <div>
                  <p><strong>Current:</strong> {appName}</p>
                  <button onClick={()=>{ setEditingAppName(appName); setShowEditAppName(true); }} disabled={!currentUserChurchId}>Edit</button>
                </div>
              ) : (
                <div style={{display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                  <input type='text' value={editingAppName} onChange={e=>setEditingAppName(e.target.value)} placeholder='Application name' />
                  <button onClick={submitAppName}>Save</button>
                  <button onClick={()=>{ setShowEditAppName(false); setEditingAppName(''); }}>Cancel</button>
                </div>
              )}
            </div>

            <div style={{marginTop:12,border:'1px solid #ddd',padding:10,borderRadius:6}}>
              <h4>Local Collection Codes</h4>
              <p style={{fontSize:12,color:'#666'}}>These codes apply only to your church. Global codes are shared across all churches.</p>
              
              <div style={{marginBottom:12}}>
                <h5>Add New Code</h5>
                <div style={{display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                  <input type='text' value={newCodeColumn} onChange={e=>setNewCodeColumn(e.target.value)} placeholder='Column name (e.g., c21)' disabled={!currentUserChurchId} />
                  <input type='text' value={newCodeLabel} onChange={e=>setNewCodeLabel(e.target.value)} placeholder='Label (e.g., SPECIAL OFFERING)' disabled={!currentUserChurchId} />
                  <button onClick={createLocalCode} disabled={!currentUserChurchId}>Create Code</button>
                </div>
              </div>

              {localCollectionCodes.length > 0 && (
                <div>
                  <h5>Your Codes</h5>
                  <table border={1} cellPadding={6} style={{borderCollapse:'collapse',width:'100%'}}>
                    <thead><tr><th>Column</th><th>Label</th><th>Actions</th></tr></thead>
                    <tbody>
                      {localCollectionCodes.map(code=> (
                        <tr key={code.id}>
                          <td>{code.column_name}</td>
                          <td>{code.code}</td>
                          <td>
                            {editingCodeId !== code.id ? (
                              <>
                                <button onClick={()=>{ setEditingCodeId(code.id); setEditCodeForm({column_name:code.column_name, code:code.code}); }} style={{marginRight:8}}>Edit</button>
                                <button onClick={()=>deleteLocalCode(code.id)}>Delete</button>
                              </>
                            ) : (
                              <>
                                <input type='text' value={editCodeForm.column_name} onChange={e=>setEditCodeForm({...editCodeForm,column_name:e.target.value})} style={{marginRight:4}} />
                                <input type='text' value={editCodeForm.code} onChange={e=>setEditCodeForm({...editCodeForm,code:e.target.value})} style={{marginRight:4}} />
                                <button onClick={updateLocalCode} style={{marginRight:4}}>Save</button>
                                <button onClick={()=>{ setEditingCodeId(null); setEditCodeForm({column_name:'',code:''}); }}>Cancel</button>
                              </>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            <div style={{marginTop:12,border:'1px solid #ddd',padding:10,borderRadius:6}}>
              <h4>Role Policy Grid</h4>
              {String(user?.role || '').toLowerCase() !== 'system_admin' && (
                <p style={{fontSize:12,color:'#666'}}>Local admin can assign existing roles to local church users but cannot define or update role policies.</p>
              )}

              {String(user?.role || '').toLowerCase() === 'system_admin' && (
                <>
                  <div style={{marginBottom:12,padding:10,border:'1px solid #eee',borderRadius:6}}>
                    <h5 style={{marginTop:0}}>Add Role</h5>
                    <div style={{display:'flex',gap:8,flexWrap:'wrap',alignItems:'center'}}>
                      <input placeholder='role key (e.g. auditor)' value={newRole.role} onChange={e=>setNewRole(prev=>({...prev, role:e.target.value}))} />
                      <input placeholder='display name' value={newRole.display_name} onChange={e=>setNewRole(prev=>({...prev, display_name:e.target.value}))} />
                      <label><input type='checkbox' checked={!!newRole.can_view_dashboard} onChange={e=>setNewRole(prev=>({...prev, can_view_dashboard:e.target.checked}))} /> Dashboard</label>
                      <label><input type='checkbox' checked={!!newRole.can_manage_users} onChange={e=>setNewRole(prev=>({...prev, can_manage_users:e.target.checked}))} /> Users</label>
                      <label><input type='checkbox' checked={!!newRole.can_manage_members} onChange={e=>setNewRole(prev=>({...prev, can_manage_members:e.target.checked}))} /> Members</label>
                      <label><input type='checkbox' checked={!!newRole.can_manage_collections} onChange={e=>setNewRole(prev=>({...prev, can_manage_collections:e.target.checked}))} /> Collections</label>
                      <label><input type='checkbox' checked={!!newRole.can_view_reports} onChange={e=>setNewRole(prev=>({...prev, can_view_reports:e.target.checked}))} /> Reports</label>
                      <label><input type='checkbox' checked={!!newRole.can_manage_settings} onChange={e=>setNewRole(prev=>({...prev, can_manage_settings:e.target.checked}))} /> Settings</label>
                      <button onClick={createRolePolicy}>Add Role</button>
                    </div>
                  </div>

                  <div style={{overflow:'auto'}}>
                    <table border={1} cellPadding={6} style={{borderCollapse:'collapse',width:'100%'}}>
                      <thead>
                        <tr>
                          <th>Role</th>
                          <th>Display Name</th>
                          <th>Dashboard</th>
                          <th>Users</th>
                          <th>Members</th>
                          <th>Collections</th>
                          <th>Reports</th>
                          <th>Settings</th>
                          <th>Manage Roles</th>
                          <th>Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(rolesCatalog || []).map(r=> (
                          <tr key={r.role}>
                            <td>{r.role}</td>
                            <td>{editingRole && editingRole.role===r.role ? <input value={editingRole.display_name || ''} onChange={e=>setEditingRole(prev=>({...prev, display_name:e.target.value}))} /> : (r.display_name || r.role)}</td>
                            <td>{editingRole && editingRole.role===r.role ? <input type='checkbox' checked={!!editingRole.can_view_dashboard} onChange={e=>setEditingRole(prev=>({...prev, can_view_dashboard:e.target.checked}))} /> : (r.can_view_dashboard ? 'Yes':'No')}</td>
                            <td>{editingRole && editingRole.role===r.role ? <input type='checkbox' checked={!!editingRole.can_manage_users} onChange={e=>setEditingRole(prev=>({...prev, can_manage_users:e.target.checked}))} /> : (r.can_manage_users ? 'Yes':'No')}</td>
                            <td>{editingRole && editingRole.role===r.role ? <input type='checkbox' checked={!!editingRole.can_manage_members} onChange={e=>setEditingRole(prev=>({...prev, can_manage_members:e.target.checked}))} /> : (r.can_manage_members ? 'Yes':'No')}</td>
                            <td>{editingRole && editingRole.role===r.role ? <input type='checkbox' checked={!!editingRole.can_manage_collections} onChange={e=>setEditingRole(prev=>({...prev, can_manage_collections:e.target.checked}))} /> : (r.can_manage_collections ? 'Yes':'No')}</td>
                            <td>{editingRole && editingRole.role===r.role ? <input type='checkbox' checked={!!editingRole.can_view_reports} onChange={e=>setEditingRole(prev=>({...prev, can_view_reports:e.target.checked}))} /> : (r.can_view_reports ? 'Yes':'No')}</td>
                            <td>{editingRole && editingRole.role===r.role ? <input type='checkbox' checked={!!editingRole.can_manage_settings} onChange={e=>setEditingRole(prev=>({...prev, can_manage_settings:e.target.checked}))} /> : (r.can_manage_settings ? 'Yes':'No')}</td>
                            <td>{editingRole && editingRole.role===r.role ? <input type='checkbox' checked={!!editingRole.can_manage_roles} onChange={e=>setEditingRole(prev=>({...prev, can_manage_roles:e.target.checked}))} /> : (r.can_manage_roles ? 'Yes':'No')}</td>
                            <td>
                              {editingRole && editingRole.role===r.role ? (
                                <>
                                  <button onClick={saveRolePolicy}>Save</button>
                                  <button style={{marginLeft:6}} onClick={()=>setEditingRole(null)}>Cancel</button>
                                </>
                              ) : (
                                <>
                                  <button onClick={()=>setEditingRole({...r})}>Edit</button>
                                  {!r.system_protected && <button style={{marginLeft:6}} onClick={()=>removeRolePolicy(r.role)}>Delete</button>}
                                </>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          </div>
        )}

        {page==='members' && (
          <div>
            <h3>Members</h3>
            <div style={{marginBottom:8,display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
              <input placeholder='Search members' value={membersQ} onChange={e=>setMembersQ(e.target.value)} />
              <button onClick={()=>fetchMembers(membersQ)}>Search</button>
              <label style={{marginLeft:12}}>Filter</label>
              <select value={membersSearchField} onChange={e=>setMembersSearchField(e.target.value)}>
                <option value='all'>All fields</option>
                <option value='name'>Name</option>
                <option value='member_id'>Member ID</option>
                <option value='phone'>Phone</option>
                <option value='church'>Church</option>
                <option value='sno'>SNO</option>
              </select>
              <input placeholder='Filter loaded members...' value={membersSearchText} onChange={e=>setMembersSearchText(e.target.value)} />
              <button style={{marginLeft:8}} onClick={()=>{ setEditingMember(null); setMemberForm({}); setShowMemberForm(true); }}>New Member</button>
              <label style={{marginLeft:12}}><input type='checkbox' checked={showAllMemberCols} onChange={e=>setShowAllMemberCols(e.target.checked)} /> Show all columns</label>
              <label style={{marginLeft:12}}>Max rows</label>
              <select value={membersMaxRows} onChange={e=>setMembersMaxRows(Number(e.target.value))}>
                {[10,30,50,100].map(n=> <option key={n} value={n}>{n}</option>)}
              </select>
              <span style={{color:'#666'}}>Showing {Math.min(filteredMembers.length, membersMaxRows)} of {filteredMembers.length}</span>
            </div>

            <div style={{maxHeight:400, overflow:'auto'}}>
              <table style={{width:'100%', borderCollapse:'collapse'}}>
                <thead>
                  <tr>
                    {(showAllMemberCols? (membersFields || []) : ['sno','MEMBER_NAME','MEMBER_ID','PHONE','church']).map(h=> <th key={h}>{h}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {filteredMembers.slice(0, membersMaxRows).map(m=> (
                    <tr key={m.id} onClick={()=>{ setEditingMember(m); setMemberForm({...m}); setShowMemberForm(true); }} style={{cursor:'pointer'}}>
                      {(showAllMemberCols? (membersFields || []).map(h=> <td key={h}>{m[h]!==null&&m[h]!==undefined? String(m[h]): ''}</td>) : [m.sno, m.MEMBER_NAME, m.MEMBER_ID, m.PHONE, m.church].map((v,i)=> <td key={i}>{v||''}</td>))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {showMemberForm && (
              <div style={{marginTop:12,border:'1px solid #ddd',padding:8}}>
                <h4>{editingMember? 'Edit Member' : 'New Member'}</h4>
                <div style={{display:'flex',gap:8,flexWrap:'wrap'}}>
                  {(membersFields && membersFields.length? membersFields : Object.keys(memberForm||{})).map(key=>{
                    if(key==='id' || key==='created_at') return null
                    const val = memberForm[key]===undefined? '': memberForm[key]
                    if(key==='church'){
                      return (
                        <select key={key} value={val||''} onChange={e=>{
                          const selected = e.target.value
                          if(!canAccessChurch(selected)){ denyRestrictedChurchAccess('member data'); return }
                          setMemberForm(prev=>({...prev,[key]: selected}))
                        }}>
                          <option value=''>-- church --</option>
                          {churches.map(c=> <option key={c.id} value={c.id} disabled={!canAccessChurch(c.id)}>{c.name}</option>)}
                        </select>
                      )
                    }
                    const isNumber = typeof val === 'number' || key.toLowerCase().includes('id') || key.toLowerCase().includes('sno') || key.toLowerCase().includes('pledge')
                    return (
                      <input key={key} placeholder={key} value={val||''} type={isNumber? 'number':'text'} onChange={e=>setMemberForm(prev=>({...prev,[key]: isNumber? (e.target.value===''? null: Number(e.target.value)) : e.target.value }))} />
                    )
                  })}
                </div>
                <div style={{marginTop:8}}>
                  <button onClick={async ()=>{
                    try{
                      if(editingMember){
                        const res = await authFetch(`http://localhost:8000/members/${editingMember.id}`,{method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify(memberForm)});
                        const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
                        setStatus('Member updated');
                      }else{
                        const res = await authFetch('http://localhost:8000/members',{method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(memberForm)});
                        const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
                        setStatus('Member created');
                      }
                      setShowMemberForm(false);
                      setEditingMember(null);
                      setMemberForm({});
                      await fetchMembers('');
                    }catch(e){ setStatus('Save failed: '+e.message) }
                  }}>Save</button>
                  <button onClick={()=>{ setShowMemberForm(false); setEditingMember(null); setMemberForm({}) }} style={{marginLeft:8}}>Cancel</button>
                </div>
              </div>
            )}

            {showCollectionsTable && (
              <div style={{marginTop:12}}>
                <h4>Members Collections</h4>
                <div style={{maxHeight:300, overflow:'auto'}}>
                  <table style={{width:'100%', borderCollapse:'collapse'}}>
                    <thead>
                      <tr>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12) : ['id','collection_code','member_id','church','s1','s2','s3','s4','s5']).map(c=> <th key={c}>{labelForColumn(c)}</th>)}</tr>
                    </thead>
                    <tbody>
                      {membersCollections.map(r=> (
                        <tr key={r.id}><td>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12).map(k=> displayCellValue(k, r)) : [r.id,r.collection_code,r.member_id,r.church,r.s1,r.s2,r.s3,r.s4,r.s5].map(v=> v)).map((v,i)=><span key={i}>{v!==null&&v!==undefined?String(v):''}{i < 8? ' | ' : ''}</span>)}</td></tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {page==='collections' && (
          <div>
            <h3>Collections — Upload & Manage</h3>
            <div style={{marginBottom:8}}>
              <button onClick={()=>{ setShowCollectionsTable(s=>!s); if(!showCollectionsTable) fetchMembersCollections() }}>{showCollectionsTable? 'Hide Collections':'Show Collections'}</button>
            </div>
            <CollectionsUpload
              token={token}
              authFetch={authFetch}
              collectionCodes={collectionCodes}
              churches={churches}
              fetchCodes={fetchCodes}
              user={user}
              labelForColumn={labelForColumn}
              scopedChurchId={currentUserChurchId}
              onRestrictedChurchAttempt={denyRestrictedChurchAccess}
            />
            {showCollectionsTable && (
              <div style={{marginTop:12}}>
                <h4>Members Collections</h4>
                <div style={{maxHeight:300, overflow:'auto'}}>
                  <table style={{width:'100%', borderCollapse:'collapse'}}>
                    <thead>
                      <tr>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12) : ['id','collection_code','member_id','church','s1','s2','s3','s4','s5']).map(c=> <th key={c}>{labelForColumn(c)}</th>)}</tr>
                    </thead>
                    <tbody>
                      {membersCollections.map(r=> (
                        <tr key={r.id}><td>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12).map(k=> displayCellValue(k, r)) : [r.id,r.collection_code,r.member_id,r.church,r.s1,r.s2,r.s3,r.s4,r.s5].map(v=> v)).map((v,i)=><span key={i}>{v!==null&&v!==undefined?String(v):''}{i < 8? ' | ' : ''}</span>)}</td></tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {page==='members_collections' && (
          <div>
            <h3>Members Collections</h3>
            <div style={{marginBottom:8, display:'flex', gap:8, alignItems:'center', flexWrap:'wrap'}}>
              <button onClick={fetchMembersCollections}>Refresh</button>
              <label>Search in</label>
              <select value={mcSearchField} onChange={e=>{ setMcSearchField(e.target.value); setMcPage(1) }}>
                <option value='all'>All fields</option>
                <option value='collection_code'>Collection Code</option>
                <option value='s4'>Member Name</option>
                <option value='member_id'>Member ID</option>
                <option value='s1'>S1</option>
                <option value='church'>Church</option>
              </select>
              <input placeholder='Search' value={mcFilterText} onChange={e=>{ setMcFilterText(e.target.value); setMcPage(1) }} />
              <select value={mcFilterCode} onChange={e=>{ setMcFilterCode(e.target.value); setMcPage(1) }}>
                <option value=''>-- all codes --</option>
                {(collectionCodes||[]).map(c=> <option key={c.column_name} value={c.column_name}>{c.code || c.column_name}</option>)}
              </select>
              <button onClick={()=> verifyNames()}>Verify Names</button>
              <label>From</label>
              <input type='date' value={mcFrom} onChange={e=>{ setMcFrom(e.target.value); setMcPage(1) }} />
              <label>To</label>
              <input type='date' value={mcTo} onChange={e=>{ setMcTo(e.target.value); setMcPage(1) }} />
              <label>Max rows</label>
              <select value={mcPageSize} onChange={e=>{ setMcPageSize(Number(e.target.value)); setMcPage(1) }}>
                {[10,30,50,100].map(n=> <option key={n} value={n}>{n}</option>)}
              </select>
            </div>

            <div className='table-wrap' style={{maxHeight:500}}>
              <table>
                <thead>
                  <tr>
                    {(membersCollectionsFields.length? membersCollectionsFields : ['id','collection_code','member_id','church','s1','s2','s3','s4','s5']).map(c=> (
                      <th key={c} style={{cursor:'pointer'}} onClick={()=>{
                        if(mcSortKey===c) setMcSortDir(d=> d==='asc'? 'desc':'asc'); else { setMcSortKey(c); setMcSortDir('asc') }
                      }}>{labelForColumn(c)} {mcSortKey===c? (mcSortDir==='asc'? '▲':'▼') : ''}</th>
                    ))}
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    try{
                      // apply client-side filters/sort/paging
                      let rows = Array.isArray(membersCollections) ? membersCollections : [];
                      if(mcFilterCode) rows = rows.filter(r=> {
                        try{ return r.collection_code === mcFilterCode || r.collection_code === (collectionCodes.find(c=>c.column_name===mcFilterCode)?.code) }
                        catch(e){ return false }
                      })
                      if(mcFilterText) rows = rows.filter(r => {
                        try{
                          const q = mcFilterText.toLowerCase()
                          if(mcSearchField === 'all') return Object.values(r).join(' ').toLowerCase().includes(q)
                          return String(r?.[mcSearchField] ?? '').toLowerCase().includes(q)
                        }catch(e){ return false }
                      })
                      if(mcFrom){ const dfrom = new Date(mcFrom); rows = rows.filter(r=> { try{ return r.s2 && new Date(r.s2) >= dfrom }catch(e){return false} }) }
                      if(mcTo){ const dto = new Date(mcTo); rows = rows.filter(r=> { try{ return r.s2 && new Date(r.s2) <= dto }catch(e){return false} }) }
                      // sort
                      rows = rows.slice().sort((a,b)=>{
                        const va = a && a[mcSortKey]; const vb = b && b[mcSortKey];
                        if(va==null && vb==null) return 0; if(va==null) return mcSortDir==='asc'? -1:1; if(vb==null) return mcSortDir==='asc'? 1:-1;
                        if(typeof va === 'number' && typeof vb === 'number') return mcSortDir==='asc'? va-vb : vb-va;
                        return mcSortDir==='asc'? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
                      })
                      const total = rows.length;
                      const start = (mcPage-1)*mcPageSize; const end = start + mcPageSize
                      const pageRows = rows.slice(start, end)
                      return pageRows.map((r,idx)=> (
                        <tr key={r && (r.id||idx)}>
                          {(membersCollectionsFields.length? membersCollectionsFields : ['id','collection_code','member_id','church','s1','s2','s3','s4','s5']).map(k=> {
                            if(k === 'verified') return <td key={k}>{r && r.__verified? 'Yes':'No'}</td>
                            return <td key={k}>{displayCellValue(k, r)}</td>
                          })}
                          <td>
                            <button onClick={()=>{ setEditingCollection({...r}); }}>Edit</button>
                          </td>
                        </tr>
                      ))
                    }catch(err){
                      console.error('Render error in members_collections table', err)
                      return <tr><td colSpan={ (membersCollectionsFields.length? membersCollectionsFields.length:10) + 1 }>Error rendering collections: {String(err.message||err)}</td></tr>
                    }
                  })()}
                </tbody>
              </table>
            </div>
            <div style={{marginTop:8}}>
              <button onClick={()=> setMcPage(p=> Math.max(1,p-1))}>Prev</button>
              <span style={{margin:'0 8px'}}>Page {mcPage}</span>
              <button onClick={()=> setMcPage(p=> p+1)}>Next</button>
            </div>

            {/* Edit modal */}
            {editingCollection && (
              <div className='card' style={{marginTop:12}}>
                <h4>Edit Collection #{editingCollection.id}</h4>
                <div className='form-row'>
                  { (membersCollectionsFields.length? membersCollectionsFields.slice(0,12) : ['collection_code','member_id','church','s1','s2','s3','s4','s5']).map(k=>{
                    if(k==='id' || k==='added_at') return null
                    const val = editingCollection[k]===null||editingCollection[k]===undefined? '': editingCollection[k]
                    if(k==='s2'){
                      const v = val? new Date(val).toISOString().slice(0,10) : ''
                      return <input key={k} type='date' value={v} onChange={e=> setEditingCollection(prev=>({...prev, [k]: e.target.value}))} />
                    }
                    if(k==='church'){
                      return (
                        <div key={k} style={{display:'flex',flexDirection:'column'}}>
                          <select value={val||''} onChange={e=>{
                            const newChurch = e.target.value || null
                            if(!canAccessChurch(newChurch)){ denyRestrictedChurchAccess('members_collection data'); return }
                            setEditingCollection(prev=>{
                              const next = {...prev, [k]: newChurch}
                              try{
                                const s2 = next.s2 ? new Date(next.s2) : (next.s2===undefined? null : new Date(next.s2))
                                const s3 = next.s3!=null? Number(next.s3) : null
                                const churchId = newChurch || next.church || null
                                if(s2 && s3!=null && churchId){
                                  const ymd = s2.toISOString().slice(0,10).replace(/-/g,'')
                                  const cidn = String(Number(churchId)).padStart(3,'0')
                                  const s3n = String(Number(s3)).padStart(3,'0')
                                  next.s1 = `${ymd}${cidn}${s3n}`
                                }
                              }catch(e){}
                              return next
                            })
                          }}>
                            <option value=''>-- church --</option>
                            {churches.map(c=> <option key={c.id} value={c.id} disabled={!canAccessChurch(c.id)}>{c.name}</option>)}
                          </select>
                          <div style={{fontSize:12,color:'#666'}}>{editingCollection && editingCollection.__verified? 'Name verified in members':'Not verified'}</div>
                        </div>
                      )
                    }
                    if(k==='s4'){
                      return (
                        <div key={k} style={{display:'flex',flexDirection:'column'}}>
                          <input placeholder={k} value={val||''} onChange={e=> setEditingCollection(prev=>({...prev, [k]: e.target.value}))} />
                          <div style={{marginTop:6}}>
                            <button onClick={async ()=>{
                              try{
                                const nm = editingCollection.s4 || ''
                                if(!nm) return setStatus('No name to search')
                                const res = await fetch(`http://localhost:8000/members?q=${encodeURIComponent(nm)}`)
                                const cand = await res.json()
                                const scored = (Array.isArray(cand)? cand: []).map(c=> ({...c, _score: levenshtein(nm, c.MEMBER_NAME || '')})).sort((a,b)=> a._score - b._score).slice(0,6)
                                setEditingCollection(prev=>({...prev, __suggestions: scored, __verified: scored.length>0 && scored[0]._score <= Math.max(2, Math.floor((nm.length||1)*0.25)) }))
                              }catch(e){ setStatus('Suggestion lookup failed: '+e.message) }
                            }}>Find suggestions</button>
                          </div>
                          {editingCollection.__suggestions && editingCollection.__suggestions.length>0 && (
                            <div style={{marginTop:6}}>
                              <div style={{fontSize:12,color:'#333'}}>Suggestions:</div>
                              {editingCollection.__suggestions.map(s=> (
                                <div key={s.id} style={{display:'flex',gap:8,alignItems:'center'}}>
                                  <div style={{flex:1}}>{s.MEMBER_NAME} (ID: {s.MEMBER_ID || s.id})</div>
                                  <button onClick={()=>{
                                    // map suggestion to this collection row: set member_id and name
                                    setEditingCollection(prev=>({...prev, member_id: s.MEMBER_ID || s.id, s4: s.MEMBER_NAME, __verified: true}))
                                  }}>Map</button>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      )
                    }
                    return <input key={k} placeholder={k} value={val||''} onChange={e=> setEditingCollection(prev=>({...prev, [k]: e.target.value}))} />
                  })}
                </div>
                <div style={{marginTop:8}}>
                  <button onClick={async ()=>{
                    try{
                      const id = editingCollection.id
                      const payload = {...editingCollection}; delete payload.id
                      const res = await authFetch(`http://localhost:8000/members_collection/${id}`, {method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)})
                      const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
                      setStatus('Saved')
                      setEditingCollection(null)
                      await fetchMembersCollections()
                    }catch(e){ setStatus('Save failed: '+e.message) }
                  }}>Save</button>
                  <button style={{marginLeft:8}} onClick={()=> setEditingCollection(null)}>Cancel</button>
                </div>
              </div>
            )}
          </div>
        )}

        {page==='reports' && (
          <div>
            <h3>Reports</h3>
            <div>
              <h4>Members Collection Report</h4>
              <div style={{display:'flex', gap:8, alignItems:'center', flexWrap:'wrap'}}>
                <label>From:</label>
                <input type='date' value={reportFrom} onChange={e=>setReportFrom(e.target.value)} />
                <label>To:</label>
                <input type='date' value={reportTo} onChange={e=>setReportTo(e.target.value)} />
                <button onClick={fetchMembersCollectionReport}>Run Report</button>
                <button style={{marginLeft:8}} onClick={async ()=>{ setShowCollectionsInReports(s=>!s); if(!showCollectionsInReports) await fetchMembersCollections(); }}>{showCollectionsInReports? 'Hide Collections':'View Collections'}</button>
                <label style={{marginLeft:8}}>Max rows</label>
                <select value={reportMaxRows} onChange={e=>setReportMaxRows(Number(e.target.value))}>
                  {[10,30,50,100].map(n=> <option key={n} value={n}>{n}</option>)}
                </select>
                <span style={{color:'#666'}}>Showing {Math.min(reportRows.length, reportMaxRows)} of {reportRows.length}</span>
              </div>
              <div style={{maxHeight:400, overflow:'auto', marginTop:8}}>
                <table style={{width:'100%', borderCollapse:'collapse'}}>
                  <thead>
                    <tr>
                      {reportRows[0] ? Object.keys(reportRows[0]).map(k=> <th key={k}>{labelForColumn(k)}</th>) : <th>No rows</th>}
                    </tr>
                  </thead>
                  <tbody>
                    {(() => {
                      try{
                        return (Array.isArray(reportRows)? reportRows : []).slice(0, reportMaxRows).map((r,idx)=> (
                          <tr key={idx}>{Object.keys(r||{}).map(k=> <td key={k} style={{padding:6}}>{String((r&&r[k])||'')}</td>)}</tr>
                        ))
                      }catch(e){
                        console.error('Render error in reports table', e)
                        return <tr><td>Error rendering report: {String(e.message||e)}</td></tr>
                      }
                    })()}
                  </tbody>
                </table>
              </div>
              <div style={{marginTop:8}}>
                <button onClick={aggregateByCollectionCode}>Aggregate by Collection Code</button>
                {aggRows && aggRows.length>0 && (
                  <div style={{marginTop:8}}>
                    <h4>Aggregated by Collection Code</h4>
                    <table style={{width:'100%',borderCollapse:'collapse'}}>
                      <thead><tr><th>Collection</th><th>Count</th><th>Total s5</th><th>Total s6</th><th>Total s7</th></tr></thead>
                      <tbody>{aggRows.map(a=> <tr key={a.collection_code}><td>{a.collection_code}</td><td>{a.count}</td><td>{a.s5}</td><td>{a.s6}</td><td>{a.s7}</td></tr>)}</tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>
              {showCollectionsInReports && (
                <div style={{marginTop:12}}>
                  <h4>Members Collections (Reports)</h4>
                  <div style={{marginBottom:8,display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                    <label>Max rows</label>
                    <select value={reportCollectionsMaxRows} onChange={e=>setReportCollectionsMaxRows(Number(e.target.value))}>
                      {[10,30,50,100].map(n=> <option key={n} value={n}>{n}</option>)}
                    </select>
                    <span style={{color:'#666'}}>Showing {Math.min((membersCollections||[]).length, reportCollectionsMaxRows)} of {(membersCollections||[]).length}</span>
                  </div>
                  <div style={{maxHeight:300, overflow:'auto'}}>
                    <table style={{width:'100%', borderCollapse:'collapse'}}>
                      <thead>
                        <tr>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12) : ['id','collection_code','member_id','church','s1','s2','s3','s4','s5']).map(c=> <th key={c}>{labelForColumn(c)}</th>)}</tr>
                      </thead>
                      <tbody>
                        {(() => {
                          try{
                            const rows = Array.isArray(membersCollections)? membersCollections : [];
                            if(rows.length === 0) return <tr><td colSpan={(membersCollectionsFields.length? membersCollectionsFields.slice(0,12).length:9)}>No collection rows</td></tr>
                            return rows.slice(0, reportCollectionsMaxRows).map(r=> (
                              <tr key={r && r.id}><td>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12).map(k=> displayCellValue(k, r)) : [r.id,r.collection_code,r.member_id,r.church,r.s1,r.s2,r.s3,r.s4,r.s5].map(v=> v)).map((v,i)=><span key={i}>{v!==null&&v!==undefined?String(v):''}{i < 8? ' | ' : ''}</span>)}</td></tr>
                            ))
                          }catch(e){ console.error('Error rendering report collections', e); return <tr><td>Error rendering collections: {String(e.message||e)}</td></tr> }
                        })()}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
          </div>
        )}
        </main>
      </div>
    </div>
  )
}

// ----- Subcomponents -----
function CreateUserForm({onCreate, churches, scopedChurchId, canAccessChurch, onRestrictedChurchAttempt, roleOptions = ['uploader'], roleLabel = (r)=>String(r)}){
  const [u, setU] = useState('')
  const [p, setP] = useState('')
  const [firstName, setFirstName] = useState('')
  const [middleName, setMiddleName] = useState('')
  const [lastName, setLastName] = useState('')
  const [email, setEmail] = useState('')
  const [phone, setPhone] = useState('')
  const [c, setC] = useState(churches[0]?.id || null)
  const [r, setR] = useState((roleOptions && roleOptions.length ? roleOptions[0] : 'uploader'))
  const passwordHint = 'Password: minimum 8 characters, include at least 1 letter, 1 number, and 1 special character.'
  useEffect(()=>{
    if(scopedChurchId !== null && scopedChurchId !== undefined && !Number.isNaN(Number(scopedChurchId))){
      setC(String(scopedChurchId))
    }
  }, [scopedChurchId])
  return (
    <div>
      <div style={{display:'flex', gap:8, alignItems:'center', flexWrap:'wrap'}}>
        <input placeholder='username' value={u} onChange={e=>setU(e.target.value)} />
        <input placeholder='password' value={p} onChange={e=>setP(e.target.value)} />
        <input placeholder='first name' value={firstName} onChange={e=>setFirstName(e.target.value)} />
        <input placeholder='middle name' value={middleName} onChange={e=>setMiddleName(e.target.value)} />
        <input placeholder='last name' value={lastName} onChange={e=>setLastName(e.target.value)} />
        <input placeholder='email' value={email} onChange={e=>setEmail(e.target.value)} />
        <input placeholder='phone' value={phone} onChange={e=>setPhone(e.target.value)} />
        <select value={c||''} onChange={e=>{
          const val = e.target.value
          if(typeof canAccessChurch === 'function' && !canAccessChurch(val)){
            if(typeof onRestrictedChurchAttempt === 'function') onRestrictedChurchAttempt('user creation')
            return
          }
          setC(val)
        }}>
          <option value=''>-- church --</option>
          {churches.map(ch=> <option key={ch.id} value={ch.id} disabled={typeof canAccessChurch === 'function' ? !canAccessChurch(ch.id) : false}>{ch.name}</option>)}
        </select>
        <select value={r} onChange={e=>setR(e.target.value)}>
          {(roleOptions && roleOptions.length ? roleOptions : ['uploader']).map(opt => <option key={opt} value={opt}>{roleLabel(opt)}</option>)}
        </select>
        <button onClick={()=> onCreate(u,p,c,r,firstName,middleName,lastName,email,phone)}>Create</button>
      </div>
      <div style={{marginTop:6,fontSize:12,color:'#666'}}>{passwordHint}</div>
    </div>
  )
}

function CollectionsUpload({token, authFetch, collectionCodes, churches, fetchCodes, user, labelForColumn, scopedChurchId, onRestrictedChurchAttempt}){
  // simplified copy of the prior upload UI kept local to this component scope
  const [step, setStep] = useState(1)
  const [file, setFile] = useState(null)
  const [headers, setHeaders] = useState([])
  const [preview, setPreview] = useState([])
  const [fullPreview, setFullPreview] = useState([])
  const [mapping, setMapping] = useState({})
  const [s1Column, setS1Column] = useState(null)
  const [selectedChurch, setSelectedChurch] = useState(churches[0]?.id || '')
  const [selectedDate, setSelectedDate] = useState('')
  const [uploaderName, setUploaderName] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [apiKeyStatus, setApiKeyStatus] = useState('')
  const [mappedPreview, setMappedPreview] = useState([])
  const [validationErrors, setValidationErrors] = useState([])
  const [promptState, setPromptState] = useState({ open: false, level: 'info', message: '' })
  const [copiedPrompt, setCopiedPrompt] = useState(false)

  // total steps (1=file/date/church, 2=mapping, 3=preview/edit, 4=fix, 5=done)
  const totalSteps = 5

  useEffect(()=>{ fetchCodes() }, [])
  useEffect(()=>{ if(user && !uploaderName) setUploaderName(user.username) }, [user])
  useEffect(()=>{
    if(scopedChurchId !== null && scopedChurchId !== undefined && !Number.isNaN(Number(scopedChurchId))){
      setSelectedChurch(String(scopedChurchId))
    }
  }, [scopedChurchId])

  function canAccessChurch(churchId){
    if(churchId === null || churchId === undefined || churchId === '') return true
    if(scopedChurchId === null || scopedChurchId === undefined || Number.isNaN(Number(scopedChurchId))) return true
    return Number(churchId) === Number(scopedChurchId)
  }

  function handleRestrictedChurch(context){
    if(typeof onRestrictedChurchAttempt === 'function') onRestrictedChurchAttempt(context)
    showPrompt('error', `Restricted information: you cannot access ${context} for another church`)
  }

  function showPrompt(level, message){
    setCopiedPrompt(false)
    setPromptState({ open: true, level: level || 'info', message: String(message || '') })
  }

  async function copyPromptMessage(){
    try{
      if(promptState && promptState.message){
        await navigator.clipboard.writeText(promptState.message)
        setCopiedPrompt(true)
      }
    }catch(e){
      setCopiedPrompt(false)
    }
  }

  function coerceValue(key, val){ if(val===null||val===undefined||val==='') return null; if(typeof val==='number' && key!=='s1') return val; const sval = String(val).trim(); if(key==='s2'){ const d=new Date(sval); if(!isNaN(d)) return d.toISOString(); return sval } if(key==='s1'){ return sval } const sNumeric = new Set(['s3','s5','s6','s7','s8','s9','s13']); if(sNumeric.has(key) || key.startsWith('c') || key.startsWith('l')){ const n=Number(sval.replace(/[^0-9.\-]/g,'')); return isNaN(n)? null: n } return sval }

  async function uploadFile(){
    if(!file) return;
    const fd = new FormData(); fd.append('batch', file, file.name);
    try{
      const headers = {};
      const res = await authFetch('http://localhost:8000/upload/headers',{method:'POST', body: fd, headers});
      const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
      setHeaders(data.headers);
      setFullPreview(data.full_preview||[]);
      // prefer server preview when non-empty, otherwise use full_preview
      const initialPreview = (data.preview && data.preview.length) ? data.preview : (data.full_preview||[]);
      setPreview(initialPreview);
      setS1Column(data.s1_column||null);

      // Build initial mapping: prefer server suggestions, else try to match by collectionCodes.code
      const m = {};
      data.headers.forEach(h=>{
        const suggested = data.suggestions && data.suggestions[h] ? data.suggestions[h] : '';
        if(suggested){ m[h]=suggested; return }
        // try to match header text to a collectionCodes.code (case-insensitive)
        const found = (collectionCodes||[]).find(c=> {
          if(!c || !c.code) return false
          const code = String(c.code||'').toLowerCase();
          const hh = String(h||'').toLowerCase();
          return code === hh || hh.includes(code) || code.includes(hh)
        })
        if(found) m[h] = found.column_name
        else m[h] = ''
      })

      setMapping(m);
      // compute mapped preview immediately using the rows returned by the server
      recomputeMappedPreview(m, initialPreview);
      setStep(2)
    }catch(e){ showPrompt('error', 'Upload failed: ' + (e && e.message ? e.message : String(e))) }
  }

  function recomputeMappedPreview(mappingToUse = mapping, rowsSource = null){
    const rowsSrc = rowsSource || preview || fullPreview || [];
    const rows = (rowsSrc||[]).map((r, idx)=>{
      const out = {collection_code:'import'};
      if(selectedChurch) out.church = selectedChurch;
      if(selectedDate) out.s2 = new Date(selectedDate).toISOString();
      if(uploaderName) out.source = uploaderName;
      Object.keys(r).forEach(h=>{
        const mapped = mappingToUse[h];
        if(mapped){ out[mapped] = coerceValue(mapped, r[h]) }
      })
      try{
        const s2val = out.s2; let s2dt=null;
        if(typeof s2val==='string'){ const d=new Date(s2val); if(!isNaN(d)) s2dt=d }
        else if(s2val instanceof Date) s2dt=s2val;
        const s3val = out.s3; const s3int = s3val!=null? Number(s3val) : null;
        const church_id = out.church || selectedChurch || null;
        if(s2dt && s3int!=null && !out.s1){ const ymd = s2dt.toISOString().slice(0,10).replace(/-/g,''); const cidn = String(church_id||'').padStart(3,'0'); const s3n = String(Number(s3int)).padStart(3,'0'); out.s1 = `${ymd}${cidn}${s3n}` }
        // If s3 missing but s1 is a combined serial, try to extract s3 and church from s1
        if((out.s3===null || out.s3===undefined) && out.s1){
          try{
            const s1s = String(out.s1).replace(/[^0-9]/g,'');
            if(s1s.length >= 14){
              const s3ex = parseInt(s1s.slice(-3),10);
              const churchEx = parseInt(s1s.slice(-6,-3),10);
              if(!isNaN(s3ex)) out.s3 = s3ex;
              if(out.church==null && !isNaN(churchEx)) out.church = churchEx;
            }
          }catch(e){}
        }
        if(out.s3===null || out.s3===undefined || out.s3===''){
          out.s3 = idx + 1;
        }
      }catch(e){}
      return out
    })
    setMappedPreview(rows); setValidationErrors([]); return rows
  }

  function preValidateRows(rows){
    const msgs = [];
    const rs = rows && rows.length? rows : mappedPreview;
    rs.forEach((r, idx)=>{
      const rowNum = idx+1;
      // s2
      if(!r.s2){ msgs.push(`Row ${rowNum}: s2 (date) is missing. Set Date in Step 1 or map a column to s2.`) }
      // church
      if(!r.church){ msgs.push(`Row ${rowNum}: church is missing. Select Church in Step 1 or map a column to church.`) }
      // s3
      const s3val = r.s3;
      const s1val = r.s1;
      if((s3val===null || s3val===undefined || s3val==='') && !(s1val && String(s1val).replace(/[^0-9]/g,'').length >= 14)){
        msgs.push(`Row ${rowNum}: s3 (serial) is missing — map your serial column to s3 or ensure s1 contains the combined serial.`)
      }
      // s4
      if(!r.s4){ msgs.push(`Row ${rowNum}: s4 (name) is missing. Map the name column to s4.`) }
    })
    return msgs;
  }

  function formatValidationErrors(errs){
    if(!errs || !errs.length) return [];
    const out = [];
    errs.forEach(e=>{
      if(typeof e === 'string'){
        out.push(e);
        return;
      }
      const idx = e.index != null ? e.index : '?';
      const items = e.errors || [];
      items.forEach(it=>{
        let loc = it.loc;
        if(Array.isArray(loc)) loc = loc.join('.');
        const msg = it.msg || (it.message || JSON.stringify(it));
        out.push(`Row ${Number(idx)+1}: ${loc} — ${msg}`);
      })
    })
    return out;
  }

  

  async function validateRows(rows){ try{ const headers = {'Content-Type':'application/json'}; const res = await authFetch('http://localhost:8000/members_collections/validate',{method:'POST', headers, body: JSON.stringify(rows)}); const data = await res.json(); if(!res.ok){ if(res.status===422 && data && data.detail && data.detail.validation_errors){ if(data.detail.rows) setMappedPreview(data.detail.rows); return data.detail.validation_errors || [] } throw new Error(data.detail||JSON.stringify(data)) } if(data.rows) setMappedPreview(data.rows); return data.validation_errors || [] }catch(err){ return [{error: err.message}] } }

  async function submitMapped(){ const rows = mappedPreview.length? mappedPreview : recomputeMappedPreview(); const val = await validateRows(rows); if(val && val.length){ setValidationErrors(val); setStep(4); showPrompt('warning', 'Validation errors present. Open Step 4 to review details.'); return } try{ const headers = {'Content-Type':'application/json'}; const res = await authFetch('http://localhost:8000/members_collections/bulk',{method:'POST', headers, body: JSON.stringify(rows)}); const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data)); showPrompt('success', `Inserted ${data.inserted} rows`); setStep(5) }catch(e){ showPrompt('error', 'Submit failed: ' + (e && e.message ? e.message : String(e))) } }

  return (
    <div>
      {promptState.open && (
        <div style={{marginBottom:10, border:'1px solid #d0d7de', borderLeft:`5px solid ${promptState.level==='error' ? '#d1242f' : promptState.level==='warning' ? '#d97706' : promptState.level==='success' ? '#2f7d32' : '#2563eb'}`, background:'#f8fafc', padding:10, borderRadius:6}}>
          <div style={{display:'flex', justifyContent:'space-between', gap:8, alignItems:'center', marginBottom:8}}>
            <strong>{promptState.level==='error' ? 'Error' : promptState.level==='warning' ? 'Warning' : promptState.level==='success' ? 'Success' : 'Info'}</strong>
            <div style={{display:'flex', gap:8}}>
              <button onClick={copyPromptMessage}>Copy message</button>
              <button onClick={()=> setPromptState(prev=> ({...prev, open:false}))}>Close</button>
            </div>
          </div>
          <textarea readOnly value={promptState.message} style={{width:'100%', minHeight:90, fontFamily:'Consolas, monospace', fontSize:13, padding:8, border:'1px solid #cbd5e1', borderRadius:4, background:'#ffffff'}} />
          {copiedPrompt && <div style={{marginTop:6, color:'#2f7d32'}}>Message copied to clipboard.</div>}
        </div>
      )}
      <div style={{marginBottom:8,color:'#333'}}>Step {step} of {totalSteps}</div>
      {step===1 && (
        <div>
          <div>
            <input type='file' onChange={e=>setFile(e.target.files[0])} />
          </div>
          <div style={{marginTop:8}}>
            <label>Date:</label>
            <input type='date' value={selectedDate} onChange={e=>{ setSelectedDate(e.target.value) }} />
            <label style={{marginLeft:8}}>Church:</label>
            <select value={selectedChurch||''} onChange={e=>{
              const nextChurch = e.target.value
              if(!canAccessChurch(nextChurch)){ handleRestrictedChurch('collection upload'); return }
              setSelectedChurch(nextChurch)
            }}>
              <option value=''>-- select --</option>
              {churches.map(c=> <option key={c.id} value={c.id} disabled={!canAccessChurch(c.id)}>{c.name}</option>)}
            </select>
            <label style={{marginLeft:8}}>Uploader name:</label>
            <input value={uploaderName} onChange={e=>setUploaderName(e.target.value)} />
          </div>
          <div style={{marginTop:8}}>
            <button onClick={uploadFile}>Upload & Inspect</button>
          </div>
        </div>
      )}
      {step===2 && (
        <div>
          <div>
            <label>Date:</label>
            <input type='date' value={selectedDate} onChange={e=>{ setSelectedDate(e.target.value); recomputeMappedPreview() }} />
            <label style={{marginLeft:8}}>Church:</label>
            <select value={selectedChurch||''} onChange={e=>{
              const nextChurch = e.target.value
              if(!canAccessChurch(nextChurch)){ handleRestrictedChurch('collection upload'); return }
              setSelectedChurch(nextChurch); recomputeMappedPreview()
            }}>
              <option value=''>-- select --</option>
              {churches.map(c=> <option key={c.id} value={c.id} disabled={!canAccessChurch(c.id)}>{c.name}</option>)}
            </select>
            <label style={{marginLeft:8}}>Uploader name:</label>
            <input value={uploaderName} onChange={e=>{ setUploaderName(e.target.value); recomputeMappedPreview() }} />
          </div>
              <div style={{marginTop:8, maxHeight: 380, overflow: 'auto', border: '1px solid #ddd'}}>
                <table border={1} cellPadding={6} style={{borderCollapse:'collapse', minWidth:'max-content'}}>
              <thead><tr><th>Header</th><th>Map to</th></tr></thead>
              <tbody>{headers.map(h=> (
                <tr key={h}><td>{h}</td><td>
                  <select value={mapping[h]||''} onChange={e=>{ setMapping(prev=> ({...prev, [h]: e.target.value})); recomputeMappedPreview({...mapping, [h]: e.target.value}) }}>
                    <option value=''>-- none --</option>
                    {(collectionCodes||[]).map(c=> <option key={c.column_name} value={c.column_name}>{c.column_name} — {c.code}</option>)}
                  </select>
                </td></tr>
              ))}</tbody>
            </table>
              </div>
              <div style={{marginTop:8, display:'flex', gap:8, alignItems:'center'}}>
                <button onClick={()=>setStep(1)}>Back</button>
                <button onClick={()=>setStep(3)} style={{marginLeft:8}}>Next — Preview</button>
                <button onClick={async ()=>{
                    // build header mappings array
                    const arr = Object.keys(mapping).map(h=> ({ header_name: h, mapped_column: mapping[h] }));
                    try{
                      const res = await authFetch('http://localhost:8000/header_mappings',{method:'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(arr)});
                      const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
                      showPrompt('success', 'Mappings saved');
                    }catch(e){ showPrompt('error', 'Save mapping failed: ' + (e && e.message ? e.message : String(e))) }
                }} style={{marginLeft:8}}>Save mapping</button>
              </div>
        </div>
      )}

      {step===3 && (
        <div>
          <h4>Mapped preview ({mappedPreview.length} rows)</h4>
          <div style={{maxHeight:400, overflowX:'auto', overflowY:'auto', border:'1px solid #ddd'}}>
            <table style={{width:'100%', minWidth:'max-content', borderCollapse:'separate', borderSpacing:0}}>
              <thead><tr>{mappedPreview[0] ? Object.keys(mappedPreview[0]).map(k=> <th key={k} style={{position:'sticky', top:0, background:'#fff', zIndex:2, borderBottom:'1px solid #ddd'}}>{labelForColumn(k)}</th>) : <th style={{position:'sticky', top:0, background:'#fff', zIndex:2, borderBottom:'1px solid #ddd'}}>No rows</th>}</tr></thead>
              <tbody>{mappedPreview.map((r,idx)=> (
                <tr key={idx}>{Object.keys(r).map(k=> <td key={k}><input value={r[k]||''} onChange={e=>{ const v=e.target.value; setMappedPreview(prev=>{ const nxt=[...prev]; nxt[idx] = {...nxt[idx], [k]: v}; return nxt }) }} /></td>)}</tr>
              ))}</tbody>
            </table>
          </div>
          <div style={{marginTop:8}}>
            <button onClick={()=>setStep(2)}>Back</button>
            <button onClick={async ()=>{ await submitMapped() }} style={{marginLeft:8}}>Validate & Submit</button>
          </div>
        </div>
      )}

      {step===4 && (
        <div>
          <h4>Fix validation errors</h4>
          {validationErrors && validationErrors.length ? (
            <ul>
              {formatValidationErrors(validationErrors).map((m,i)=> <li key={i}>{m}</li>)}
            </ul>
          ) : (
            <div>No validation errors</div>
          )}
          <button onClick={()=>setStep(3)}>Back to preview</button>
        </div>
      )}

      {step===5 && (
        <div>
          <h4>Done</h4>
          <button onClick={()=>{ setStep(1); setMappedPreview([]); setPreview([]); setFullPreview([]) }}>Start another</button>
        </div>
      )}
    </div>
  )
}
 