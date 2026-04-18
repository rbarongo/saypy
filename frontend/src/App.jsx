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
  const [systemName, setSystemName] = useState('Church Offerings')
  const [editingSystemName, setEditingSystemName] = useState('')
  const [showEditSystemName, setShowEditSystemName] = useState(false)

  async function fetchSystemName(){
    try{
      const res = await fetch('http://localhost:8000/config')
      const data = await res.json().catch(()=>({}))
      if(res.ok && data.system_name) setSystemName(data.system_name)
    }catch(e){}
  }

  async function submitSystemName(){
    try{
      const res = await fetch('http://localhost:8000/config/system_name', {
        method: 'PUT',
        headers: authHeaders({'Content-Type':'application/json'}),
        body: JSON.stringify({system_name: editingSystemName})
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setSystemName(editingSystemName)
      setShowEditSystemName(false)
      setStatus('System name updated')
    }catch(e){ setStatus('Update system name failed: '+e.message) }
  }

  useEffect(()=>{
    fetchChurches()
    fetchSystemName()
  }, [])

  useEffect(()=>{
    if(user) fetchRoles()
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
  const [newCodeCustomName, setNewCodeCustomName] = useState('')
  const [editingCodeId, setEditingCodeId] = useState(null)
  const [editCodeForm, setEditCodeForm] = useState({column_name:'', code:'', custom_collection_name:''})
  const [showCollectionCodesPanel, setShowCollectionCodesPanel] = useState(false)
  const [collectionCodesMaxRows, setCollectionCodesMaxRows] = useState(30)
  const [gridModeMain, setGridModeMain] = useState('classic')
  const [uploadGridMode, setUploadGridMode] = useState('classic')
  const [mappedPreviewMode, setMappedPreviewMode] = useState('scrollable')
  const [editingRole, setEditingRole] = useState(null)
  const [newRole, setNewRole] = useState({
    role: '',
    display_name: '',
    can_view_dashboard: true,
    can_manage_users: false,
    can_manage_members: false,
    can_manage_collections: false,
    can_manage_members_collections: false,
    can_view_collection_codes: false,
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
        body: JSON.stringify({column_name: newCodeColumn, code: newCodeLabel, custom_collection_name: (newCodeCustomName || null)})
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setNewCodeColumn('')
      setNewCodeLabel('')
      setNewCodeCustomName('')
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
        body: JSON.stringify({column_name: editCodeForm.column_name, code: editCodeForm.code, custom_collection_name: (editCodeForm.custom_collection_name || null)})
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setEditingCodeId(null)
      setEditCodeForm({column_name:'', code:'', custom_collection_name:''})
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
      // Fetch church-scoped codes directly from backend (local + global).
      const res = await fetch(`http://localhost:8000/churches/${currentUserChurchId}/collection_codes`, { headers: authHeaders() })
      const data = await res.json().catch(()=>[])
      if(res.ok){
        const localCodes = data
          .filter(c=> c.church == null || Number(c.church)===Number(currentUserChurchId))
          .sort((a,b)=>{
            const aLocal = Number(a.church)===Number(currentUserChurchId) ? 0 : 1
            const bLocal = Number(b.church)===Number(currentUserChurchId) ? 0 : 1
            if(aLocal !== bLocal) return aLocal - bLocal
            return Number(a.id||0) - Number(b.id||0)
          })
        setLocalCollectionCodes(localCodes)
      } else {
        setStatus('Failed to load collection codes: ' + (data?.detail || `HTTP ${res.status}`))
      }
    }catch(e){ setStatus('Failed to load collection codes: ' + e.message) }
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
        can_manage_members_collections: false,
        can_view_collection_codes: false,
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
    }
  }, [page, currentUserChurchId])

  useEffect(()=>{
    if(page==='collections' && currentUserChurchId && (hasRoleRight('can_manage_collections', true) || hasRoleRight('can_view_collection_codes', true))){
      fetchLocalCodes()
    }
  }, [page, currentUserChurchId, rolesCatalog, user])

  useEffect(()=>{
    if(!user || status !== 'Logged in') return
    const clearLoginStatus = ()=>{
      setStatus(prev => prev === 'Logged in' ? '' : prev)
    }
    window.addEventListener('pointerdown', clearLoginStatus, { once: true })
    window.addEventListener('keydown', clearLoginStatus, { once: true })
    return ()=>{
      window.removeEventListener('pointerdown', clearLoginStatus)
      window.removeEventListener('keydown', clearLoginStatus)
    }
  }, [user, status])

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
  const [membersExtraFilters, setMembersExtraFilters] = useState([])
  const [membersSortKey, setMembersSortKey] = useState('sno')
  const [membersSortDir, setMembersSortDir] = useState('asc')
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
        if(!membersVisibleCols.length){
          const defaults = ['sno','MEMBER_NAME','MEMBER_ID','PHONE','church'].filter(k=> keys.includes(k))
          setMembersVisibleCols(defaults.length ? defaults : keys)
        }
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
  const [showTransferChurchCreator, setShowTransferChurchCreator] = useState(false)
  const [newTransferChurchName, setNewTransferChurchName] = useState('')
  const [membersFields, setMembersFields] = useState([])
  const [showAllMemberCols, setShowAllMemberCols] = useState(false)
  const [membersVisibleCols, setMembersVisibleCols] = useState([])
  const [showMembersColumnPicker, setShowMembersColumnPicker] = useState(false)
  const [membersPickerLeft, setMembersPickerLeft] = useState([])
  const [membersPickerRight, setMembersPickerRight] = useState([])
  const [membersPickerLeftSelected, setMembersPickerLeftSelected] = useState(new Set())
  const [membersPickerRightSelected, setMembersPickerRightSelected] = useState(new Set())
  const [membersCollections, setMembersCollections] = useState([])
  const [membersCollectionsFields, setMembersCollectionsFields] = useState([])
  const [showCollectionsInReports, setShowCollectionsInReports] = useState(false)
  const [mcFilterText, setMcFilterText] = useState('')
  const [mcSearchField, setMcSearchField] = useState('all')
  const [mcFilterCode, setMcFilterCode] = useState('')
  const [mcFilterMemberId, setMcFilterMemberId] = useState('')
  const [mcFilterMemberName, setMcFilterMemberName] = useState('')
  const [mcFilterS1, setMcFilterS1] = useState('')
  const [mcFilterAmountMin, setMcFilterAmountMin] = useState('')
  const [mcFilterAmountMax, setMcFilterAmountMax] = useState('')
  const [mcFrom, setMcFrom] = useState('')
  const [mcTo, setMcTo] = useState('')
  const [mcApplied, setMcApplied] = useState({
    searchField: 'all',
    text: '',
    code: '',
    memberId: '',
    memberName: '',
    s1: '',
    amountMin: '',
    amountMax: '',
    from: '',
    to: '',
  })
  const [mcSortKey, setMcSortKey] = useState('id')
  const [mcSortDir, setMcSortDir] = useState('desc')
  const [mcPage, setMcPage] = useState(1)
  const [mcPageSize, setMcPageSize] = useState(10)
  const [mcVisibleCols, setMcVisibleCols] = useState([])

  const [editingCollection, setEditingCollection] = useState(null)
  const [showMcColumnPicker, setShowMcColumnPicker] = useState(false)
  const [mcPickerLeft, setMcPickerLeft] = useState([])
  const [mcPickerRight, setMcPickerRight] = useState([])
  const [mcPickerLeftSelected, setMcPickerLeftSelected] = useState(new Set())
  const [mcPickerRightSelected, setMcPickerRightSelected] = useState(new Set())
  // Load members when navigating to Members page
  useEffect(()=>{
    if(page==='members') fetchMembers('')
  }, [page])

  useEffect(()=>{
    const prefKey = membersPrefColumnsKey()
    if(!prefKey || !membersFields.length) return
    try{
      const raw = localStorage.getItem(prefKey)
      if(!raw) return
      const parsed = JSON.parse(raw)
      if(Array.isArray(parsed)){
        const allowed = new Set(getMembersAllColumns())
        const cols = parsed.filter(c=> allowed.has(c))
        if(cols.length) setMembersVisibleCols(cols)
      }
    }catch(e){}
  }, [user?.id, user?.username, membersFields.join('|')])

  // Load members_collections when navigating to the Members Collections page
  useEffect(()=>{
    if(page==='members_collections'){
      setMcSortKey('id')
      setMcSortDir('desc')
      setMcPage(1)
      setMcPageSize(10)
      setStatus('Loading collections...')
      fetchMembersCollections({ limit: getDefaultMcFetchLimit(1, 10) }).then(()=>{
        // Keep error/status set by fetchMembersCollections when request fails.
      }).catch(()=>{})
    }
  }, [page, currentUserChurchId, churches.length])

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
  useEffect(()=>{
    if(user){
      const preferredName = displayUserName(user) || String(user?.username || '')
      setUploaderName(preferredName)
    }
  }, [user])

  // Helper: human-friendly label for a column using `collectionCodes` mapping
  function collectionCodeMeta(col){
    if(!col) return null
    const matches = (collectionCodes || []).filter(c=> c.column_name === col || c.code === col)
    if(!matches.length) return null
    const preferred = matches.find(c=> Number(c.church)===Number(currentUserChurchId)) || matches.find(c=> c.church == null) || matches[0]
    return preferred || null
  }

  async function createTransferChurchOption(){
    try{
      const canCreateChurch = String(user?.role || '').toLowerCase() === 'system_admin' || String(user?.username || '').toLowerCase() === 'saypy_admin'
      if(!canCreateChurch){
        setStatus('Only system admin can add new churches')
        return
      }
      const name = String(newTransferChurchName || '').trim()
      if(!name){
        setStatus('Enter church name before adding')
        return
      }
      const res = await authFetch('http://localhost:8000/churches', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ name })
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail || JSON.stringify(data) || 'Failed to create church')
      const created = data?.church
      if(created && created.id){
        setChurches(prev => {
          const exists = (prev || []).some(c => Number(c.id) === Number(created.id))
          if(exists) return prev
          return [...(prev || []), created]
        })
        setMemberForm(prev => ({...prev, TRANSFER_TO_CHURCH: Number(created.id), transfer_to_church: Number(created.id)}))
      }
      setShowTransferChurchCreator(false)
      setNewTransferChurchName('')
      setStatus('Church added and selected for transfer')
    }catch(e){
      setStatus('Create church failed: ' + (e?.message || String(e)))
    }
  }

  function labelForColumn(col){
    if(!col) return '';
    const found = collectionCodeMeta(col)
    if(found){
      return found.custom_collection_name || found.code || found.column_name || col
    }
    const human = { s1: 'Sno', s2: 'Date', s3: 'Serial', s4: 'Name' }[col];
    return human || col;
  }

  function mappedPreviewColumns(){
    const first = mappedPreview[0]
    if(!first) return []
    return Object.keys(first).filter(k=> !['collection_code','church','s2','source'].includes(k))
  }

  function currentChurchName(){
    return (churches || []).find(c=> Number(c.id)===Number(selectedChurch || currentUserChurchId))?.name || String(selectedChurch || currentUserChurchId || '-')
  }

  // Helper: format date to DD-MON-YYYY format (e.g., 26-Apr-2026)
  function formatDateDisplay(dateStr){
    if(!dateStr) return '';
    try{
      const d = new Date(dateStr);
      if(isNaN(d.getTime())) return String(dateStr);
      const day = String(d.getDate()).padStart(2, '0');
      const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
      const month = monthNames[d.getMonth()];
      const year = d.getFullYear();
      return `${day}-${month}-${year}`;
    }catch(e){
      return String(dateStr);
    }
  }

  // Helper: display value for a table cell; for `collection_code` show the column_name when available
  function displayCellValue(k, row){
    if(!row) return '';
    if(k === 's2'){
      // Format date field to DD-MON-YYYY
      return formatDateDisplay(row.s2);
    }
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

  function getDefaultMcFetchLimit(targetPage = mcPage, targetPageSize = mcPageSize){
    const pageNum = Math.max(1, Number(targetPage) || 1)
    const pageSizeNum = Math.max(1, Number(targetPageSize) || 10)
    return Math.max(10, pageNum * pageSizeNum)
  }

  function authFetch(url, opts={}){
    const h = opts.headers? {...opts.headers} : {}
    if(token) h['Authorization'] = `Bearer ${token}`
    return fetch(url, {...opts, headers: h})
  }

  async function fetchCodes(){
    try{
      const res = await authFetch('http://localhost:8000/collection_codes')
      const data = await res.json().catch(()=>[])
      setCollectionCodes(Array.isArray(data) ? data : [])
    }catch(e){
      setCollectionCodes([])
    }
  }
  async function fetchMembersLocal(){
    try{
      const res = await authFetch('http://localhost:8000/members')
      const data = await res.json()
      if(res.ok) setMembersLocal(data)
    }catch(e){}
  }

  async function fetchMembersCollections(criteria = null){
    try{
      const active = criteria || {}
      const params = new URLSearchParams()
      if(active.limit != null) params.set('limit', String(active.limit))
      if(active.search) params.set('search', String(active.search))
      if(active.searchField) params.set('search_field', String(active.searchField))
      if(active.collectionCode) params.set('collection_code', String(active.collectionCode))
      if(active.memberId) params.set('member_id', String(active.memberId))
      if(active.memberName) params.set('member_name', String(active.memberName))
      if(active.s1) params.set('s1', String(active.s1))
      if(active.from) params.set('start_date', String(active.from))
      if(active.to) params.set('end_date', String(active.to))
      if(active.amountMin !== '' && active.amountMin != null) params.set('amount_min', String(active.amountMin))
      if(active.amountMax !== '' && active.amountMax != null) params.set('amount_max', String(active.amountMax))
      const url = params.toString()
        ? `http://localhost:8000/reports/members_collections?${params.toString()}`
        : 'http://localhost:8000/reports/members_collections'
      const res = await authFetch(url)
      const data = await res.json()
      if(!res.ok){
        const msg = String(data?.detail || JSON.stringify(data) || 'Request failed')
        if(res.status === 403){
          setStatus('Not authorized for Members Collections. Ask admin to enable Members Collections right for your role.')
        }else{
          setStatus('Failed to load collections: ' + msg)
        }
        setMembersCollections([])
        return false
      }
      // attach helper metadata per row (verified flag and suggestions)
      const enriched = (Array.isArray(data)? data : []).map(r=> ({...r, __verified: false, __suggestions: []}))
      setMembersCollections(enriched)
      if(!enriched.length){
        setStatus('No collection rows found')
      }else{
        setStatus('')
      }
      if(enriched && enriched.length && (!membersCollectionsFields || membersCollectionsFields.length===0)){
        const keys = Object.keys(enriched[0]).filter(k=> k !== 'added_at' && !k.startsWith('__'))
        if(!keys.includes('verified')) keys.push('verified')
        setMembersCollectionsFields(keys)
        // By default, hide id, church, and collection_code; show all others
        if(!mcVisibleCols.length){
          const defaultVisible = keys.filter(k=> !['id','church','collection_code'].includes(k))
          setMcVisibleCols(defaultVisible.length ? defaultVisible : keys)
        }
      }
      return enriched.length
    }catch(e){ setStatus('Failed to load collections: '+e.message); setMembersCollections([]); return 0 }
  }

  function getMcAllColumns(){
    const fallback = ['id','collection_code','member_id','church','s1','s2','s3','s4','s5','verified']
    return (membersCollectionsFields && membersCollectionsFields.length) ? membersCollectionsFields : fallback
  }

  function getMcDisplayColumns(){
    const all = getMcAllColumns()
    if(!mcVisibleCols.length) return all
    const cols = mcVisibleCols.filter(c => all.includes(c))
    return cols.length ? cols : all
  }

  function rowMatchesCurrentChurch(row){
    if(currentUserChurchId === null || currentUserChurchId === undefined || Number.isNaN(Number(currentUserChurchId))) return true
    const rowChurchRaw = row?.church
    const rowChurchText = String(rowChurchRaw ?? '').trim().toLowerCase()
    if(Number(rowChurchRaw) === Number(currentUserChurchId)) return true
    const idInText = String(rowChurchRaw ?? '').match(/\d+/)
    if(idInText && Number(idInText[0]) === Number(currentUserChurchId)) return true
    const currentChurchName = String((churches || []).find(c=> Number(c.id)===Number(currentUserChurchId))?.name || '').trim().toLowerCase()
    if(currentChurchName){
      if(rowChurchText === currentChurchName) return true
      if(rowChurchText.includes(currentChurchName)) return true
    }
    return false
  }

  function hasActiveMcFilters(){
    return [
      mcApplied.text,
      mcApplied.code,
      mcApplied.memberId,
      mcApplied.memberName,
      mcApplied.s1,
      mcApplied.amountMin,
      mcApplied.amountMax,
      mcApplied.from,
      mcApplied.to,
    ].some(v => String(v ?? '').trim() !== '')
  }

  function getFilteredMembersCollectionsRows(){
    let rows = Array.isArray(membersCollections) ? membersCollections : []
    rows = rows.filter(r => rowMatchesCurrentChurch(r))
    const activeFilters = hasActiveMcFilters()
    rows = rows.slice().sort((a,b)=>{
      if(!activeFilters){
        const ad = a?.added_at ? new Date(a.added_at).getTime() : NaN
        const bd = b?.added_at ? new Date(b.added_at).getTime() : NaN
        if(!Number.isNaN(ad) && !Number.isNaN(bd) && ad !== bd) return bd - ad
        return Number(b?.id ?? 0) - Number(a?.id ?? 0)
      }
      const va = a && a[mcSortKey]
      const vb = b && b[mcSortKey]
      if(va==null && vb==null) return 0
      if(va==null) return mcSortDir==='asc' ? -1 : 1
      if(vb==null) return mcSortDir==='asc' ? 1 : -1
      if(typeof va === 'number' && typeof vb === 'number') return mcSortDir==='asc' ? va-vb : vb-va
      return mcSortDir==='asc' ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va))
    })
    return activeFilters ? rows : rows.slice(0, 10)
  }

  function openMcColumnPicker(){
    const all = getMcAllColumns()
    const current = getMcDisplayColumns()
    const available = all.filter(c=> !current.includes(c))
    setMcPickerLeft(available)
    setMcPickerRight(current)
    setMcPickerLeftSelected(new Set())
    setMcPickerRightSelected(new Set())
    setShowMcColumnPicker(true)
  }

  function moveMcColumnsToRight(){
    const selected = Array.from(mcPickerLeftSelected)
    if(!selected.length) return
    const nextLeft = mcPickerLeft.filter(c=> !selected.includes(c))
    const nextRight = [...mcPickerRight, ...selected.filter(c=> !mcPickerRight.includes(c))]
    setMcPickerLeft(nextLeft)
    setMcPickerRight(nextRight)
    setMcPickerLeftSelected(new Set())
  }

  function moveMcColumnsToLeft(){
    const selected = Array.from(mcPickerRightSelected)
    if(!selected.length) return
    const nextRight = mcPickerRight.filter(c=> !selected.includes(c))
    const nextLeft = [...mcPickerLeft, ...selected.filter(c=> !mcPickerLeft.includes(c))]
    setMcPickerLeft(nextLeft)
    setMcPickerRight(nextRight)
    setMcPickerRightSelected(new Set())
  }

  function moveMcColumnUp(){
    const selectedRight = Array.from(mcPickerRightSelected)
    if(selectedRight.length !== 1) return
    const col = selectedRight[0]
    const idx = mcPickerRight.findIndex(c => c === col)
    if(idx <= 0) return
    const nextRight = [...mcPickerRight]
    const swap = nextRight[idx - 1]
    nextRight[idx - 1] = nextRight[idx]
    nextRight[idx] = swap
    setMcPickerRight(nextRight)
    setMcPickerRightSelected(new Set([col]))
  }

  function moveMcColumnDown(){
    const selectedRight = Array.from(mcPickerRightSelected)
    if(selectedRight.length !== 1) return
    const col = selectedRight[0]
    const idx = mcPickerRight.findIndex(c => c === col)
    if(idx >= mcPickerRight.length - 1) return
    const nextRight = [...mcPickerRight]
    const swap = nextRight[idx + 1]
    nextRight[idx + 1] = nextRight[idx]
    nextRight[idx] = swap
    setMcPickerRight(nextRight)
    setMcPickerRightSelected(new Set([col]))
  }

  function saveMcColumns(){
    setMcVisibleCols(mcPickerRight)
    setShowMcColumnPicker(false)
  }
  function escapeHtml(value){
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;')
  }

  function membersPrefUserKey(){
    return String(user?.id || user?.username || '').trim()
  }

  function membersPrefColumnsKey(){
    const k = membersPrefUserKey()
    return k ? `saypy.members.visibleCols.${k}` : ''
  }

  function memberLabelForColumn(col){
    if(!col) return ''
    const found = collectionCodeMeta(col)
    if(found){
      return found.custom_collection_name || found.code || found.column_name || col
    }
    const map = {
      sno: 'S/N',
      MEMBER_NAME: 'Member Name',
      MEMBER_ID: 'Member ID',
      PHONE: 'Phone',
      church: 'Church',
      STATUS: 'Member Status',
      TRANSFER_TO_CHURCH: 'Transfer To Church',
      TRANSFER_DATE: 'Transfer Date',
      STATUS_UPDATED_AT: 'Status Updated',
      transfer_to_church: 'Transfer To Church',
      transfer_date: 'Transfer Date',
      status_updated_at: 'Status Updated',
    }
    return map[col] || col
  }

  function getMembersAllColumns(){
    return (membersFields && membersFields.length) ? membersFields : ['sno','MEMBER_NAME','MEMBER_ID','PHONE','church']
  }

  function getMembersDisplayColumns(){
    const all = getMembersAllColumns()
    if(!membersVisibleCols.length){
      return ['sno','MEMBER_NAME','MEMBER_ID','PHONE','church'].filter(c=> all.includes(c))
    }
    const cols = membersVisibleCols.filter(c=> all.includes(c))
    return cols.length ? cols : all
  }

  function memberDisplayCellValue(k, row){
    if(!row) return ''
    if(k === 'church'){
      return (churches||[]).find(c=> Number(c.id)===Number(row?.church))?.name || row?.church || ''
    }
    const v = row[k]
    return v===null||v===undefined ? '' : String(v)
  }

  function membersFilterableFields(){
    const all = getMembersAllColumns()
    const extras = ['church_name']
    return Array.from(new Set([...all, ...extras]))
  }

  function membersFieldLabel(field){
    if(field === 'church_name') return 'Church Name'
    return memberLabelForColumn(field)
  }

  function membersFieldValue(row, field){
    if(field === 'church_name'){
      return (churches||[]).find(c=> Number(c.id)===Number(row?.church))?.name || ''
    }
    return row?.[field]
  }

  function addMembersFilter(){
    const firstField = membersFilterableFields()[0] || 'MEMBER_NAME'
    setMembersExtraFilters(prev => [...prev, { id: Date.now() + Math.random(), field: firstField, op: 'contains', value: '' }])
  }

  function updateMembersFilter(id, patch){
    setMembersExtraFilters(prev => prev.map(f => f.id === id ? { ...f, ...patch } : f))
  }

  function removeMembersFilter(id){
    setMembersExtraFilters(prev => prev.filter(f => f.id !== id))
  }

  function clearMembersFilters(){
    setMembersExtraFilters([])
  }

  function openMembersColumnPicker(){
    const all = getMembersAllColumns()
    const current = getMembersDisplayColumns()
    setMembersPickerLeft(all.filter(c=> !current.includes(c)))
    setMembersPickerRight(current)
    setMembersPickerLeftSelected(new Set())
    setMembersPickerRightSelected(new Set())
    setShowMembersColumnPicker(true)
  }

  function moveMembersColumnsToRight(){
    const selected = Array.from(membersPickerLeftSelected)
    if(!selected.length) return
    setMembersPickerLeft(membersPickerLeft.filter(c=> !selected.includes(c)))
    setMembersPickerRight([...membersPickerRight, ...selected.filter(c=> !membersPickerRight.includes(c))])
    setMembersPickerLeftSelected(new Set())
  }

  function moveMembersColumnsToLeft(){
    const selected = Array.from(membersPickerRightSelected)
    if(!selected.length) return
    setMembersPickerRight(membersPickerRight.filter(c=> !selected.includes(c)))
    setMembersPickerLeft([...membersPickerLeft, ...selected.filter(c=> !membersPickerLeft.includes(c))])
    setMembersPickerRightSelected(new Set())
  }

  function moveMembersColumnUp(){
    const selected = Array.from(membersPickerRightSelected)
    if(selected.length !== 1) return
    const col = selected[0]
    const idx = membersPickerRight.findIndex(c=> c===col)
    if(idx <= 0) return
    const next = [...membersPickerRight]
    const swap = next[idx-1]
    next[idx-1] = next[idx]
    next[idx] = swap
    setMembersPickerRight(next)
    setMembersPickerRightSelected(new Set([col]))
  }

  function moveMembersColumnDown(){
    const selected = Array.from(membersPickerRightSelected)
    if(selected.length !== 1) return
    const col = selected[0]
    const idx = membersPickerRight.findIndex(c=> c===col)
    if(idx < 0 || idx >= membersPickerRight.length - 1) return
    const next = [...membersPickerRight]
    const swap = next[idx+1]
    next[idx+1] = next[idx]
    next[idx] = swap
    setMembersPickerRight(next)
    setMembersPickerRightSelected(new Set([col]))
  }

  function applyMembersColumnSelection(){
    setMembersVisibleCols(membersPickerRight)
    setShowMembersColumnPicker(false)
  }

  function saveMembersColumnSelection(){
    const prefKey = membersPrefColumnsKey()
    if(!prefKey){ setStatus('Could not save members columns for this user'); return }
    try{
      localStorage.setItem(prefKey, JSON.stringify(getMembersDisplayColumns()))
      setStatus('Members column selection saved for this user')
    }catch(e){
      setStatus('Failed to save members column selection: ' + (e?.message || String(e)))
    }
  }

  function exportMembersExcel(){
    try{
      const cols = getMembersDisplayColumns()
      const rows = filteredMembers.slice(0, membersMaxRows)
      const head = cols.map(c=>`<th>${escapeHtml(memberLabelForColumn(c))}</th>`).join('')
      const body = rows.map(r=> `<tr>${cols.map(c=> `<td>${escapeHtml(memberDisplayCellValue(c, r))}</td>`).join('')}</tr>`).join('')
      const table = `<table border="1"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`
      const html = `<!doctype html><html><head><meta charset="utf-8" /></head><body>${table}</body></html>`
      const blob = new Blob([html], { type: 'application/vnd.ms-excel' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      const stamp = new Date().toISOString().slice(0,19).replace(/[T:]/g,'-')
      a.href = url
      a.download = `members_${currentUserChurchId || 'all'}_${stamp}.xls`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    }catch(e){ setStatus('Export Members Excel failed: ' + (e?.message || String(e))) }
  }

  function exportMembersPdf(){
    try{
      const cols = getMembersDisplayColumns()
      const rows = filteredMembers.slice(0, membersMaxRows)
      const head = cols.map(c=>`<th style="border:1px solid #ccc;padding:6px;text-align:left">${escapeHtml(memberLabelForColumn(c))}</th>`).join('')
      const body = rows.map(r=> `<tr>${cols.map(c=> `<td style="border:1px solid #ddd;padding:6px">${escapeHtml(memberDisplayCellValue(c, r))}</td>`).join('')}</tr>`).join('')
      const win = window.open('', '_blank')
      if(!win){ setStatus('Pop-up blocked. Allow pop-ups to export PDF.'); return }
      win.document.write(`<!doctype html><html><head><title>Members</title><style>body{font-family:Arial,sans-serif;padding:12px}table{border-collapse:collapse;width:100%;font-size:12px}h3{margin:0 0 10px 0}</style></head><body><h3>Members (Church ${escapeHtml(currentUserChurchId || '-')})</h3><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></body></html>`)
      win.document.close()
      win.focus()
      win.print()
    }catch(e){ setStatus('Export Members PDF failed: ' + (e?.message || String(e))) }
  }

  function exportMembersCollectionsExcel(){
    try{
      const cols = getMcDisplayColumns()
      const rows = getFilteredMembersCollectionsRows()
      const head = cols.map(c=>`<th>${escapeHtml(labelForColumn(c))}</th>`).join('')
      const body = rows.map(r=> `<tr>${cols.map(c=> `<td>${escapeHtml(c==='verified' ? (r && r.__verified ? 'Yes' : 'No') : displayCellValue(c, r))}</td>`).join('')}</tr>`).join('')
      const table = `<table border="1"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`
      const html = `<!doctype html><html><head><meta charset="utf-8" /></head><body>${table}</body></html>`
      const blob = new Blob([html], { type: 'application/vnd.ms-excel' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      const stamp = new Date().toISOString().slice(0,19).replace(/[T:]/g,'-')
      a.href = url
      a.download = `members_collections_church_${currentUserChurchId || 'all'}_${stamp}.xls`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    }catch(e){
      setStatus('Export Excel failed: ' + (e?.message || String(e)))
    }
  }

  function exportMembersCollectionsPdf(){
    try{
      const cols = getMcDisplayColumns()
      const rows = getFilteredMembersCollectionsRows()
      const head = cols.map(c=>`<th style="border:1px solid #ccc;padding:6px;text-align:left">${escapeHtml(labelForColumn(c))}</th>`).join('')
      const body = rows.map(r=> `<tr>${cols.map(c=> `<td style="border:1px solid #ddd;padding:6px">${escapeHtml(c==='verified' ? (r && r.__verified ? 'Yes' : 'No') : displayCellValue(c, r))}</td>`).join('')}</tr>`).join('')
      const win = window.open('', '_blank')
      if(!win){ setStatus('Pop-up blocked. Allow pop-ups to export PDF.'); return }
      win.document.write(`<!doctype html><html><head><title>Members Collections</title><style>body{font-family:Arial,sans-serif;padding:12px}table{border-collapse:collapse;width:100%;font-size:12px}h3{margin:0 0 10px 0}</style></head><body><h3>Members Collections (Church ${escapeHtml(currentUserChurchId || '-')})</h3><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></body></html>`)
      win.document.close()
      win.focus()
      win.print()
    }catch(e){
      setStatus('Export PDF failed: ' + (e?.message || String(e)))
    }
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
        const res = await authFetch(`http://localhost:8000/members?q=${encodeURIComponent(name)}`)
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
    const role = String(user?.role || '').toLowerCase()
    if(role === 'system_admin') return true
    const compatibility = {
      can_manage_members_collections: new Set(['admin','treasurer','data_steward','uploader','viewer']),
      can_view_collection_codes: new Set(['admin','treasurer','uploader','viewer']),
      can_manage_collections: new Set(['admin','treasurer','uploader']),
      can_manage_members: new Set(['admin','data_steward']),
      can_view_reports: new Set(['admin','treasurer','data_steward','viewer']),
    }
    const rp = currentRolePolicy()
    if(!rp) return fallback
    const explicit = !!rp[flag]
    if(explicit) return true
    if(!!rp.system_protected && compatibility[flag] && compatibility[flag].has(role)) return true
    return false
  }

  function displayUserName(u){
    const first = String(u?.first_name || '').trim()
    const middle = String(u?.middle_name || '').trim()
    const last = String(u?.last_name || '').trim()
    const full = [first, middle, last].filter(Boolean).join(' ')
    if(full) return full
    return String(u?.username || '')
  }

  function statusMeta(){
    const raw = String(status || '').trim()
    if(!raw) return null
    const lower = raw.toLowerCase()
    const isError = lower.includes('failed') || lower.includes('error') || lower.includes('not authorized')
    const title = isError ? 'Error' : 'Success'
    const body = raw.includes(':') ? raw.split(':').slice(1).join(':').trim() || raw : raw
    return {
      title,
      body,
      style: isError
        ? { border:'1px solid #ef4444', background:'#fff5f5', title:'#111827', body:'#111827' }
        : { border:'1px solid #10b981', background:'#f5fffa', title:'#111827', body:'#111827' }
    }
  }

  function mainGridWrapStyle(maxHeight = 420){
    if(gridModeMain === 'scrollable'){
      return { maxHeight, overflowX:'auto', overflowY:'auto', border:'1px solid #ddd', borderRadius:6 }
    }
    return { maxHeight, overflow:'auto' }
  }

  function mainGridTableStyle(fill = true){
    if(gridModeMain === 'scrollable'){
      return { width:'max-content', minWidth: fill ? '100%' : 'max-content', borderCollapse:'collapse' }
    }
    return { width: fill ? '100%' : 'auto', borderCollapse:'collapse' }
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

  const filteredMembers = (members || [])
    .filter((m)=>{
      const q = String(membersSearchText || '').trim().toLowerCase()
      const churchName = (churches||[]).find(c=> Number(c.id)===Number(m?.church))?.name || m?.church || ''
      const bag = {
        name: m?.MEMBER_NAME || '',
        member_id: m?.MEMBER_ID ?? '',
        phone: m?.PHONE || '',
        church: String(churchName || ''),
        sno: m?.sno ?? '',
        all: [m?.sno, m?.MEMBER_NAME, m?.MEMBER_ID, m?.PHONE, churchName].join(' '),
      }
      const basicMatch = !q || String(bag[membersSearchField] ?? bag.all).toLowerCase().includes(q)
      if(!basicMatch) return false

      if(!membersExtraFilters.length) return true
      return membersExtraFilters.every(f=>{
        const field = f?.field
        const op = String(f?.op || 'contains')
        const needle = String(f?.value ?? '').trim().toLowerCase()
        if(!field || !needle) return true
        const hay = String(membersFieldValue(m, field) ?? '').toLowerCase()
        if(op === 'equals') return hay === needle
        if(op === 'starts_with') return hay.startsWith(needle)
        if(op === 'ends_with') return hay.endsWith(needle)
        return hay.includes(needle)
      })
    })
    .slice()
    .sort((a,b)=>{
      const av = membersFieldValue(a, membersSortKey)
      const bv = membersFieldValue(b, membersSortKey)
      const an = Number(av)
      const bn = Number(bv)
      let cmp = 0
      if(!Number.isNaN(an) && !Number.isNaN(bn) && String(av).trim() !== '' && String(bv).trim() !== ''){
        cmp = an - bn
      }else{
        cmp = String(av ?? '').localeCompare(String(bv ?? ''), undefined, { numeric: true, sensitivity: 'base' })
      }
      return membersSortDir === 'asc' ? cmp : -cmp
    })

  // If not authenticated, show a focused login screen (no menu)
  if(!user){
    return (
      <div style={{fontFamily:'Arial', minHeight:'100vh', display:'flex', flexDirection:'column', justifyContent:'center', alignItems:'center', background:'#f0f4f8'}}>
        <div style={{width:'100%', maxWidth:400, background:'#fff', borderRadius:12, boxShadow:'0 4px 24px rgba(14,30,64,0.12)', padding:'40px 36px'}}>
          <div style={{textAlign:'center', marginBottom:28}}>
            <div style={{fontSize:28, fontWeight:800, color:'#0f2d5c', letterSpacing:'-0.01em'}}>{systemName}</div>
            <div style={{fontSize:14, color:'#64748b', marginTop:6, fontWeight:500}}>Sign in to your account</div>
          </div>
          <div style={{display:'flex', flexDirection:'column', gap:12}}>
            <input
              placeholder='Username'
              value={loginUser}
              onChange={e=>setLoginUser(e.target.value)}
              onKeyDown={e=>{ if(e.key==='Enter') doLogin() }}
              style={{padding:'10px 12px', borderRadius:6, border:'1px solid #cbd5e1', fontSize:15, outline:'none', color:'#111827', background:'#fff', fontWeight:500}}
            />
            <input
              placeholder='Password'
              type='password'
              value={loginPass}
              onChange={e=>setLoginPass(e.target.value)}
              onKeyDown={e=>{ if(e.key==='Enter') doLogin() }}
              style={{padding:'10px 12px', borderRadius:6, border:'1px solid #cbd5e1', fontSize:15, outline:'none', color:'#111827', background:'#fff', fontWeight:500}}
            />
            <button
              onClick={doLogin}
              style={{padding:'10px 0', borderRadius:6, background:'#0f2d5c', color:'#fff', fontWeight:700, fontSize:15, border:'none', cursor:'pointer', marginTop:4}}
            >Login</button>
          </div>
          {statusMeta() && (
            <div style={{marginTop:16,padding:'12px 14px',borderRadius:8,border:statusMeta().style.border,background:statusMeta().style.background}}>
              <div style={{fontWeight:800,fontSize:14,color:statusMeta().style.title,marginBottom:4,letterSpacing:'0.01em'}}>{statusMeta().title}</div>
              <div style={{fontSize:14,fontWeight:600,color:statusMeta().style.body,lineHeight:1.45}}>{statusMeta().body}</div>
            </div>
          )}
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
            <button style={{marginLeft:8}} onClick={logout}>Logout</button>
          </div>
        </div>
      </header>

      <div style={{display:'grid',gridTemplateColumns:'220px 1fr',gap:16,marginTop:12}}>
        <aside style={{border:'1px solid #eee',borderRadius:8,padding:10,alignSelf:'start'}}>
          <div style={{fontWeight:700,marginBottom:10}}>Menu</div>
          <div style={{display:'flex',flexDirection:'column',gap:8}}>
            {hasRoleRight('can_view_dashboard', true) && <button onClick={()=>setPage('dashboard')} style={{textAlign:'left',background:page==='dashboard' ? '#e8f0fe' : '#fff'}}>Dashboard</button>}
            <button onClick={()=>setPage('preferences')} style={{textAlign:'left',background:page==='preferences' ? '#e8f0fe' : '#fff'}}>Preferences</button>
            {(hasRoleRight('can_manage_collections', true) || hasRoleRight('can_view_collection_codes', true)) && <button onClick={()=>setPage('collections')} style={{textAlign:'left',background:page==='collections' ? '#e8f0fe' : '#fff'}}>Collections</button>}
            {hasRoleRight('can_manage_members', true) && <button onClick={()=>setPage('members')} style={{textAlign:'left',background:page==='members' ? '#e8f0fe' : '#fff'}}>Members</button>}
            {hasRoleRight('can_manage_members_collections', true) && <button onClick={()=>setPage('members_collections')} style={{textAlign:'left',background:page==='members_collections' ? '#e8f0fe' : '#fff'}}>Members Collections</button>}
            {hasRoleRight('can_view_reports', true) && <button onClick={()=>setPage('reports')} style={{textAlign:'left',background:page==='reports' ? '#e8f0fe' : '#fff'}}>Reports</button>}
            {hasRoleRight('can_manage_users', isAdmin()) && <button onClick={()=>{ setPage('admin'); fetchUsers() }} style={{textAlign:'left',background:page==='admin' ? '#e8f0fe' : '#fff'}}>Admin</button>}
            {hasRoleRight('can_manage_settings', isAdmin()) && <button onClick={()=>setPage('settings')} style={{textAlign:'left',background:page==='settings' ? '#e8f0fe' : '#fff'}}>Settings</button>}
          </div>
        </aside>

        <main style={{borderTop:'1px solid #eee', paddingTop:12}}>
          {statusMeta() && (
            <div style={{marginBottom:12,padding:'12px 14px',borderRadius:8,border:statusMeta().style.border,background:statusMeta().style.background}}>
              <div style={{fontWeight:800,fontSize:15,color:statusMeta().style.title,marginBottom:4,letterSpacing:'0.01em'}}>{statusMeta().title}</div>
              <div style={{fontSize:15,fontWeight:600,color:statusMeta().style.body,lineHeight:1.45}}>{statusMeta().body}</div>
            </div>
          )}
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

        {page==='preferences' && (
          <div>
            <h3>Preferences</h3>

            <div style={{marginTop:12,border:'1px solid #ddd',padding:10,borderRadius:6}}>
              <h4>My Grid Preferences</h4>
              <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit,minmax(240px,1fr))',gap:12}}>
                <div style={{border:'1px solid #eee',borderRadius:6,padding:10}}>
                  <div style={{fontWeight:700,marginBottom:8}}>Main Grids Mode</div>
                  <label style={{display:'flex', gap:6, alignItems:'center', color:'#111827', fontWeight:600}}>
                    <input type='radio' name='main-grid-mode-preferences' checked={gridModeMain==='scrollable'} onChange={()=>setGridModeMain('scrollable')} />
                    Scrollable
                  </label>
                  <label style={{display:'flex', gap:6, alignItems:'center', color:'#111827', fontWeight:600, marginTop:6}}>
                    <input type='radio' name='main-grid-mode-preferences' checked={gridModeMain==='classic'} onChange={()=>setGridModeMain('classic')} />
                    Classic
                  </label>
                </div>
                <div style={{border:'1px solid #eee',borderRadius:6,padding:10}}>
                  <div style={{fontWeight:700,marginBottom:8}}>Upload Grids Mode</div>
                  <label style={{display:'flex', gap:6, alignItems:'center', color:'#111827', fontWeight:600}}>
                    <input type='radio' name='upload-grid-mode-preferences' checked={uploadGridMode==='scrollable'} onChange={()=>setUploadGridMode('scrollable')} />
                    Scrollable
                  </label>
                  <label style={{display:'flex', gap:6, alignItems:'center', color:'#111827', fontWeight:600, marginTop:6}}>
                    <input type='radio' name='upload-grid-mode-preferences' checked={uploadGridMode==='classic'} onChange={()=>setUploadGridMode('classic')} />
                    Classic
                  </label>
                </div>
                <div style={{border:'1px solid #eee',borderRadius:6,padding:10}}>
                  <div style={{fontWeight:700,marginBottom:8}}>Mapped Preview Mode</div>
                  <label style={{display:'flex', gap:6, alignItems:'center', color:'#111827', fontWeight:600}}>
                    <input type='radio' name='mapped-preview-mode-preferences' checked={mappedPreviewMode==='scrollable'} onChange={()=>setMappedPreviewMode('scrollable')} />
                    Scrollable
                  </label>
                  <label style={{display:'flex', gap:6, alignItems:'center', color:'#111827', fontWeight:600, marginTop:6}}>
                    <input type='radio' name='mapped-preview-mode-preferences' checked={mappedPreviewMode==='classic'} onChange={()=>setMappedPreviewMode('classic')} />
                    Classic
                  </label>
                </div>
              </div>
            </div>

            <div style={{marginTop:12,border:'1px solid #ddd',padding:10,borderRadius:6}}>
              <h4>Change My Password</h4>
              {!showOwnPasswordForm ? (
                <button onClick={()=> setShowOwnPasswordForm(true)}>Change Password</button>
              ) : (
                <>
                  <div style={{display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                    <input type='password' placeholder='Current password' value={ownCurrentPassword} onChange={e=>setOwnCurrentPassword(e.target.value)} />
                    <input type='password' placeholder='New password' value={ownNewPassword} onChange={e=>setOwnNewPassword(e.target.value)} />
                    <input type='password' placeholder='Confirm new password' value={ownConfirmPassword} onChange={e=>setOwnConfirmPassword(e.target.value)} />
                    <button onClick={submitOwnPasswordReset}>Update Password</button>
                    <button onClick={()=> setShowOwnPasswordForm(false)}>Cancel</button>
                  </div>
                  <div style={{marginTop:6,fontSize:12,color:'#666'}}>{PASSWORD_HINT}</div>
                </>
              )}
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
              <div style={mainGridWrapStyle(420)}>
              <table border={1} cellPadding={6} style={mainGridTableStyle(false)}>
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
              </div>

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
            {!currentUserChurchId && <div style={{backgroundColor:'#fff3cd',padding:'12px 14px',marginBottom:12,borderRadius:6,border:'1px solid #f59e0b',color:'#7c2d12',fontSize:15,fontWeight:700,lineHeight:1.45}}>⚠️ You do not have a church assigned. Only local church admins can access settings.</div>}

            {String(user?.role || '').toLowerCase() === 'system_admin' && (
              <div style={{marginTop:12,border:'1px solid #ddd',padding:10,borderRadius:6}}>
                <h4>System Name <span style={{fontSize:12,color:'#64748b',fontWeight:400}}>(shown on login screen)</span></h4>
                {!showEditSystemName ? (
                  <div style={{display:'flex',gap:10,alignItems:'center'}}>
                    <span style={{fontWeight:600}}>{systemName}</span>
                    <button onClick={()=>{ setEditingSystemName(systemName); setShowEditSystemName(true) }}>Edit</button>
                  </div>
                ) : (
                  <div style={{display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                    <input type='text' value={editingSystemName} onChange={e=>setEditingSystemName(e.target.value)} placeholder='System name' style={{minWidth:220}} />
                    <button onClick={submitSystemName}>Save</button>
                    <button onClick={()=>{ setShowEditSystemName(false); setEditingSystemName('') }}>Cancel</button>
                  </div>
                )}
              </div>
            )}
            
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
                      <label><input type='checkbox' checked={!!newRole.can_manage_members_collections} onChange={e=>setNewRole(prev=>({...prev, can_manage_members_collections:e.target.checked}))} /> Members Collections</label>
                      <label><input type='checkbox' checked={!!newRole.can_view_collection_codes} onChange={e=>setNewRole(prev=>({...prev, can_view_collection_codes:e.target.checked}))} /> View Collection Codes</label>
                      <label><input type='checkbox' checked={!!newRole.can_view_reports} onChange={e=>setNewRole(prev=>({...prev, can_view_reports:e.target.checked}))} /> Reports</label>
                      <label><input type='checkbox' checked={!!newRole.can_manage_settings} onChange={e=>setNewRole(prev=>({...prev, can_manage_settings:e.target.checked}))} /> Settings</label>
                      <button onClick={createRolePolicy}>Add Role</button>
                    </div>
                  </div>

                  <div style={mainGridWrapStyle(460)}>
                    <table border={1} cellPadding={6} style={mainGridTableStyle(true)}>
                      <thead>
                        <tr>
                          <th>Role</th>
                          <th>Display Name</th>
                          <th>Dashboard</th>
                          <th>Users</th>
                          <th>Members</th>
                          <th>Collections</th>
                          <th>Members Collections</th>
                          <th>View Collection Codes</th>
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
                            <td>{editingRole && editingRole.role===r.role ? <input type='checkbox' checked={!!editingRole.can_manage_members_collections} onChange={e=>setEditingRole(prev=>({...prev, can_manage_members_collections:e.target.checked}))} /> : (r.can_manage_members_collections ? 'Yes':'No')}</td>
                            <td>{editingRole && editingRole.role===r.role ? <input type='checkbox' checked={!!editingRole.can_view_collection_codes} onChange={e=>setEditingRole(prev=>({...prev, can_view_collection_codes:e.target.checked}))} /> : (r.can_view_collection_codes ? 'Yes':'No')}</td>
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
              <button onClick={addMembersFilter}>Add Field Filter</button>
              <button onClick={clearMembersFilters}>Clear Field Filters</button>
              <button style={{marginLeft:8}} onClick={()=>{ setEditingMember(null); setMemberForm({}); setShowMemberForm(true); }}>New Member</button>
              <button style={{marginLeft:12}} onClick={openMembersColumnPicker}>Column Picker</button>
              <button onClick={saveMembersColumnSelection}>Save Selection</button>
              <button onClick={exportMembersExcel}>Export Excel</button>
              <button onClick={exportMembersPdf}>Export PDF</button>
              <label style={{marginLeft:12}}>Max rows</label>
              <select value={membersMaxRows} onChange={e=>setMembersMaxRows(Number(e.target.value))}>
                {[10,30,50,100].map(n=> <option key={n} value={n}>{n}</option>)}
              </select>
              <span style={{color:'#666'}}>Showing {Math.min(filteredMembers.length, membersMaxRows)} of {filteredMembers.length} (filters: {membersExtraFilters.length})</span>
            </div>

            {membersExtraFilters.length > 0 && (
              <div style={{marginBottom:10,padding:'10px 12px',border:'1px solid #ddd',borderRadius:8,background:'#f8fafc'}}>
                <div style={{fontSize:13,fontWeight:700,marginBottom:8}}>Field Filters (AND logic)</div>
                <div style={{display:'flex',flexDirection:'column',gap:8}}>
                  {membersExtraFilters.map(f => (
                    <div key={f.id} style={{display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                      <select value={f.field} onChange={e=>updateMembersFilter(f.id, { field: e.target.value })}>
                        {membersFilterableFields().map(col => <option key={col} value={col}>{membersFieldLabel(col)}</option>)}
                      </select>
                      <select value={f.op} onChange={e=>updateMembersFilter(f.id, { op: e.target.value })}>
                        <option value='contains'>contains</option>
                        <option value='equals'>equals</option>
                        <option value='starts_with'>starts with</option>
                        <option value='ends_with'>ends with</option>
                      </select>
                      <input placeholder='value...' value={f.value} onChange={e=>updateMembersFilter(f.id, { value: e.target.value })} />
                      <button onClick={()=>removeMembersFilter(f.id)}>Remove</button>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div style={{marginBottom:10,padding:'10px 12px',border:'1px solid #ddd',borderRadius:8,background:'#f8fafc'}}>
              <span style={{fontSize:12,color:'#334155',fontWeight:600}}>
                Showing: {getMembersDisplayColumns().map(memberLabelForColumn).join(', ')}
              </span>
            </div>

            {showMembersColumnPicker && (
              <div style={{position:'fixed',top:0,left:0,right:0,bottom:0,background:'rgba(0,0,0,0.5)',display:'flex',alignItems:'center',justifyContent:'center',zIndex:9999}}>
                <div style={{background:'#fff',borderRadius:8,padding:24,maxWidth:800,width:'90%',maxHeight:'90vh',overflow:'auto',boxShadow:'0 10px 40px rgba(0,0,0,0.3)'}}>
                  <h3 style={{marginTop:0,marginBottom:20,fontSize:18,fontWeight:800,color:'#111827'}}>Members Column Picker</h3>
                  <div style={{display:'grid',gridTemplateColumns:'1fr auto 1fr',gap:16,marginBottom:20}}>
                    <div>
                      <div style={{fontSize:12,fontWeight:800,marginBottom:8,color:'#111827'}}>AVAILABLE COLUMNS</div>
                      <div style={{border:'1px solid #ddd',borderRadius:6,minHeight:260,maxHeight:360,overflowY:'auto',background:'#fafafa'}}>
                        {membersPickerLeft.length === 0 ? (
                          <div style={{padding:12,textAlign:'center',color:'#475569',fontSize:12,fontWeight:600}}>No available columns</div>
                        ) : membersPickerLeft.map(col => (
                          <div key={col} onClick={()=>{
                            const next = new Set(membersPickerLeftSelected)
                            if(next.has(col)) next.delete(col); else next.add(col)
                            setMembersPickerLeftSelected(next)
                          }} style={{padding:'10px 12px',borderBottom:'1px solid #e5e5e5',cursor:'pointer',background:membersPickerLeftSelected.has(col) ? '#e3f2fd' : '#fafafa',fontSize:13,color:'#111827',fontWeight:600,userSelect:'none'}}>
                            {memberLabelForColumn(col)}
                          </div>
                        ))}
                      </div>
                    </div>
                    <div style={{display:'flex',flexDirection:'column',justifyContent:'center',gap:8}}>
                      <button onClick={moveMembersColumnsToRight} disabled={membersPickerLeftSelected.size===0}>Add -&gt;</button>
                      <button onClick={moveMembersColumnsToLeft} disabled={membersPickerRightSelected.size===0}>&lt;- Remove</button>
                    </div>
                    <div>
                      <div style={{fontSize:12,fontWeight:800,marginBottom:8,color:'#111827'}}>SELECTED COLUMNS</div>
                      <div style={{border:'1px solid #ddd',borderRadius:6,minHeight:260,maxHeight:360,overflowY:'auto',background:'#f0f9ff'}}>
                        {membersPickerRight.length === 0 ? (
                          <div style={{padding:12,textAlign:'center',color:'#475569',fontSize:12,fontWeight:600}}>No selected columns</div>
                        ) : membersPickerRight.map((col, idx) => (
                          <div key={col} onClick={()=>{
                            const next = new Set(membersPickerRightSelected)
                            if(next.has(col)) next.delete(col); else next.add(col)
                            setMembersPickerRightSelected(next)
                          }} style={{padding:'10px 12px',borderBottom:'1px solid #e5e5e5',cursor:'pointer',background:membersPickerRightSelected.has(col) ? '#bbdefb' : '#f0f9ff',fontSize:13,color:'#111827',fontWeight:600,userSelect:'none',display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                            <span>{memberLabelForColumn(col)}</span>
                            <span style={{fontSize:11,color:'#334155',fontWeight:700}}>{idx + 1}</span>
                          </div>
                        ))}
                      </div>
                      {membersPickerRightSelected.size === 1 && (
                        <div style={{marginTop:8,display:'flex',gap:6}}>
                          <button onClick={moveMembersColumnUp} style={{flex:1}}>Move Up</button>
                          <button onClick={moveMembersColumnDown} style={{flex:1}}>Move Down</button>
                        </div>
                      )}
                    </div>
                  </div>
                  <div style={{display:'flex',justifyContent:'flex-end',gap:8}}>
                    <button onClick={()=>setShowMembersColumnPicker(false)}>Cancel</button>
                    <button onClick={applyMembersColumnSelection}>Apply</button>
                  </div>
                </div>
              </div>
            )}

            <div style={mainGridWrapStyle(400)}>
              <table style={mainGridTableStyle(true)}>
                <thead>
                  <tr>
                    {getMembersDisplayColumns().map(h=> (
                      <th key={h} style={{cursor:'pointer'}} onClick={()=>{
                        if(membersSortKey === h){
                          setMembersSortDir(d => d === 'asc' ? 'desc' : 'asc')
                        }else{
                          setMembersSortKey(h)
                          setMembersSortDir('asc')
                        }
                      }}>
                        {memberLabelForColumn(h)} {membersSortKey===h ? (membersSortDir==='asc' ? '▲' : '▼') : ''}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filteredMembers.slice(0, membersMaxRows).map(m=> (
                    <tr key={m.id} onClick={()=>{ setEditingMember(m); setMemberForm({...m}); setShowMemberForm(true); }} style={{cursor:'pointer'}}>
                      {getMembersDisplayColumns().map(h=> <td key={h}>{memberDisplayCellValue(h, m)}</td>)}
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
                    if(key==='STATUS'){
                      return (
                        <select key={key} value={val||''} onChange={e=>setMemberForm(prev=>({...prev,[key]: e.target.value || null}))}>
                          <option value=''>-- status --</option>
                          <option value='active'>active</option>
                          <option value='inactive by transfer'>inactive by transfer</option>
                          <option value='inactive by removal'>inactive by removal</option>
                          <option value='inactive by death'>inactive by death</option>
                          <option value='inactive unknown reason'>inactive unknown reason</option>
                        </select>
                      )
                    }
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
                    if(key==='TRANSFER_TO_CHURCH' || key==='transfer_to_church'){
                      const canCreateChurch = String(user?.role || '').toLowerCase() === 'system_admin' || String(user?.username || '').toLowerCase() === 'saypy_admin'
                      const selectedTransferChurch = memberForm?.TRANSFER_TO_CHURCH ?? memberForm?.transfer_to_church ?? ''
                      return (
                        <div key={key} style={{display:'flex',flexDirection:'column',gap:6,minWidth:260}}>
                          <select value={selectedTransferChurch || ''} onChange={e=>{
                            const selected = e.target.value
                            if(!canAccessChurch(selected)){ denyRestrictedChurchAccess('member transfer'); return }
                            const mapped = selected === '' ? null : Number(selected)
                            setMemberForm(prev=>({...prev, TRANSFER_TO_CHURCH: mapped, transfer_to_church: mapped}))
                          }}>
                            <option value=''>-- transfer to church --</option>
                            {churches.map(c=> <option key={c.id} value={c.id} disabled={!canAccessChurch(c.id)}>{c.name}</option>)}
                          </select>
                          <div style={{display:'flex',gap:6,alignItems:'center',flexWrap:'wrap'}}>
                            <button type='button' onClick={()=>setShowTransferChurchCreator(v=>!v)}>
                              {showTransferChurchCreator ? 'Cancel add church' : 'Add missing church'}
                            </button>
                            {!canCreateChurch && <span style={{fontSize:12,color:'#666'}}>System admin only</span>}
                          </div>
                          {showTransferChurchCreator && (
                            <div style={{display:'flex',gap:6,alignItems:'center',flexWrap:'wrap'}}>
                              <input
                                placeholder='New church name'
                                value={newTransferChurchName}
                                onChange={e=>setNewTransferChurchName(e.target.value)}
                                disabled={!canCreateChurch}
                                style={{minWidth:180}}
                              />
                              <button type='button' onClick={createTransferChurchOption} disabled={!canCreateChurch}>Create</button>
                            </div>
                          )}
                        </div>
                      )
                    }
                    if(key==='TRANSFER_DATE' || key==='transfer_date'){
                      const v = val? new Date(val).toISOString().slice(0,10) : ''
                      return <input key={key} type='date' value={v} onChange={e=> setMemberForm(prev=>({...prev, [key]: e.target.value || null}))} />
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

          </div>
        )}

        {page==='collections' && (
          <div>
            <h3>Collections — Upload & Manage</h3>
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
              uploadGridMode={uploadGridMode}
              mappedPreviewMode={mappedPreviewMode}
              canManageCollections={hasRoleRight('can_manage_collections', true)}
            />

            <div style={{marginTop:12,border:'1px solid #ddd',padding:10,borderRadius:6}}>
              <h4>Collection Codes</h4>
              {(() => {
                const canCollectionCodeEdit = hasRoleRight('can_manage_collections', true) || hasRoleRight('can_view_collection_codes', true)
                const localChurchCollectionCodes = (localCollectionCodes || []).filter(c=> Number(c.church)===Number(currentUserChurchId))
                return (
                  <>
              <p style={{fontSize:12,color:'#666'}}>
                {canCollectionCodeEdit 
                  ? 'View existing church collection codes, edit them, or add more for your church.' 
                  : 'View existing church collection codes.'}
              </p>
              <p style={{fontSize:12,color:'#666'}}>These codes apply only to your church. Global codes are shared across all churches.</p>
              {!currentUserChurchId && <div style={{backgroundColor:'#fff3cd',padding:'12px 14px',borderRadius:6,border:'1px solid #f59e0b',color:'#7c2d12',fontSize:15,fontWeight:700,lineHeight:1.45}}>A church assignment is required to manage collection codes.</div>}

              <div style={{marginBottom:12}}>
                <button
                  type='button'
                  onClick={()=>setShowCollectionCodesPanel(v=>!v)}
                  style={{background:'none',border:'none',color:'#0a58ca',textDecoration:'underline',cursor:'pointer',padding:0}}
                >
                  {showCollectionCodesPanel ? 'Hide collection codes' : 'View collection codes'}
                </button>
              </div>

              {showCollectionCodesPanel && (
                <div style={{marginBottom:12,display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                  <label>Show</label>
                  <select value={collectionCodesMaxRows} onChange={e=>setCollectionCodesMaxRows(Number(e.target.value))}>
                    {[10,30,50,100,200].map(n=> <option key={n} value={n}>{n}</option>)}
                  </select>
                  <span style={{color:'#666'}}>Showing {Math.min(localCollectionCodes.length, collectionCodesMaxRows)} of {localCollectionCodes.length}</span>
                </div>
              )}

              {canCollectionCodeEdit && (
                <div style={{marginBottom:12}}>
                  <h5>Add New Code</h5>
                  <div style={{display:'flex',gap:8,alignItems:'center',flexWrap:'wrap'}}>
                    <input type='text' value={newCodeColumn} onChange={e=>setNewCodeColumn(e.target.value)} placeholder='Code key (e.g. c21)' disabled={!currentUserChurchId} />
                    <input type='text' value={newCodeLabel} onChange={e=>setNewCodeLabel(e.target.value)} placeholder='Collection label' disabled={!currentUserChurchId} />
                    <input type='text' value={newCodeCustomName} onChange={e=>setNewCodeCustomName(e.target.value)} placeholder='Custom collection name (optional)' disabled={!currentUserChurchId} />
                    <button onClick={createLocalCode} disabled={!currentUserChurchId}>Add Code</button>
                  </div>
                </div>
              )}

              {showCollectionCodesPanel && (
              <div>
                <h5>Church Collection Codes Grid (Church ID: {currentUserChurchId || '-'})</h5>
                {localChurchCollectionCodes.length === 0 ? (
                  <div style={{color:'#666'}}>No collection codes found for this church.</div>
                ) : (
                  <div style={mainGridWrapStyle(420)}>
                  <table border={1} cellPadding={6} style={mainGridTableStyle(true)}>
                    <thead><tr><th>ID</th><th>Church ID</th><th>Code Key</th><th>Label</th><th>Custom Name</th><th>Scope</th>{canCollectionCodeEdit && <th>Actions</th>}</tr></thead>
                    <tbody>
                      {localChurchCollectionCodes.slice(0, collectionCodesMaxRows).map(code=> (
                        <tr key={code.id}>
                          <td>{code.id}</td>
                          <td>{code.church}</td>
                          <td>{code.column_name}</td>
                          <td>{code.code}</td>
                          <td>{code.custom_collection_name || '-'}</td>
                          <td>Local</td>
                          {canCollectionCodeEdit && (
                            <td>
                              {editingCodeId !== code.id ? (
                                <>
                                  <button onClick={()=>{ setEditingCodeId(code.id); setEditCodeForm({column_name:code.column_name, code:code.code, custom_collection_name:(code.custom_collection_name || '')}); }} style={{marginRight:8}}>Edit</button>
                                  {hasRoleRight('can_manage_collections', true) && <button onClick={()=>deleteLocalCode(code.id)}>Delete</button>}
                                </>
                              ) : (
                                <>
                                  <input type='text' value={editCodeForm.column_name} onChange={e=>setEditCodeForm({...editCodeForm,column_name:e.target.value})} style={{marginRight:4}} />
                                  <input type='text' value={editCodeForm.code} onChange={e=>setEditCodeForm({...editCodeForm,code:e.target.value})} style={{marginRight:4}} />
                                  <input type='text' value={editCodeForm.custom_collection_name} onChange={e=>setEditCodeForm({...editCodeForm,custom_collection_name:e.target.value})} style={{marginRight:4}} />
                                  <button onClick={updateLocalCode} style={{marginRight:4}}>Save</button>
                                  <button onClick={()=>{ setEditingCodeId(null); setEditCodeForm({column_name:'',code:'',custom_collection_name:''}); }}>Cancel</button>
                                </>
                              )}
                            </td>
                          )}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  </div>
                )}
              </div>
              )}
                  </>
                )
              })()}
            </div>
          </div>
        )}

        {page==='members_collections' && (
          <div>
            <h3>Members Collections</h3>
            <div style={{marginBottom:8,fontSize:13,color:'#475569',fontWeight:600}}>
              Showing latest 10 records for your church by default (Church ID: {currentUserChurchId || '-'}). Apply filters to view more matching records.
            </div>
            <div style={{marginBottom:8, display:'flex', gap:8, alignItems:'center', flexWrap:'wrap'}}>
              <button onClick={()=> fetchMembersCollections(hasActiveMcFilters() ? {
                search: mcApplied.text,
                searchField: mcApplied.searchField,
                collectionCode: mcApplied.code,
                memberId: mcApplied.memberId,
                memberName: mcApplied.memberName,
                s1: mcApplied.s1,
                from: mcApplied.from,
                to: mcApplied.to,
                amountMin: mcApplied.amountMin,
                amountMax: mcApplied.amountMax,
              } : { limit: getDefaultMcFetchLimit(mcPage, mcPageSize) })}>Refresh</button>
              <label>Search in</label>
              <select value={mcSearchField} onChange={e=>setMcSearchField(e.target.value)}>
                <option value='all'>All fields</option>
                <option value='collection_code'>Collection Code</option>
                <option value='s4'>Member Name</option>
                <option value='member_id'>Member ID</option>
                <option value='s1'>S1</option>
                <option value='church'>Church</option>
              </select>
              <input placeholder='Search text' value={mcFilterText} onChange={e=>setMcFilterText(e.target.value)} />
              <select value={mcFilterCode} onChange={e=>setMcFilterCode(e.target.value)}>
                <option value=''>-- all codes --</option>
                {(collectionCodes||[]).map(c=> <option key={c.column_name} value={c.column_name}>{c.code || c.column_name}</option>)}
              </select>
              <input placeholder='Member ID' value={mcFilterMemberId} onChange={e=>setMcFilterMemberId(e.target.value)} />
              <input placeholder='Member name' value={mcFilterMemberName} onChange={e=>setMcFilterMemberName(e.target.value)} />
              <input placeholder='S1 contains' value={mcFilterS1} onChange={e=>setMcFilterS1(e.target.value)} />
              <button onClick={()=> verifyNames()}>Verify Names</button>
              <label>From</label>
              <input type='date' value={mcFrom} onChange={e=>setMcFrom(e.target.value)} />
              <label>To</label>
              <input type='date' value={mcTo} onChange={e=>setMcTo(e.target.value)} />
              <label>Amount min</label>
              <input type='number' step='0.01' value={mcFilterAmountMin} onChange={e=>setMcFilterAmountMin(e.target.value)} style={{width:110}} />
              <label>Amount max</label>
              <input type='number' step='0.01' value={mcFilterAmountMax} onChange={e=>setMcFilterAmountMax(e.target.value)} style={{width:110}} />
              <button onClick={()=>{
                const next = {
                  searchField: mcSearchField,
                  text: mcFilterText,
                  code: mcFilterCode,
                  memberId: mcFilterMemberId,
                  memberName: mcFilterMemberName,
                  s1: mcFilterS1,
                  amountMin: mcFilterAmountMin,
                  amountMax: mcFilterAmountMax,
                  from: mcFrom,
                  to: mcTo,
                }
                setMcApplied(next)
                setMcPage(1)
                fetchMembersCollections({
                  search: next.text,
                  searchField: next.searchField,
                  collectionCode: next.code,
                  memberId: next.memberId,
                  memberName: next.memberName,
                  s1: next.s1,
                  from: next.from,
                  to: next.to,
                  amountMin: next.amountMin,
                  amountMax: next.amountMax,
                })
              }}>Submit</button>
              <button onClick={()=>{
                setMcFilterText('')
                setMcFilterCode('')
                setMcFilterMemberId('')
                setMcFilterMemberName('')
                setMcFilterS1('')
                setMcFilterAmountMin('')
                setMcFilterAmountMax('')
                setMcFrom('')
                setMcTo('')
                setMcApplied({ searchField: 'all', text: '', code: '', memberId: '', memberName: '', s1: '', amountMin: '', amountMax: '', from: '', to: '' })
                setMcPage(1)
                fetchMembersCollections({ limit: getDefaultMcFetchLimit(1, mcPageSize) })
              }}>Clear</button>
              <label>Max rows</label>
              <select value={mcPageSize} onChange={async e=>{
                const nextSize = Number(e.target.value)
                setMcPageSize(nextSize)
                setMcPage(1)
                if(!hasActiveMcFilters()){
                  await fetchMembersCollections({ limit: getDefaultMcFetchLimit(1, nextSize) })
                }
              }}>
                {[10,30,50,100].map(n=> <option key={n} value={n}>{n}</option>)}
              </select>
              <button onClick={exportMembersCollectionsExcel}>Export Excel</button>
              <button onClick={exportMembersCollectionsPdf}>Export PDF</button>
            </div>

            <div style={{marginBottom:10,padding:'10px 12px',border:'1px solid #ddd',borderRadius:8,background:'#f8fafc'}}>
              <button onClick={openMcColumnPicker} style={{background:'none',border:'none',color:'#0a58ca',textDecoration:'underline',cursor:'pointer',padding:0,fontSize:13,fontWeight:700}}>
                Column Picker ({getMcDisplayColumns().length} visible)
              </button>
              <span style={{marginLeft:12,fontSize:12,color:'#334155',fontWeight:600}}>
                Showing: {getMcDisplayColumns().map(labelForColumn).join(', ')}
              </span>
            </div>

            {showMcColumnPicker && (
              <div style={{position:'fixed',top:0,left:0,right:0,bottom:0,background:'rgba(0,0,0,0.5)',display:'flex',alignItems:'center',justifyContent:'center',zIndex:9999}}>
                <div style={{background:'#fff',borderRadius:8,padding:24,maxWidth:800,width:'90%',maxHeight:'90vh',overflow:'auto',boxShadow:'0 10px 40px rgba(0,0,0,0.3)'}}>
                  <h3 style={{marginTop:0,marginBottom:20,fontSize:18,fontWeight:800,color:'#111827'}}>Column Picker</h3>

                  <div style={{display:'grid',gridTemplateColumns:'1fr auto 1fr',gap:16,marginBottom:20}}>
                    <div>
                      <div style={{fontSize:12,fontWeight:800,marginBottom:8,color:'#111827'}}>AVAILABLE COLUMNS</div>
                      <div style={{border:'1px solid #ddd',borderRadius:6,minHeight:300,maxHeight:400,overflowY:'auto',background:'#fafafa'}}>
                        {mcPickerLeft.length === 0 ? (
                          <div style={{padding:12,textAlign:'center',color:'#475569',fontSize:12,fontWeight:600}}>No available columns</div>
                        ) : (
                          <div>
                            {mcPickerLeft.map(col => (
                              <div
                                key={col}
                                onClick={()=>{
                                  const newSelected = new Set(mcPickerLeftSelected)
                                  if(newSelected.has(col)) newSelected.delete(col)
                                  else newSelected.add(col)
                                  setMcPickerLeftSelected(newSelected)
                                }}
                                style={{
                                  padding:'10px 12px',
                                  borderBottom:'1px solid #e5e5e5',
                                  cursor:'pointer',
                                  background:mcPickerLeftSelected.has(col) ? '#e3f2fd' : '#fafafa',
                                  fontSize:13,
                                  color:'#111827',
                                  fontWeight:600,
                                  userSelect:'none'
                                }}
                              >
                                {labelForColumn(col)}
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>

                    <div style={{display:'flex',flexDirection:'column',justifyContent:'center',gap:8}}>
                      <button
                        onClick={moveMcColumnsToRight}
                        disabled={mcPickerLeftSelected.size === 0}
                        style={{padding:'8px 12px',fontSize:14,cursor:mcPickerLeftSelected.size===0? 'default':'pointer',opacity:mcPickerLeftSelected.size===0?0.5:1,background:'#0a58ca',color:'#fff',border:'none',borderRadius:4,fontWeight:600}}
                      >
                        Add →
                      </button>
                      <button
                        onClick={moveMcColumnsToLeft}
                        disabled={mcPickerRightSelected.size === 0}
                        style={{padding:'8px 12px',fontSize:14,cursor:mcPickerRightSelected.size===0? 'default':'pointer',opacity:mcPickerRightSelected.size===0?0.5:1,background:'#dc3545',color:'#fff',border:'none',borderRadius:4,fontWeight:600}}
                      >
                        ← Remove
                      </button>
                    </div>

                    <div>
                      <div style={{fontSize:12,fontWeight:800,marginBottom:8,color:'#111827'}}>SELECTED COLUMNS</div>
                      <div style={{border:'1px solid #ddd',borderRadius:6,minHeight:300,maxHeight:400,overflowY:'auto',background:'#f0f9ff'}}>
                        {mcPickerRight.length === 0 ? (
                          <div style={{padding:12,textAlign:'center',color:'#475569',fontSize:12,fontWeight:600}}>No selected columns</div>
                        ) : (
                          <div>
                            {mcPickerRight.map((col, idx) => (
                              <div
                                key={col}
                                onClick={()=>{
                                  const newSelected = new Set(mcPickerRightSelected)
                                  if(newSelected.has(col)) newSelected.delete(col)
                                  else newSelected.add(col)
                                  setMcPickerRightSelected(newSelected)
                                }}
                                style={{
                                  padding:'10px 12px',
                                  borderBottom:'1px solid #e5e5e5',
                                  cursor:'pointer',
                                  background:mcPickerRightSelected.has(col) ? '#bbdefb' : '#f0f9ff',
                                  fontSize:13,
                                  color:'#111827',
                                  fontWeight:600,
                                  userSelect:'none',
                                  display:'flex',
                                  justifyContent:'space-between',
                                  alignItems:'center'
                                }}
                              >
                                <span>{labelForColumn(col)}</span>
                                <span style={{fontSize:11,color:'#334155',fontWeight:700}}>{idx + 1}</span>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>

                      {mcPickerRightSelected.size === 1 && (
                        <div style={{marginTop:8,display:'flex',gap:6}}>
                          <button
                            onClick={moveMcColumnUp}
                            style={{flex:1,padding:'6px 8px',fontSize:12,background:'#28a745',color:'#fff',border:'none',borderRadius:4,cursor:'pointer',fontWeight:600}}
                          >
                            Move Up
                          </button>
                          <button
                            onClick={moveMcColumnDown}
                            style={{flex:1,padding:'6px 8px',fontSize:12,background:'#28a745',color:'#fff',border:'none',borderRadius:4,cursor:'pointer',fontWeight:600}}
                          >
                            Move Down
                          </button>
                        </div>
                      )}
                    </div>
                  </div>

                  <div style={{display:'flex',justifyContent:'flex-end',gap:8,marginTop:20}}>
                    <button
                      onClick={()=>setShowMcColumnPicker(false)}
                      style={{padding:'8px 16px',fontSize:13,background:'#e0e0e0',border:'none',borderRadius:4,cursor:'pointer',fontWeight:600}}
                    >
                      Cancel
                    </button>
                    <button
                      onClick={saveMcColumns}
                      style={{padding:'8px 16px',fontSize:13,background:'#0a58ca',color:'#fff',border:'none',borderRadius:4,cursor:'pointer',fontWeight:600}}
                    >
                      Apply
                    </button>
                  </div>
                </div>
              </div>
            )}

            <div className='table-wrap' style={mainGridWrapStyle(500)}>
              <table style={mainGridTableStyle(true)}>
                <thead>
                  <tr>
                    {getMcDisplayColumns().map(c=> (
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
                      const rows = getFilteredMembersCollectionsRows()
                      const start = (mcPage-1)*mcPageSize
                      const end = start + mcPageSize
                      const pageRows = rows.slice(start, end)
                      return pageRows.map((r,idx)=> (
                        <tr key={r && (r.id||idx)}>
                          {getMcDisplayColumns().map(k=> {
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
                      return <tr><td colSpan={ getMcDisplayColumns().length + 1 }>Error rendering collections: {String(err.message||err)}</td></tr>
                    }
                  })()}
                </tbody>
              </table>
            </div>
            <div style={{marginTop:8}}>
              <button onClick={()=> setMcPage(p=> Math.max(1,p-1))} disabled={mcPage<=1}>Prev</button>
              <span style={{margin:'0 8px'}}>Page {mcPage} / {Math.max(1, Math.ceil(getFilteredMembersCollectionsRows().length / mcPageSize))} ({getFilteredMembersCollectionsRows().length} records)</span>
              <button onClick={async ()=>{
                if(hasActiveMcFilters()){
                  setMcPage(p=> {
                    const pages = Math.max(1, Math.ceil(getFilteredMembersCollectionsRows().length / mcPageSize))
                    return Math.min(pages, p+1)
                  })
                  return
                }
                const nextPage = mcPage + 1
                const count = await fetchMembersCollections({ limit: getDefaultMcFetchLimit(nextPage, mcPageSize) })
                if(count > (nextPage - 1) * mcPageSize){
                  setMcPage(nextPage)
                }
              }} disabled={hasActiveMcFilters() ? mcPage >= Math.max(1, Math.ceil(getFilteredMembersCollectionsRows().length / mcPageSize)) : getFilteredMembersCollectionsRows().length < mcPage * mcPageSize}>Next</button>
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
              <div style={{...mainGridWrapStyle(400), marginTop:8}}>
                <table style={mainGridTableStyle(true)}>
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
                    <div style={mainGridWrapStyle(320)}>
                    <table style={mainGridTableStyle(true)}>
                      <thead><tr><th>Collection</th><th>Count</th><th>Total s5</th><th>Total s6</th><th>Total s7</th></tr></thead>
                      <tbody>{aggRows.map(a=> <tr key={a.collection_code}><td>{a.collection_code}</td><td>{a.count}</td><td>{a.s5}</td><td>{a.s6}</td><td>{a.s7}</td></tr>)}</tbody>
                    </table>
                    </div>
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
                  <div style={mainGridWrapStyle(300)}>
                    <table style={mainGridTableStyle(true)}>
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

function CollectionsUpload({token, authFetch, collectionCodes, churches, fetchCodes, user, labelForColumn, scopedChurchId, onRestrictedChurchAttempt, uploadGridMode, mappedPreviewMode, canManageCollections}){
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
  const [showQuickForm, setShowQuickForm] = useState(false)
  const [quickRows, setQuickRows] = useState([{ id: 1, name: '', collection_date: '', total_amount: '', detail_entries: [], selected_detail: '', selected_detail_amount: '' }])
  const [quickSubmitting, setQuickSubmitting] = useState(false)

  // total steps (1=file/date/church, 2=mapping, 3=preview/edit, 4=fix, 5=done)
  const totalSteps = 5

  useEffect(()=>{ fetchCodes() }, [])
  useEffect(()=>{
    if(user && !uploaderName){
      const first = String(user?.first_name || '').trim()
      const middle = String(user?.middle_name || '').trim()
      const last = String(user?.last_name || '').trim()
      const full = [first, middle, last].filter(Boolean).join(' ')
      setUploaderName(full || String(user?.username || ''))
    }
  }, [user])
  useEffect(()=>{
    if(scopedChurchId !== null && scopedChurchId !== undefined && !Number.isNaN(Number(scopedChurchId))){
      setSelectedChurch(String(scopedChurchId))
    }
  }, [scopedChurchId])

  function savedCollectionDateKey(){
    const userKey = String(user?.id || user?.username || '').trim()
    return userKey ? `saypy.collection.selectedDate.${userKey}` : ''
  }

  useEffect(()=>{
    const storageKey = savedCollectionDateKey()
    if(!storageKey) return
    try{
      const savedValue = localStorage.getItem(storageKey)
      if(savedValue){
        setSelectedDate(savedValue)
      }
    }catch(e){}
  }, [user?.id, user?.username])

  function saveSelectedCollectionDate(){
    const storageKey = savedCollectionDateKey()
    if(!storageKey){
      showPrompt('warning', 'Unable to save date selection because no user context was found.')
      return
    }
    if(!selectedDate){
      showPrompt('warning', 'Choose a date before saving the selection.')
      return
    }
    try{
      localStorage.setItem(storageKey, selectedDate)
      showPrompt('success', `Saved date selection for ${String(user?.username || user?.id || 'current user')}: ${formatPreviewDate(selectedDate)}`)
    }catch(e){
      showPrompt('error', 'Failed to save date selection: ' + (e?.message || String(e)))
    }
  }

  function canAccessChurch(churchId){
    if(churchId === null || churchId === undefined || churchId === '') return true
    if(scopedChurchId === null || scopedChurchId === undefined || Number.isNaN(Number(scopedChurchId))) return true
    return Number(churchId) === Number(scopedChurchId)
  }

  function handleRestrictedChurch(context){
    if(typeof onRestrictedChurchAttempt === 'function') onRestrictedChurchAttempt(context)
    showPrompt('error', `Restricted information: you cannot access ${context} for another church`)
  }

  function localCollectionCodeMeta(col){
    if(!col) return null
    const matches = (collectionCodes || []).filter(c=> c.column_name === col || c.code === col)
    if(!matches.length) return null
    const preferred = matches.find(c=> Number(c.church)===Number(scopedChurchId)) || matches.find(c=> c.church == null) || matches[0]
    return preferred || null
  }

  function localLabelForColumn(col){
    if(!col) return ''
    const found = localCollectionCodeMeta(col)
    if(found) return found.custom_collection_name || found.code || found.column_name || col
    return labelForColumn(col)
  }

  function mappedPreviewColumnsLocal(){
    const first = mappedPreview[0]
    if(!first) return []
    return Object.keys(first).filter(k=> {
      if(['collection_code','church','s2','source'].includes(k)) return false
      return localLabelForColumn(k) !== 'NA.'
    })
  }

  function currentChurchNameLocal(){
    return (churches || []).find(c=> Number(c.id)===Number(selectedChurch || scopedChurchId))?.name || String(selectedChurch || scopedChurchId || '-')
  }

  function uploadGridWrapStyle(maxHeight = 420){
    if(uploadGridMode === 'scrollable'){
      return { maxHeight, overflowX:'auto', overflowY:'auto', border:'1px solid #ddd', borderRadius:6 }
    }
    return { maxHeight, overflow:'auto', border:'1px solid #ddd' }
  }

  function uploadGridTableStyle(fill = true){
    if(uploadGridMode === 'scrollable'){
      return { width:'max-content', minWidth: fill ? '100%' : 'max-content', borderCollapse:'collapse' }
    }
    return { width: fill ? '100%' : 'auto', borderCollapse:'collapse' }
  }

  function formatPreviewDate(value){
    if(!value) return '-'
    const dt = new Date(value)
    if(Number.isNaN(dt.getTime())) return String(value)
    return new Intl.DateTimeFormat('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
    }).format(dt)
  }

  const visibleChurches = (churches || []).filter(c=> canAccessChurch(c.id))

  const quickDetailOptions = (collectionCodes || [])
    .filter(c=> {
      const churchOk = c && (c.church == null || Number(c.church) === Number(scopedChurchId))
      if(!churchOk) return false
      const col = String(c.column_name || '').toLowerCase()
      const codeLabel = String(c.code || '').trim().toUpperCase()
      if(!col) return false
      if(codeLabel === 'UNUSED') return false
      if(['collection_code', 'church', 'source', 'notes'].includes(col)) return false
      if(col === 's1' || col === 's2' || col === 's3' || col === 's4') return false
      return true
    })
    .sort((a,b)=>{
      const al = String(a.custom_collection_name || a.code || a.column_name || '').toLowerCase()
      const bl = String(b.custom_collection_name || b.code || b.column_name || '').toLowerCase()
      return al.localeCompare(bl)
    })

  function makeQuickRow(){
    return {
      id: Date.now() + Math.floor(Math.random() * 1000),
      name: '',
      collection_date: selectedDate || '',
      total_amount: '',
      detail_entries: [],
      selected_detail: '',
      selected_detail_amount: '',
    }
  }

  function updateQuickRow(rowId, patch){
    setQuickRows(prev=> prev.map(r=> r.id === rowId ? {...r, ...patch} : r))
  }

  function addQuickRow(){
    setQuickRows(prev=> [...prev, makeQuickRow()])
  }

  function removeQuickRow(rowId){
    setQuickRows(prev=> {
      const next = prev.filter(r=> r.id !== rowId)
      return next.length ? next : [makeQuickRow()]
    })
  }

  function addQuickDetailEntry(rowId){
    let warning = ''
    setQuickRows(prev=> prev.map(row=>{
      if(row.id !== rowId) return row
      const selectedDetail = String(row.selected_detail || '').trim()
      const selectedAmount = Number(String(row.selected_detail_amount || '').trim())
      if(!selectedDetail){
        warning = 'Select a collection detail item before clicking Add item.'
        return row
      }
      if(!Number.isFinite(selectedAmount) || selectedAmount <= 0){
        warning = 'Enter a valid collection detail amount greater than zero.'
        return row
      }
      if((row.detail_entries || []).some(e=> e.code === selectedDetail)){
        warning = 'This collection detail item is already added. Remove it first to change the amount.'
        return row
      }
      return {
        ...row,
        detail_entries: [...(row.detail_entries || []), { code: selectedDetail, amount: selectedAmount }],
        selected_detail: '',
        selected_detail_amount: '',
      }
    }))
    if(warning) showPrompt('warning', warning)
  }

  function removeQuickDetailEntry(rowId, code){
    setQuickRows(prev=> prev.map(row=>{
      if(row.id !== rowId) return row
      return {
        ...row,
        detail_entries: (row.detail_entries || []).filter(e=> e.code !== code),
      }
    }))
  }

  function quickRowDetailsTotal(row){
    return (row?.detail_entries || []).reduce((sum, e)=> sum + Number(e?.amount || 0), 0)
  }

  async function submitQuickRows(){
    if(!canManageCollections){
      showPrompt('warning', 'Your role can view the Collections menu but cannot submit collection rows.')
      return
    }
    const effectiveChurch = selectedChurch || scopedChurchId
    if(!effectiveChurch){
      showPrompt('warning', 'Select a church before submitting collection rows.')
      return
    }
    if(!quickDetailOptions.length){
      showPrompt('warning', 'No collection detail items are configured for this church. Add collection codes first.')
      return
    }

    const issues = []
    const rowsForBulk = []
    quickRows.forEach((r, idx)=>{
      const rowNo = idx + 1
      const name = String(r.name || '').trim()
      const dateText = String(r.collection_date || '').trim()
      const totalAmount = Number(String(r.total_amount || '').trim())
      const detailEntries = Array.isArray(r.detail_entries) ? r.detail_entries : []
      const detailTotal = detailEntries.reduce((sum, entry)=> sum + Number(entry?.amount || 0), 0)
      if(!name) issues.push(`Row ${rowNo}: Name is required.`)
      if(!dateText) issues.push(`Row ${rowNo}: Date of collection is required.`)
      if(!Number.isFinite(totalAmount) || totalAmount <= 0) issues.push(`Row ${rowNo}: Total collection amount must be greater than zero.`)
      if(!detailEntries.length) issues.push(`Row ${rowNo}: Add at least one collection detail item.`)
      if(Math.abs(detailTotal - totalAmount) > 0.009){
        issues.push(`Row ${rowNo}: Sum of detail amounts (${detailTotal.toFixed(2)}) does not match total collection (${totalAmount.toFixed(2)}).`)
      }
      const parsed = dateText ? new Date(dateText) : null
      if(parsed && !Number.isNaN(parsed.getTime())){
        detailEntries.forEach(entry=>{
          const detailCol = entry.code
          const detailAmount = Number(entry.amount || 0)
          const option = quickDetailOptions.find(o=> o.column_name === detailCol)
          if(!option) return
          rowsForBulk.push({
            collection_code: detailCol,
            church: effectiveChurch,
            s2: parsed.toISOString(),
            s4: name,
            s5: detailAmount,
            source: uploaderName || String(user?.username || ''),
            notes: `${option.custom_collection_name || option.code || option.column_name} | Total: ${totalAmount}`,
            [detailCol]: detailAmount,
          })
        })
      }
    })

    if(issues.length){
      showPrompt('warning', issues.join('\n'))
      return
    }
    if(!rowsForBulk.length){
      showPrompt('warning', 'No valid rows were prepared for submission.')
      return
    }

    setQuickSubmitting(true)
    try{
      const validateRes = await authFetch('http://localhost:8000/members_collections/validate', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify(rowsForBulk),
      })
      const validateData = await validateRes.json().catch(()=>({}))
      if(!validateRes.ok){
        throw new Error(validateData.detail || JSON.stringify(validateData) || 'Validation failed')
      }
      const validateErrors = Array.isArray(validateData.validation_errors) ? validateData.validation_errors : []
      if(validateErrors.length){
        setValidationErrors(validateErrors)
        setStep(4)
        showPrompt('warning', 'Quick form validation found issues. Review Step 4 for details.')
        return
      }

      const res = await authFetch('http://localhost:8000/members_collections/bulk', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify(rowsForBulk),
      })
      const data = await res.json().catch(()=>({}))
      if(!res.ok) throw new Error(data.detail || JSON.stringify(data) || 'Submit failed')
      showPrompt('success', `Saved ${data.inserted || 0} collection entries. You can keep adding more rows before closing the form.`)
      setQuickRows([makeQuickRow()])
    }catch(e){
      showPrompt('error', 'Quick collection submit failed: ' + (e?.message || String(e)))
    }finally{
      setQuickSubmitting(false)
    }
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
      <div style={{marginBottom:12, border:'1px solid #dbeafe', background:'#f8fbff', borderRadius:10, padding:12}}>
        <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', gap:8, flexWrap:'wrap'}}>
          <div>
            <div style={{fontWeight:800, color:'#0f172a'}}>Collection Form</div>
            <div style={{fontSize:12, color:'#475569'}}>Enter one or more rows manually and submit to the same collections API used by upload.</div>
          </div>
          <button type='button' onClick={()=>setShowQuickForm(v=>!v)}>{showQuickForm ? 'Close form' : 'Open form'}</button>
        </div>

        {showQuickForm && (
          <div style={{marginTop:12}}>
            {!canManageCollections && <div style={{marginBottom:10, color:'#7c2d12', background:'#fff7ed', border:'1px solid #fdba74', borderRadius:6, padding:8}}>Your account can access Collections but cannot submit entries. Ask an admin to grant Collections management rights.</div>}
            {quickRows.map((row, idx)=> (
              <div key={row.id} style={{border:'1px solid #d1d5db', borderRadius:8, padding:10, marginBottom:10, background:'#fff'}}>
                <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:8}}>
                  <strong style={{color:'#0f172a'}}>Collection row {idx + 1}</strong>
                  <button type='button' onClick={()=>removeQuickRow(row.id)} disabled={quickRows.length === 1 || quickSubmitting}>Remove</button>
                </div>
                <div style={{display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(190px, 1fr))', gap:8, marginBottom:8}}>
                  <div>
                    <label style={{display:'block', fontSize:12, color:'#334155', marginBottom:4}}>Name</label>
                    <input type='text' placeholder='Member or collection name' value={row.name} onChange={e=>updateQuickRow(row.id, {name: e.target.value})} disabled={quickSubmitting} style={{width:'100%'}} />
                  </div>
                  <div>
                    <label style={{display:'block', fontSize:12, color:'#334155', marginBottom:4}}>Date of collection</label>
                    <input type='date' value={row.collection_date} onChange={e=>updateQuickRow(row.id, {collection_date: e.target.value})} disabled={quickSubmitting} style={{width:'100%'}} />
                  </div>
                  <div>
                    <label style={{display:'block', fontSize:12, color:'#334155', marginBottom:4}}>Total collection amount</label>
                    <input type='number' min='0' step='0.01' placeholder='0.00' value={row.total_amount} onChange={e=>updateQuickRow(row.id, {total_amount: e.target.value})} disabled={quickSubmitting} style={{width:'100%'}} />
                  </div>
                </div>
                <div style={{border:'1px solid #e2e8f0', borderRadius:8, padding:10, background:'#f8fafc'}}>
                  <label style={{display:'block', fontSize:12, color:'#334155', marginBottom:6}}>Collection details</label>
                  <div style={{display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(180px, 1fr))', gap:8, alignItems:'end'}}>
                    <div>
                      <label style={{display:'block', fontSize:12, color:'#64748b', marginBottom:4}}>Collection item</label>
                      <select
                        value={row.selected_detail || ''}
                        onChange={e=>updateQuickRow(row.id, {selected_detail: e.target.value})}
                        disabled={quickSubmitting || !quickDetailOptions.length}
                        style={{width:'100%'}}
                      >
                        <option value=''>-- select collection item --</option>
                        {quickDetailOptions.map(opt=> (
                          <option key={opt.column_name} value={opt.column_name}>
                            {opt.custom_collection_name || opt.code || opt.column_name}
                          </option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label style={{display:'block', fontSize:12, color:'#64748b', marginBottom:4}}>Item amount</label>
                      <input type='number' min='0' step='0.01' placeholder='0.00' value={row.selected_detail_amount || ''} onChange={e=>updateQuickRow(row.id, {selected_detail_amount: e.target.value})} disabled={quickSubmitting} style={{width:'100%'}} />
                    </div>
                    <div>
                      <button type='button' onClick={()=>addQuickDetailEntry(row.id)} disabled={quickSubmitting || !quickDetailOptions.length}>Add item</button>
                    </div>
                  </div>
                  <div style={{marginTop:10, display:'flex', justifyContent:'space-between', alignItems:'center', gap:8, flexWrap:'wrap'}}>
                    <strong style={{fontSize:12, color:'#334155'}}>Added items</strong>
                    <span style={{fontSize:12, color:'#334155'}}>Details total: {quickRowDetailsTotal(row).toFixed(2)} | Expected total: {Number(row.total_amount || 0).toFixed(2)}</span>
                  </div>
                  {(!row.detail_entries || !row.detail_entries.length) ? (
                    <div style={{marginTop:6, fontSize:12, color:'#64748b'}}>No collection detail items added yet.</div>
                  ) : (
                    <div style={{marginTop:8, border:'1px solid #d1d5db', borderRadius:6, overflow:'hidden'}}>
                      <table style={{width:'100%', borderCollapse:'collapse'}}>
                        <thead>
                          <tr>
                            <th style={{textAlign:'left', padding:'6px 8px', borderBottom:'1px solid #e2e8f0'}}>Item</th>
                            <th style={{textAlign:'right', padding:'6px 8px', borderBottom:'1px solid #e2e8f0'}}>Amount</th>
                            <th style={{textAlign:'center', padding:'6px 8px', borderBottom:'1px solid #e2e8f0'}}>Action</th>
                          </tr>
                        </thead>
                        <tbody>
                          {row.detail_entries.map(entry=> (
                            <tr key={entry.code}>
                              <td style={{padding:'6px 8px', borderBottom:'1px solid #f1f5f9'}}>{localLabelForColumn(entry.code)}</td>
                              <td style={{padding:'6px 8px', borderBottom:'1px solid #f1f5f9', textAlign:'right'}}>{Number(entry.amount || 0).toFixed(2)}</td>
                              <td style={{padding:'6px 8px', borderBottom:'1px solid #f1f5f9', textAlign:'center'}}>
                                <button type='button' onClick={()=>removeQuickDetailEntry(row.id, entry.code)} disabled={quickSubmitting}>Remove</button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>
            ))}

            <div style={{display:'flex', gap:8, flexWrap:'wrap'}}>
              <button type='button' onClick={addQuickRow} disabled={quickSubmitting}>Add another row</button>
              <button type='button' onClick={submitQuickRows} disabled={quickSubmitting || !canManageCollections}>{quickSubmitting ? 'Submitting...' : 'Submit rows'}</button>
              <button type='button' onClick={()=>setShowQuickForm(false)} disabled={quickSubmitting}>Close form</button>
            </div>
          </div>
        )}
      </div>

      {promptState.open && (
        <div style={{marginBottom:10, border:'1px solid #d0d7de', borderLeft:`5px solid ${promptState.level==='error' ? '#d1242f' : promptState.level==='warning' ? '#d97706' : promptState.level==='success' ? '#2f7d32' : '#2563eb'}`, background:'#f8fafc', padding:12, borderRadius:8}}>
          <div style={{display:'flex', justifyContent:'space-between', gap:8, alignItems:'center', marginBottom:8}}>
            <strong style={{color:'#111827', fontSize:16, fontWeight:800, letterSpacing:'0.01em'}}>{promptState.level==='error' ? 'Error' : promptState.level==='warning' ? 'Warning' : promptState.level==='success' ? 'Success' : 'Info'}</strong>
            <div style={{display:'flex', gap:8}}>
              <button onClick={copyPromptMessage}>Copy message</button>
              <button onClick={()=> setPromptState(prev=> ({...prev, open:false}))}>Close</button>
            </div>
          </div>
          <div style={{width:'100%', minHeight:70, whiteSpace:'pre-wrap', fontFamily:'Segoe UI, Arial, sans-serif', fontSize:15, fontWeight:600, lineHeight:1.45, color:'#111827', padding:'10px 12px', border:'1px solid #cbd5e1', borderRadius:6, background:'#ffffff'}}>{promptState.message}</div>
          {copiedPrompt && <div style={{marginTop:8, color:'#14532d', fontWeight:700}}>Message copied to clipboard.</div>}
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
            <button type='button' onClick={saveSelectedCollectionDate} style={{marginLeft:8}}>Save Selection</button>
            <label style={{marginLeft:8}}>Church:</label>
            <select value={selectedChurch||''} onChange={e=>{
              const nextChurch = e.target.value
              if(!canAccessChurch(nextChurch)){ handleRestrictedChurch('collection upload'); return }
              setSelectedChurch(nextChurch)
            }}>
              <option value=''>-- select --</option>
              {visibleChurches.map(c=> <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
            <label style={{marginLeft:8}}>Uploader name:</label>
            <input value={uploaderName} readOnly disabled style={{opacity:1, color:'#111827', background:'#f3f4f6', fontWeight:600}} />
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
            <button type='button' onClick={saveSelectedCollectionDate} style={{marginLeft:8}}>Save Selection</button>
            <label style={{marginLeft:8}}>Church:</label>
            <select value={selectedChurch||''} onChange={e=>{
              const nextChurch = e.target.value
              if(!canAccessChurch(nextChurch)){ handleRestrictedChurch('collection upload'); return }
              setSelectedChurch(nextChurch); recomputeMappedPreview()
            }}>
              <option value=''>-- select --</option>
              {visibleChurches.map(c=> <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
            <label style={{marginLeft:8}}>Uploader name:</label>
            <input value={uploaderName} readOnly disabled style={{opacity:1, color:'#111827', background:'#f3f4f6', fontWeight:600}} />
          </div>
              <div style={{marginTop:8, ...uploadGridWrapStyle(380)}}>
                <table border={1} cellPadding={6} style={uploadGridTableStyle(false)}>
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
          <div style={{marginBottom:10, display:'flex', gap:16, flexWrap:'wrap', alignItems:'stretch', padding:'12px 14px', border:'1px solid #d1d5db', borderRadius:8, background:'#f8fafc'}}>
            <div style={{display:'flex', flexDirection:'column', minWidth:180}}>
              <div style={{fontSize:12, fontWeight:800, letterSpacing:'0.05em', textTransform:'uppercase', color:'#475569'}}>Church</div>
              <div style={{color:'#111827', fontWeight:700, fontSize:16, lineHeight:1.4}}>{currentChurchNameLocal()}</div>
            </div>
            <div style={{display:'flex', flexDirection:'column', minWidth:180}}>
              <div style={{fontSize:12, fontWeight:800, letterSpacing:'0.05em', textTransform:'uppercase', color:'#475569'}}>Tarehe</div>
              <div style={{color:'#111827', fontWeight:700, fontSize:16, lineHeight:1.4}}>{formatPreviewDate(selectedDate)}</div>
            </div>
            <div style={{display:'flex', flexDirection:'column', minWidth:220}}>
              <div style={{fontSize:12, fontWeight:800, letterSpacing:'0.05em', textTransform:'uppercase', color:'#475569'}}>Source</div>
              <div style={{color:'#111827', fontWeight:700, fontSize:16, lineHeight:1.4}}>{uploaderName || '-'}</div>
            </div>
          </div>
          {mappedPreviewMode==='scrollable' ? (
            <div style={{maxHeight:420, overflowX:'auto', overflowY:'auto', border:'1px solid #ddd', borderRadius:8}}>
              <table style={{width:'max-content', minWidth:'100%', borderCollapse:'separate', borderSpacing:0}}>
                <thead><tr>{mappedPreview[0] ? mappedPreviewColumnsLocal().map(k=> <th key={k} style={{position:'sticky', top:0, background:'#fff', zIndex:2, borderBottom:'1px solid #ddd', whiteSpace:'nowrap', minWidth:180}}>{localLabelForColumn(k)}</th>) : <th style={{position:'sticky', top:0, background:'#fff', zIndex:2, borderBottom:'1px solid #ddd'}}>No rows</th>}</tr></thead>
                <tbody>{mappedPreview.map((r,idx)=> (
                  <tr key={idx}>{mappedPreviewColumnsLocal().map(k=> <td key={k} style={{whiteSpace:'nowrap'}}><input value={r[k]||''} onChange={e=>{ const v=e.target.value; setMappedPreview(prev=>{ const nxt=[...prev]; nxt[idx] = {...nxt[idx], [k]: v}; return nxt }) }} style={{minWidth:180, width:180}} /></td>)}</tr>
                ))}</tbody>
              </table>
            </div>
          ) : (
            <div style={{maxHeight:420, overflowX:'auto', overflowY:'auto', border:'1px solid #ddd', borderRadius:8}}>
              <table style={{width:'100%', minWidth:'max-content', borderCollapse:'separate', borderSpacing:0}}>
                <thead><tr>{mappedPreview[0] ? mappedPreviewColumnsLocal().map(k=> <th key={k} style={{position:'sticky', top:0, background:'#fff', zIndex:2, borderBottom:'1px solid #ddd', whiteSpace:'nowrap'}}>{localLabelForColumn(k)}</th>) : <th style={{position:'sticky', top:0, background:'#fff', zIndex:2, borderBottom:'1px solid #ddd'}}>No rows</th>}</tr></thead>
                <tbody>{mappedPreview.map((r,idx)=> (
                  <tr key={idx}>{mappedPreviewColumnsLocal().map(k=> <td key={k}><input value={r[k]||''} onChange={e=>{ const v=e.target.value; setMappedPreview(prev=>{ const nxt=[...prev]; nxt[idx] = {...nxt[idx], [k]: v}; return nxt }) }} style={{minWidth:160}} /></td>)}</tr>
                ))}</tbody>
              </table>
            </div>
          )}
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
 