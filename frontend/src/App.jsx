import React, { useState, useEffect } from 'react'
import { jsPDF } from 'jspdf'
import autoTable from 'jspdf-autotable'
import { ToastContainer, toast } from 'react-toastify'
import 'react-toastify/dist/ReactToastify.css'
import './styles.css'
import './adminkit-modal.scss'
import './adminkit-dark.css'
import usePermissions from './hooks/usePermissions'
import Alert from './components/Alert'
import LoginScreen from './components/LoginScreen'
import AppHeader from './components/AppHeader'
import SidebarNav from './components/SidebarNav'
import PreferencesPage from './components/PreferencesPage'
import AdminUsersPage from './components/AdminUsersPage'
import SettingsPage from './components/SettingsPage'
import MembersPage from './components/MembersPage'
import CoreApp from './components/members-core/CoreApp'
import SelfRegistrationPage from './components/SelfRegistrationPage'
import MemberPortalApp from './components/member-portal/MemberPortalApp'
import MembersCollectionsPage from './components/MembersCollectionsPage'
import CollectionsPage from './components/CollectionsPage'
import ReportsPage from './components/ReportsPage'
import AppFooter from './components/AppFooter'

// `backend_old` is retired from the running app — every call still written against
// this literal 'http://localhost:8000' prefix throughout the codebase gets rewritten
// below to the new api/ backend (port 8001) instead. Routes that don't exist yet on
// the new backend will 404/error; the pages that depend on them are marked disabled
// in SidebarNav until they're rebuilt against api/.
const LOCAL_API_BASE = 'http://localhost:8000'
const API_BASE = (import.meta.env.VITE_API_BASE_URL || '').trim().replace(/\/$/, '')

function rewriteApiUrl(input) {
  // Prefer explicit env base; otherwise inherit current page protocol to avoid mixed-content fetch failures.
  const base = API_BASE || (typeof window !== 'undefined' ? `${window.location.protocol}//${window.location.hostname}:8001` : LOCAL_API_BASE)
  if (typeof input === 'string' && input.startsWith(LOCAL_API_BASE)) {
    return `${base}${input.slice(LOCAL_API_BASE.length)}`
  }
  if (typeof Request !== 'undefined' && input instanceof Request && input.url.startsWith(LOCAL_API_BASE)) {
    return new Request(`${base}${input.url.slice(LOCAL_API_BASE.length)}`, input)
  }
  return input
}

if (typeof window !== 'undefined' && !window.__saypyFetchPatched) {
  const nativeFetch = window.fetch.bind(window)
  window.fetch = (input, init) => nativeFetch(rewriteApiUrl(input), init)
  window.__saypyFetchPatched = true
}

if (typeof window !== 'undefined' && !window.__saypyDatePickerIconClickPatched) {
  document.addEventListener('pointerdown', (event) => {
    const target = event.target
    if (!(target instanceof HTMLInputElement)) return
    if (target.type !== 'date' || target.disabled || target.readOnly) return
    const rect = target.getBoundingClientRect()
    const clickX = event.clientX - rect.left
    const iconHitZoneStart = rect.width - 40
    if (clickX < iconHitZoneStart) return
    if (typeof target.showPicker === 'function') {
      try { target.showPicker() } catch (e) { }
    }
  })
  window.__saypyDatePickerIconClickPatched = true
}

if (typeof window !== 'undefined' && !window.__saypyDatePickerTonePatched) {
  const DATE_INPUT_LIGHT_CLASS = 'date-input-on-light'
  const DATE_INPUT_DARK_CLASS = 'date-input-on-dark'

  const classifyDateInputTone = (el) => {
    if (!(el instanceof HTMLInputElement) || el.type !== 'date') return
    const style = window.getComputedStyle(el)
    const bg = String(style.backgroundColor || '')
    const parts = bg.match(/[\d.]+/g)
    if (!parts || parts.length < 3) return
    const r = Number(parts[0])
    const g = Number(parts[1])
    const b = Number(parts[2])
    const luminance = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255
    const isLight = Number.isFinite(luminance) && luminance >= 0.6
    el.classList.remove(DATE_INPUT_LIGHT_CLASS, DATE_INPUT_DARK_CLASS)
    el.classList.add(isLight ? DATE_INPUT_LIGHT_CLASS : DATE_INPUT_DARK_CLASS)
  }

  const classifyAllDateInputs = () => {
    document.querySelectorAll('input[type="date"]').forEach(classifyDateInputTone)
  }

  document.addEventListener('focusin', (event) => {
    const target = event.target
    if (target instanceof HTMLInputElement && target.type === 'date') {
      classifyDateInputTone(target)
    }
  })

  window.addEventListener('resize', classifyAllDateInputs)
  window.addEventListener('load', classifyAllDateInputs)
  setTimeout(classifyAllDateInputs, 0)
  window.__saypyDatePickerTonePatched = true
}

// Lightweight URL sync (path-segment based) — keeps whichever
// page/Setup-menu is active reflected in the URL path and restorable on refresh.
const CORE_SETUP_MENUS = new Set(['dashboard', 'member_management', 'church_management', 'user_management', 'configuration', 'collection_management'])

function parseUrlState() {
  if (typeof window === 'undefined') return { page: 'members_core', setupRoute: { menu: 'dashboard' } }
  const pathname = window.location.pathname.replace(/\/$/, '')
  if (!pathname || pathname === '/' || pathname === '/dashboard') {
    // Default landing spot is Setup Dashboard
    return { page: 'members_core', setupRoute: { menu: 'dashboard' } }
  }

  // Parse path: /menu/sub/subsub or /page
  const parts = pathname.slice(1).split('/').filter(Boolean)
  const firstSegment = parts[0]

  if (CORE_SETUP_MENUS.has(firstSegment)) {
    const menu = firstSegment
    const sub = parts[1] || null
    const subsub = parts[2] || null
    const setupRoute = { menu }
    if (sub) setupRoute.sub = sub
    if (subsub) setupRoute.subsub = subsub
    return { page: 'members_core', setupRoute }
  } else {
    // page is the pathname without the leading slash
    const page = pathname.slice(1)
    return { page, setupRoute: { menu: 'dashboard' } }
  }
}

function buildUrlFromState(page, setupRoute) {
  if (page === 'members_core' && setupRoute?.menu) {
    if (setupRoute.menu === 'dashboard') {
      return '/dashboard'
    }
    let path = `/${setupRoute.menu}`
    if (setupRoute.sub) {
      path += `/${setupRoute.sub}`
      if (setupRoute.subsub) {
        path += `/${setupRoute.subsub}`
      }
    }
    return path
  }
  return `/${page}`
}

// Single-file frontend with Login, RBAC, and pages for Admin/Members/Collections/Reports
export default function App() {
  const [page, setPage] = useState(() => parseUrlState().page)
  // Which leaf of the Setup nav tree (Member/Church/User Management, Configuration)
  // is active — set by SidebarNav, read by CoreApp to decide what to render.
  const [setupRoute, setSetupRoute] = useState(() => parseUrlState().setupRoute)

  // Keep the URL in sync with page/setupRoute
  useEffect(() => {
    const url = buildUrlFromState(page, setupRoute)
    const current = window.location.pathname
    if (url !== current) {
      window.history.pushState({ page, setupRoute }, '', url)
    }
  }, [page, setupRoute])

  useEffect(() => {
    function handlePopState() {
      const state = parseUrlState()
      setPage(state.page)
      setSetupRoute(state.setupRoute)
    }
    window.addEventListener('popstate', handlePopState)
    return () => window.removeEventListener('popstate', handlePopState)
  }, [])
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [token, setToken] = useState(localStorage.getItem('token') || '')
  const [user, setUser] = useState(JSON.parse(localStorage.getItem('user') || 'null'))
  const [status, setStatus] = useState('')

  useEffect(() => {
    if (status) {
      const meta = statusMeta()
      if (meta) {
        const lower = status.toLowerCase()
        let type = meta.level
        if (lower.includes('warning') || lower.includes('warn') || lower.includes('attention') || lower.includes('duplicate')) {
          type = 'warning'
        } else if (lower.includes('info') || lower.includes('notice')) {
          type = 'info'
        }
        toast(meta.body || status, {
          type: type,
          position: "top-right",
          autoClose: 4000,
          theme: document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light'
        })
      }
      setStatus('')
    }
  }, [status])
  const [rolesCatalog, setRolesCatalog] = useState([])
  const currentUserChurchId = user && user.church != null ? Number(user.church) : null
  const { hasRoleRight, isAdmin, isUploader, roleLabel, currentRolePolicy } = usePermissions(user, rolesCatalog)

  // AdminKit's CSS is only attached while logged in — LoginScreen has no
  // shared DOM ancestor with the app shell (this is an early, exclusive
  // return below), so this guarantees zero style leakage onto the auth page
  // without any CSS-scoping tricks.
  useEffect(() => {
    const ADMINKIT_LINK_ID = 'adminkit-app-css'
    const INTER_FONT_LINK_ID = 'adminkit-inter-font'
    if (user) {
      if (!document.getElementById(ADMINKIT_LINK_ID)) {
        const link = document.createElement('link')
        link.id = ADMINKIT_LINK_ID
        link.rel = 'stylesheet'
        link.href = '/adminkit/app.css'
        document.head.appendChild(link)
      }
      if (!document.getElementById(INTER_FONT_LINK_ID)) {
        const fontLink = document.createElement('link')
        fontLink.id = INTER_FONT_LINK_ID
        fontLink.rel = 'stylesheet'
        fontLink.href = 'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap'
        document.head.appendChild(fontLink)
      }
    } else {
      document.getElementById(ADMINKIT_LINK_ID)?.remove()
      document.getElementById(INTER_FONT_LINK_ID)?.remove()
    }
  }, [user])

  function canAccessChurch(churchId) {
    if (churchId === null || churchId === undefined || churchId === '') return true
    if (currentUserChurchId === null || Number.isNaN(currentUserChurchId)) return true
    return Number(churchId) === currentUserChurchId
  }

  function denyRestrictedChurchAccess(context) {
    setStatus(`Restricted information: you cannot access ${context} for another church`)
  }

  function passwordPolicyMessage(password) {
    const p = String(password || '')
    if (p.length < 8) return 'Password must be at least 8 characters long'
    if (!/[A-Za-z]/.test(p)) return 'Password must include at least one letter'
    if (!/[0-9]/.test(p)) return 'Password must include at least one number'
    if (!/[^A-Za-z0-9]/.test(p)) return 'Password must include at least one special character'
    return ''
  }
  const PASSWORD_HINT = 'Password: minimum 8 characters, include at least 1 letter, 1 number, and 1 special character.'

  function roleOptionsForCurrentUser() {
    const dynamicRoles = (rolesCatalog || []).map(r => String(r.role || '').toLowerCase()).filter(Boolean)
    if (dynamicRoles.length) {
      const me = String(user?.role || '').toLowerCase()
      if (me === 'system_admin') return dynamicRoles
      return dynamicRoles.filter(r => r !== 'system_admin')
    }
    const me = String(user?.role || '').toLowerCase()
    if (me === 'system_admin') {
      return ['admin', 'treasurer', 'head_treasurer', 'pastor', 'elder', 'data_steward', 'uploader', 'viewer', 'system_admin']
    }
    return ['admin', 'treasurer', 'head_treasurer', 'pastor', 'elder', 'data_steward', 'uploader', 'viewer']
  }

  // ----- Shared data -----
  const [churches, setChurches] = useState([])
  const [appName, setAppName] = useState('StewardHub')  // Custom app name for the church
  const [systemName, setSystemName] = useState('StewardHub')
  const [editingSystemName, setEditingSystemName] = useState('')
  const [showEditSystemName, setShowEditSystemName] = useState(false)

  async function fetchSystemName() {
    try {
      const res = await fetch('http://localhost:8000/config')
      const data = await res.json().catch(() => ({}))
      if (res.ok && data.system_name) setSystemName(data.system_name)
    } catch (e) { }
  }

  async function submitSystemName() {
    try {
      const res = await fetch('http://localhost:8000/config/system_name', {
        method: 'PUT',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ system_name: editingSystemName })
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setSystemName(editingSystemName)
      setShowEditSystemName(false)
      setStatus('System name updated')
    } catch (e) { setStatus('Update system name failed: ' + e.message) }
  }

  useEffect(() => {
    fetchSystemName()
  }, [])

  useEffect(() => {
    // Unlike backend_old, api/'s GET /churches requires auth, so this can only
    // succeed once `token` is set — re-run whenever login state changes.
    fetchChurches()
  }, [user, token])

  useEffect(() => {
    if (user) fetchRoles()
  }, [user, token])

  // Fetch app_name when user logs in and has a church assigned
  useEffect(() => {
    if (user && currentUserChurchId) {
      fetchAppName()
    } else if (!user) {
      setAppName('StewardHub')
    }
  }, [user, currentUserChurchId])

  useEffect(() => {
    if (token) { localStorage.setItem('token', token) }
    else { localStorage.removeItem('token') }
    if (user) { localStorage.setItem('user', JSON.stringify(user)) }
    else { localStorage.removeItem('user') }
  }, [token, user])

  function authHeaders(extra = {}) {
    const h = { ...extra }
    if (token) h['Authorization'] = `Bearer ${token}`
    return h
  }

  async function fetchChurches() {
    // api/'s GET /churches requires auth, unlike backend_old's public version — pass
    // the token when we have one, and guard against non-array error-body responses.
    try {
      const res = await fetch('http://localhost:8000/churches', { headers: authHeaders() })
      const data = await res.json().catch(() => [])
      if (res.ok && Array.isArray(data)) setChurches(data)
    } catch (e) { }
  }

  async function fetchRoles() {
    try {
      const res = await fetch('http://localhost:8000/roles', { headers: authHeaders() })
      const data = await res.json().catch(() => [])
      if (res.ok && Array.isArray(data)) setRolesCatalog(data)
    } catch (e) {
      // keep fallback hardcoded roles
    }
  }

  async function fetchAppName() {
    try {
      const churchId = currentUserChurchId || (user && user.church)
      if (!churchId) return
      const res = await fetch(`http://localhost:8000/churches/${churchId}/app_name`, { headers: authHeaders() })
      const data = await res.json().catch(() => ({}))
      if (res.ok && data.app_name) {
        setAppName(data.app_name)
      }
    } catch (e) {
      // Silently fail; keep default app name
    }
  }

  // ----- Login / token management -----
  const [loginUser, setLoginUser] = useState('')
  const [loginPass, setLoginPass] = useState('')
  async function doLogin() {
    try {
      // api/'s auth contract differs from backend_old's: POST /auth/login (not
      // /users/login), and the response is { access_token, user } (not { token, user }),
      // with the user shaped as { id, username, full_name, email, role, church_id, ... }
      // rather than backend_old's { first_name, last_name, church, ... }.
      const res = await fetch('http://localhost:8000/auth/login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: loginUser, password: loginPass }) })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setToken(data.access_token)
      setUser({ ...data.user, church: data.user.church_id, first_name: data.user.full_name })
      setStatus('Logged in')
      // `page` already reflects the URL (or defaults to Setup) from initial
      // load — don't clobber a deep link by forcing navigation here.
    } catch (e) { setStatus('Login failed: ' + e.message) }
  }
  function logout() { setToken(''); setUser(null); setStatus('Logged out'); }

  // ----- Admin: Settings -----
  const [editingAppName, setEditingAppName] = useState('')
  const [showEditAppName, setShowEditAppName] = useState(false)
  const [localCollectionCodes, setLocalCollectionCodes] = useState([])
  const [newCodeColumn, setNewCodeColumn] = useState('')
  const [newCodeLabel, setNewCodeLabel] = useState('')
  const [newCodeCustomName, setNewCodeCustomName] = useState('')
  const [newCodeScope, setNewCodeScope] = useState('')
  const [newCodeSplitPct, setNewCodeSplitPct] = useState('')
  const [editingCodeId, setEditingCodeId] = useState(null)
  const [editCodeForm, setEditCodeForm] = useState({ column_name: '', code: '', custom_collection_name: '', scope: '', conference_split_pct: '' })
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
    can_delete_members_collections: false,
    can_unlock_members_collections: false,
    can_view_collection_codes: false,
    can_view_reports: false,
    can_manage_settings: false,
    can_manage_roles: false,
  })

  async function submitAppName() {
    try {
      if (!currentUserChurchId) { setStatus('Error: Unable to determine church'); return }
      const url = `http://localhost:8000/churches/${currentUserChurchId}/app_name`
      console.log('Updating app_name at:', url, 'with:', { app_name: editingAppName })
      const res = await fetch(url, {
        method: 'PUT',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ app_name: editingAppName })
      })
      console.log('Response status:', res.status, res.statusText)
      const data = await res.json().catch(() => ({}))
      console.log('Response data:', data)
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setAppName(editingAppName)
      setShowEditAppName(false)
      setStatus('App name updated successfully')
    } catch (e) {
      console.error('submitAppName error:', e)
      setStatus('Update app name failed: ' + e.message)
    }
  }

  async function createLocalCode() {
    try {
      if (!currentUserChurchId) { setStatus('Error: Unable to determine church'); return }
      if (!newCodeColumn.trim()) { setStatus('Error: Column name is required'); return }
      if (!newCodeLabel.trim()) { setStatus('Error: Code label is required'); return }
      const res = await fetch(`http://localhost:8000/churches/${currentUserChurchId}/collection_codes`, {
        method: 'POST',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ column_name: newCodeColumn, code: newCodeLabel, custom_collection_name: (newCodeCustomName || null), scope: (newCodeScope || null), conference_split_pct: (newCodeSplitPct !== '' ? Number(newCodeSplitPct) : null) })
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setNewCodeColumn('')
      setNewCodeLabel('')
      setNewCodeCustomName('')
      setNewCodeScope('')
      setNewCodeSplitPct('')
      fetchLocalCodes()
      setStatus('Code created successfully')
    } catch (e) { setStatus('Create code failed: ' + e.message) }
  }

  async function updateLocalCode() {
    try {
      if (!currentUserChurchId) { setStatus('Error: Unable to determine church'); return }
      if (!editingCodeId) { setStatus('Error: No code selected'); return }
      const res = await fetch(`http://localhost:8000/churches/${currentUserChurchId}/collection_codes/${editingCodeId}`, {
        method: 'PUT',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ column_name: editCodeForm.column_name, code: editCodeForm.code, custom_collection_name: (editCodeForm.custom_collection_name || null), scope: (editCodeForm.scope || null), conference_split_pct: (editCodeForm.conference_split_pct !== '' && editCodeForm.conference_split_pct != null ? Number(editCodeForm.conference_split_pct) : null) })
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setEditingCodeId(null)
      setEditCodeForm({ column_name: '', code: '', custom_collection_name: '', scope: '', conference_split_pct: '' })
      fetchLocalCodes()
      setStatus('Code updated successfully')
    } catch (e) { setStatus('Update code failed: ' + e.message) }
  }

  async function deleteLocalCode(codeId) {
    try {
      if (!currentUserChurchId) { setStatus('Error: Unable to determine church'); return }
      if (!window.confirm('Are you sure you want to delete this code?')) return
      const res = await fetch(`http://localhost:8000/churches/${currentUserChurchId}/collection_codes/${codeId}`, {
        method: 'DELETE',
        headers: authHeaders()
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      fetchLocalCodes()
      setStatus('Code deleted successfully')
    } catch (e) { setStatus('Delete code failed: ' + e.message) }
  }

  async function fetchLocalCodes() {
    try {
      if (!currentUserChurchId) { return }
      // Fetch church-scoped codes directly from backend (local + global).
      const res = await fetch(`http://localhost:8000/churches/${currentUserChurchId}/collection_codes`, { headers: authHeaders() })
      const data = await res.json().catch(() => [])
      if (res.ok) {
        const localCodes = data
          .filter(c => c.church == null || Number(c.church) === Number(currentUserChurchId))
          .sort((a, b) => {
            const aLocal = Number(a.church) === Number(currentUserChurchId) ? 0 : 1
            const bLocal = Number(b.church) === Number(currentUserChurchId) ? 0 : 1
            if (aLocal !== bLocal) return aLocal - bLocal
            return Number(a.id || 0) - Number(b.id || 0)
          })
        setLocalCollectionCodes(localCodes)
      } else {
        setStatus('Failed to load collection codes: ' + (data?.detail || `HTTP ${res.status}`))
      }
    } catch (e) { setStatus('Failed to load collection codes: ' + e.message) }
  }

  async function createRolePolicy() {
    try {
      if (String(user?.role || '').toLowerCase() !== 'system_admin') {
        setStatus('Only system admin can define roles')
        return
      }
      const roleName = String(newRole.role || '').trim().toLowerCase()
      if (!roleName) { setStatus('Role key is required'); return }
      const res = await fetch('http://localhost:8000/roles', {
        method: 'POST',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...newRole, role: roleName })
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus('Role created')
      setNewRole({
        role: '',
        display_name: '',
        can_view_dashboard: true,
        can_manage_users: false,
        can_manage_members: false,
        can_manage_collections: false,
        can_manage_members_collections: false,
        can_delete_members_collections: false,
        can_unlock_members_collections: false,
        can_view_collection_codes: false,
        can_view_reports: false,
        can_manage_settings: false,
        can_manage_roles: false,
      })
      await fetchRoles()
    } catch (e) { setStatus('Create role failed: ' + e.message) }
  }

  async function saveRolePolicy() {
    try {
      if (!editingRole) { return }
      if (String(user?.role || '').toLowerCase() !== 'system_admin') {
        setStatus('Only system admin can update roles')
        return
      }
      const roleName = String(editingRole.role || '').trim().toLowerCase()
      const res = await fetch(`http://localhost:8000/roles/${encodeURIComponent(roleName)}`, {
        method: 'PUT',
        headers: { ...authHeaders(), 'Content-Type': 'application/json' },
        body: JSON.stringify(editingRole)
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus('Role updated')
      setEditingRole(null)
      await fetchRoles()
    } catch (e) { setStatus('Update role failed: ' + e.message) }
  }

  async function removeRolePolicy(roleName) {
    try {
      if (String(user?.role || '').toLowerCase() !== 'system_admin') {
        setStatus('Only system admin can delete roles')
        return
      }
      if (!window.confirm(`Delete role ${roleName}?`)) return
      const res = await fetch(`http://localhost:8000/roles/${encodeURIComponent(roleName)}`, {
        method: 'DELETE',
        headers: authHeaders(),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus('Role deleted')
      await fetchRoles()
    } catch (e) { setStatus('Delete role failed: ' + e.message) }
  }

  useEffect(() => {
    if (page === 'settings' && currentUserChurchId) {
      setEditingAppName(appName)
    }
  }, [page, currentUserChurchId])

  useEffect(() => {
    if (page === 'collections' && currentUserChurchId && (hasRoleRight('can_manage_collections', true) || hasRoleRight('can_view_collection_codes', true))) {
      fetchLocalCodes()
    }
  }, [page, currentUserChurchId, rolesCatalog, user])

  useEffect(() => {
    if (!user || status !== 'Logged in') return
    const clearLoginStatus = () => {
      setStatus(prev => prev === 'Logged in' ? '' : prev)
    }
    window.addEventListener('pointerdown', clearLoginStatus, { once: true })
    window.addEventListener('keydown', clearLoginStatus, { once: true })
    return () => {
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
  const [userEditForm, setUserEditForm] = useState({ username: '', first_name: '', middle_name: '', last_name: '', email: '', phone: '', role: 'uploader', church: '' })
  const [resetUser, setResetUser] = useState(null)
  const [resetPassword, setResetPassword] = useState('')
  const [showOwnPasswordForm, setShowOwnPasswordForm] = useState(false)
  const [ownCurrentPassword, setOwnCurrentPassword] = useState('')
  const [ownNewPassword, setOwnNewPassword] = useState('')
  const [ownConfirmPassword, setOwnConfirmPassword] = useState('')
  async function fetchUsers() {
    try {
      const res = await fetch('http://localhost:8000/users', { headers: authHeaders() })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        const detail = (data && data.detail) ? String(data.detail) : `HTTP ${res.status}`
        if (res.status === 401) {
          logout()
          throw new Error('Session expired. Please login again. ' + detail)
        }
        throw new Error(detail)
      }
      setUsersList(data)
    } catch (e) { setStatus('Fetch users failed: ' + e.message) }
  }
  async function createUser(username, password, church, role, firstName = '', middleName = '', lastName = '', email = '', phone = '') {
    try {
      if (!canAccessChurch(church)) { denyRestrictedChurchAccess('user creation'); return }
      const policyMsg = passwordPolicyMessage(password)
      if (policyMsg) { setStatus('Create user failed: ' + policyMsg); return }
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
      const res = await fetch('http://localhost:8000/users/register', { method: 'POST', headers: authHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify(body) })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus('User created')
      await fetchUsers()
    } catch (e) { setStatus('Create user failed: ' + e.message) }
  }

  function beginEditUser(u) {
    if (!u) return
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

  async function saveUserEdit() {
    if (!editingUser) return
    try {
      if (!canAccessChurch(userEditForm.church)) { denyRestrictedChurchAccess('user update'); return }
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
        method: 'PUT',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify(payload)
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus('User updated')
      setEditingUser(null)
      await fetchUsers()
    } catch (e) { setStatus('Update user failed: ' + e.message) }
  }

  async function submitResetPassword() {
    if (!resetUser) return
    try {
      const policyMsg = passwordPolicyMessage(resetPassword)
      if (policyMsg) {
        setStatus('Password reset failed: ' + policyMsg)
        return
      }
      if (!canAccessChurch(resetUser.church)) { denyRestrictedChurchAccess('password reset'); return }
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
        method: 'PUT',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify(payload)
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus(`Password reset for ${resetUser.username}`)
      setResetUser(null)
      setResetPassword('')
      await fetchUsers()
    } catch (e) { setStatus('Password reset failed: ' + e.message) }
  }

  async function submitOwnPasswordReset() {
    try {
      if (!ownCurrentPassword || !ownNewPassword) {
        setStatus('Password change failed: current and new password are required')
        return
      }
      const policyMsg = passwordPolicyMessage(ownNewPassword)
      if (policyMsg) {
        setStatus('Password change failed: ' + policyMsg)
        return
      }
      if (ownNewPassword !== ownConfirmPassword) {
        setStatus('Password change failed: password confirmation does not match')
        return
      }
      const res = await fetch('http://localhost:8000/users/me/reset-password', {
        method: 'POST',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ current_password: ownCurrentPassword, new_password: ownNewPassword })
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data) || `HTTP ${res.status}`)
      setStatus('Password changed successfully')
      setShowOwnPasswordForm(false)
      setOwnCurrentPassword('')
      setOwnNewPassword('')
      setOwnConfirmPassword('')
    } catch (e) {
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
  async function fetchMembers(q = '') {
    try {
      const url = q ? `http://localhost:8000/members?q=${encodeURIComponent(q)}` : 'http://localhost:8000/members'
      const res = await fetch(url, { headers: authHeaders() })
      const data = await res.json()
      setMembers(data)
      if (data && data.length && (!membersFields || membersFields.length === 0)) {
        // derive fields from first row, exclude internal timestamps
        const keys = Object.keys(data[0]).filter(k => k !== 'created_at' && k !== 'id')
        setMembersFields(keys)
        if (!membersVisibleCols.length) {
          const defaults = ['sno', 'MEMBER_NAME', 'MEMBER_ID', 'PHONE', 'church'].filter(k => keys.includes(k))
          setMembersVisibleCols(defaults.length ? defaults : keys)
        }
      }
    } catch (e) { setStatus('Failed to load members: ' + e.message) }
  }

  function nextMemberNumericValue(candidateKeys, fallbackStart = 1) {
    const nums = (members || [])
      .map((m) => {
        for (const k of candidateKeys) {
          const raw = m?.[k]
          const n = Number(raw)
          if (Number.isFinite(n) && n > 0) return n
        }
        return null
      })
      .filter((n) => n !== null)
    if (!nums.length) return fallbackStart
    return Math.max(...nums) + 1
  }
  async function updateMember(id, payload) {
    try {
      const res = await fetch(`http://localhost:8000/members/${id}`, { method: 'PUT', headers: authHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify(payload) })
      if (!res.ok) throw new Error('Failed')
      setStatus('Member updated')
      await fetchMembers()
    } catch (e) { setStatus('Update failed: ' + e.message) }
  }

  // Members UI local state (was missing and caused runtime errors)
  const [showMemberForm, setShowMemberForm] = useState(false)
  const [editingMember, setEditingMember] = useState(null)
  // Map Member / Map Families state
  const [mapMode, setMapMode] = useState(null) // null | 'member' | 'family'
  const [mapTargetMember, setMapTargetMember] = useState(null)
  const [mapFilter, setMapFilter] = useState('')
  const [mapRight, setMapRight] = useState([]) // array of member objects on RHS
  const [mapDesignated, setMapDesignated] = useState(null) // id of designated unique/family name
  const [mapSaveMessage, setMapSaveMessage] = useState('')
  const [memberForm, setMemberForm] = useState({})
  const [showMemberGroupInfo, setShowMemberGroupInfo] = useState(false)
  const [showMembershipInfo, setShowMembershipInfo] = useState(false)
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
  const [mcFilterPhone, setMcFilterPhone] = useState('')
  const [mcFilterVerified, setMcFilterVerified] = useState('')
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
    phone: '',
    verified: '',
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
  const [showVerifyMapping, setShowVerifyMapping] = useState(false)
  const [verifyMappingRows, setVerifyMappingRows] = useState([])
  const [verifySavingRowId, setVerifySavingRowId] = useState(null)
  const [verifySavingAll, setVerifySavingAll] = useState(false)
  const [deleteRequests, setDeleteRequests] = useState([])
  const [deleteRequestsLoading, setDeleteRequestsLoading] = useState(false)
  const [deleteRequestStatusFilter, setDeleteRequestStatusFilter] = useState('pending')
  const [pendingDeleteRequestsCount, setPendingDeleteRequestsCount] = useState(0)
  // Load members when navigating to Members page
  useEffect(() => {
    if (page === 'members') fetchMembers('')
  }, [page])

  useEffect(() => {
    const prefKey = membersPrefColumnsKey()
    if (!prefKey || !membersFields.length) return
    try {
      const raw = localStorage.getItem(prefKey)
      if (!raw) return
      const parsed = JSON.parse(raw)
      if (Array.isArray(parsed)) {
        const allowed = new Set(getMembersAllColumns())
        const cols = parsed.filter(c => allowed.has(c))
        if (cols.length) setMembersVisibleCols(cols)
      }
    } catch (e) { }
  }, [user?.id, user?.username, membersFields.join('|')])

  useEffect(() => {
    const prefKey = membersCollectionsPrefColumnsKey()
    if (!prefKey || !membersCollectionsFields.length) return
    try {
      const raw = localStorage.getItem(prefKey)
      if (!raw) return
      const parsed = JSON.parse(raw)
      if (Array.isArray(parsed)) {
        const allowed = new Set(getMcAllColumns())
        const cols = parsed.filter(c => allowed.has(c))
        if (cols.length) setMcVisibleCols(cols)
      }
    } catch (e) { }
  }, [user?.id, user?.username, membersCollectionsFields.join('|')])

  // Load members_collections when navigating to the Members Collections page
  useEffect(() => {
    if (page === 'members_collections') {
      setMcSortKey('id')
      setMcSortDir('desc')
      setMcPage(1)
      setMcPageSize(10)
      setStatus('Loading collections...')
      fetchMembersCollections({ limit: getDefaultMcFetchLimit(1, 10) }).then(() => {
        // Keep error/status set by fetchMembersCollections when request fails.
      }).catch(() => { })
      if (hasRoleRight('can_delete_members_collections', false) || hasRoleRight('can_unlock_members_collections', false)) {
        fetchDeleteRequests(deleteRequestStatusFilter).catch(() => { })
        fetchPendingDeleteRequestsCount().catch(() => { })
      }
    }
  }, [page, currentUserChurchId, churches.length])

  // Global error handlers to avoid uncaught errors causing dev overlay crashes
  useEffect(() => {
    const onErr = (e) => {
      console.error('Global error', e);
      try { setStatus('Error: ' + (e && e.message ? e.message : String(e))) } catch (_) { }
    }
    const onRej = (e) => {
      console.error('Unhandled rejection', e);
      try { setStatus('Unhandled rejection: ' + (e && e.reason ? (e.reason.message || String(e.reason)) : String(e))) } catch (_) { }
    }
    window.addEventListener('error', onErr)
    window.addEventListener('unhandledrejection', onRej)
    return () => { window.removeEventListener('error', onErr); window.removeEventListener('unhandledrejection', onRej) }
  }, [])

  // A 401 from any members-core/api.js call (JWT expired/invalid) dispatches
  // this event — logging out here, at the top level, is what actually drops
  // `user` to null and takes the whole admin shell (sidebar included) off
  // screen in favor of the plain LoginScreen. Without this, only api.js's
  // own internal session was being cleared, leaving CoreApp's rarely-used
  // inner LoginGate to render *inside* the still-visible sidebar/header.
  useEffect(() => {
    function onSessionExpired() {
      setToken(''); setUser(null); setStatus('Your session has expired — please sign in again.')
    }
    window.addEventListener('saypy:session-expired', onSessionExpired)
    return () => window.removeEventListener('saypy:session-expired', onSessionExpired)
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

  useEffect(() => { fetchCodes(); fetchMembersLocal() }, [])
  useEffect(() => { if (step === 3 || step === 2) recomputeMappedPreview() }, [step, mapping, fullPreview, selectedChurch, selectedDate, uploaderName])
  useEffect(() => {
    if (user) {
      const preferredName = displayUserName(user) || String(user?.username || '')
      setUploaderName(preferredName)
    }
  }, [user])

  // Helper: human-friendly label for a column using `collectionCodes` mapping
  function collectionCodeMeta(col) {
    if (!col) return null
    const matches = (collectionCodes || []).filter(c => c.column_name === col || c.code === col)
    if (!matches.length) return null
    const preferred = matches.find(c => Number(c.church) === Number(currentUserChurchId)) || matches.find(c => c.church == null) || matches[0]
    return preferred || null
  }

  async function createTransferChurchOption() {
    try {
      const canCreateChurch = String(user?.role || '').toLowerCase() === 'system_admin' || String(user?.username || '').toLowerCase() === 'saypy_admin'
      if (!canCreateChurch) {
        setStatus('Only system admin can add new churches')
        return
      }
      const name = String(newTransferChurchName || '').trim()
      if (!name) {
        setStatus('Enter church name before adding')
        return
      }
      const res = await authFetch('http://localhost:8000/churches', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name })
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data) || 'Failed to create church')
      const created = data?.church
      if (created && created.id) {
        setChurches(prev => {
          const exists = (prev || []).some(c => Number(c.id) === Number(created.id))
          if (exists) return prev
          return [...(prev || []), created]
        })
        setMemberForm(prev => ({ ...prev, TRANSFER_TO_CHURCH: Number(created.id), transfer_to_church: Number(created.id) }))
      }
      setShowTransferChurchCreator(false)
      setNewTransferChurchName('')
      setStatus('Church added and selected for transfer')
    } catch (e) {
      setStatus('Create church failed: ' + (e?.message || String(e)))
    }
  }

  function labelForColumn(col) {
    if (!col) return '';
    const found = collectionCodeMeta(col)
    if (found) {
      return found.custom_collection_name || found.code || found.column_name || col
    }
    const human = {
      s1: 'Sno',
      s2: 'Date',
      s3: 'Serial',
      s4: 'Name',
      collection_entry_method: 'Collection Entry Method',
      entry_method: 'Entry Method',
    }[col];
    return human || col;
  }

  function mappedPreviewColumns() {
    const first = mappedPreview[0]
    if (!first) return []
    return Object.keys(first).filter(k => !['collection_code', 'church', 's2', 'source'].includes(k))
  }

  function currentChurchName() {
    return (churches || []).find(c => Number(c.id) === Number(selectedChurch || currentUserChurchId))?.name || String(selectedChurch || currentUserChurchId || '-')
  }

  // Helper: format date to DD-MON-YYYY format (e.g., 26-Apr-2026)
  function formatDateDisplay(dateStr) {
    if (!dateStr) return '';
    try {
      const d = new Date(dateStr);
      if (isNaN(d.getTime())) return String(dateStr);
      const day = String(d.getDate()).padStart(2, '0');
      const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      const month = monthNames[d.getMonth()];
      const year = d.getFullYear();
      return `${day}-${month}-${year}`;
    } catch (e) {
      return String(dateStr);
    }
  }

  // Helper: display value for a table cell; for `collection_code` show the column_name when available
  function displayCellValue(k, row) {
    if (!row) return '';
    if (k === 's2') {
      // Format date field to DD-MON-YYYY
      return formatDateDisplay(row.s2);
    }
    if (k === 'collection_code') {
      const raw = row.collection_code;
      const found = (collectionCodes || []).find(c => c.column_name === raw || c.code === raw);
      if (found) return found.column_name || found.code || raw;
      return raw || '';
    }
    if (k === 'collection_entry_method') {
      const raw = String(row.collection_entry_method || '').trim().toLowerCase();
      if (raw === 'form_entry') return 'form entry';
      if (raw === 'import') return 'import';
      if (raw === 'form') return 'form entry';
      return raw;
    }
    if (k === 'verified') {
      return row && row.__verified ? 'Yes' : 'No';
    }
    const v = row[k];
    return v === null || v === undefined ? '' : String(v);
  }

  function normalizeCollectionFieldKey(col) {
    return String(col || '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '')
  }

  function isCollectionReferenceIdField(col) {
    const key = normalizeCollectionFieldKey(col)
    if (!key || key === 'id' || key === 'church') return false
    if (!key.endsWith('_id')) return false
    return /(member|official|group|family)/.test(key)
  }

  function memberLookupColumnForCollectionField(col) {
    const key = normalizeCollectionFieldKey(col)
    const rows = Array.isArray(membersLocal) ? membersLocal : []
    const hasColumn = (candidate) => rows.some(m => m && Object.prototype.hasOwnProperty.call(m, candidate))
    const preferredMap = {
      member_id: 'MEMBER_ID',
      official_member_id: 'OFFICIAL_MEMBER_ID',
      group_id: 'GROUP_ID',
      family_id: 'FAMILY_ID',
      default_family_id: 'DEFAULT_FAMILY_ID',
    }
    const preferred = preferredMap[key]
    if (preferred && hasColumn(preferred)) return preferred
    if (preferred && hasColumn(preferred.toLowerCase())) return preferred.toLowerCase()
    if (hasColumn('MEMBER_ID')) return 'MEMBER_ID'
    if (hasColumn('member_id')) return 'member_id'
    if (preferred) return preferred
    return 'MEMBER_ID'
  }

  function findMemberByCollectionFieldValue(col, value) {
    const text = String(value ?? '').trim()
    if (!text) return null
    const source = memberLookupColumnForCollectionField(col)
    const candidates = [
      source,
      String(source || '').toLowerCase(),
      String(source || '').toUpperCase(),
      'MEMBER_ID',
      'member_id',
      'OFFICIAL_MEMBER_ID',
      'official_member_id',
      'GROUP_ID',
      'group_id',
      'FAMILY_ID',
      'family_id',
      'DEFAULT_FAMILY_ID',
      'default_family_id',
      'id',
    ].filter(Boolean)
    return (membersLocal || []).find(m => candidates.some(c => String(m?.[c] ?? '').trim() === text)) || null
  }

  function memberTranslationSummary(member) {
    if (!member) return ''
    const parts = []
    const name = String(member.MEMBER_NAME || member.member_name || '').trim()
    if (name) parts.push(name)
    const memberId = member.MEMBER_ID ?? member.member_id
    if (memberId !== undefined && memberId !== null && String(memberId).trim() !== '') parts.push(`Member ID: ${memberId}`)
    const officialId = member.OFFICIAL_MEMBER_ID ?? member.official_member_id
    if (officialId !== undefined && officialId !== null && String(officialId).trim() !== '') parts.push(`Official ID: ${officialId}`)
    const groupId = member.GROUP_ID ?? member.group_id
    if (groupId !== undefined && groupId !== null && String(groupId).trim() !== '') parts.push(`Group ID: ${groupId}`)
    const familyId = member.FAMILY_ID ?? member.family_id
    if (familyId !== undefined && familyId !== null && String(familyId).trim() !== '') parts.push(`Family ID: ${familyId}`)
    return parts.join(' | ')
  }

  function collectionFieldTranslation(col, value) {
    const key = normalizeCollectionFieldKey(col)
    const raw = value === null || value === undefined ? '' : String(value).trim()
    if (!raw) return ''
    if (key === 'church') {
      return (churches || []).find(c => Number(c.id) === Number(raw))?.name || raw
    }
    if (key === 'collection_code') {
      const found = (collectionCodes || []).find(c => String(c.column_name) === raw || String(c.code) === raw)
      if (!found) return ''
      return [found.custom_collection_name, found.code, found.column_name].filter(Boolean).join(' | ')
    }
    if (key === 's2') {
      return formatDateDisplay(raw)
    }
    if (isCollectionReferenceIdField(col)) {
      const match = findMemberByCollectionFieldValue(col, raw)
      return memberTranslationSummary(match)
    }
    if (key === 'member_id') {
      const match = findMemberByCollectionFieldValue(col, raw)
      return memberTranslationSummary(match)
    }
    return ''
  }

  function collectionFieldIdOptions(col) {
    const source = memberLookupColumnForCollectionField(col)
    const seen = new Set()
    return (membersLocal || []).reduce((out, member) => {
      const value = member?.[source] ?? member?.[String(source || '').toLowerCase()] ?? member?.MEMBER_ID ?? member?.member_id ?? member?.id
      if (value === null || value === undefined || String(value).trim() === '') return out
      const strValue = String(value)
      if (seen.has(strValue)) return out
      seen.add(strValue)
      const name = String(member?.MEMBER_NAME || member?.member_name || '').trim()
      out.push({
        value: strValue,
        label: name ? `${strValue} - ${name}` : strValue,
      })
      return out
    }, [])
  }

  function getDefaultMcFetchLimit(targetPage = mcPage, targetPageSize = mcPageSize) {
    const pageNum = Math.max(1, Number(targetPage) || 1)
    const pageSizeNum = Math.max(1, Number(targetPageSize) || 10)
    return Math.max(10, pageNum * pageSizeNum)
  }

  function authFetch(url, opts = {}) {
    const h = opts.headers ? { ...opts.headers } : {}
    if (token) h['Authorization'] = `Bearer ${token}`
    return fetch(url, { ...opts, headers: h })
  }

  async function fetchCodes() {
    try {
      const res = await authFetch('http://localhost:8000/collection_codes')
      const data = await res.json().catch(() => [])
      setCollectionCodes(Array.isArray(data) ? data : [])
    } catch (e) {
      setCollectionCodes([])
    }
  }
  async function fetchMembersLocal() {
    try {
      const res = await authFetch('http://localhost:8000/members')
      const data = await res.json()
      if (res.ok) setMembersLocal(data)
    } catch (e) { }
  }

  async function fetchMembersCollections(criteria = null) {
    try {
      const active = criteria || {}
      const params = new URLSearchParams()
      if (active.limit != null) params.set('limit', String(active.limit))
      if (active.search) params.set('search', String(active.search))
      if (active.searchField) params.set('search_field', String(active.searchField))
      if (active.collectionCode) params.set('collection_code', String(active.collectionCode))
      if (active.memberId) params.set('member_id', String(active.memberId))
      if (active.memberName) params.set('member_name', String(active.memberName))
      if (active.phone) params.set('phone', String(active.phone))
      if (active.verified !== '' && active.verified != null) params.set('verified', String(active.verified))
      if (active.s1) params.set('s1', String(active.s1))
      if (active.from) params.set('start_date', String(active.from))
      if (active.to) params.set('end_date', String(active.to))
      if (active.amountMin !== '' && active.amountMin != null) params.set('amount_min', String(active.amountMin))
      if (active.amountMax !== '' && active.amountMax != null) params.set('amount_max', String(active.amountMax))
      const url = params.toString()
        ? `http://localhost:8000/reports/members_collections?${params.toString()}`
        : 'http://localhost:8000/reports/members_collections'
      const res = await authFetch(url)
      const data = await res.json()
      if (!res.ok) {
        const msg = String(data?.detail || JSON.stringify(data) || 'Request failed')
        if (res.status === 403) {
          setStatus('Not authorized for Members Collections. Ask admin to enable Members Collections right for your role.')
        } else {
          setStatus('Failed to load collections: ' + msg)
        }
        setMembersCollections([])
        return false
      }
      // attach helper metadata per row (verified flag and suggestions)
      const enriched = (Array.isArray(data) ? data : []).map(r => ({ ...r, __verified: Number(r?.member_id || 0) > 0, __suggestions: [] }))
      setMembersCollections(enriched)
      if (!enriched.length) {
        setStatus('No collection rows found')
      } else {
        setStatus('')
      }
      if (enriched && enriched.length && (!membersCollectionsFields || membersCollectionsFields.length === 0)) {
        const keys = Object.keys(enriched[0]).filter(k => k !== 'added_at' && !k.startsWith('__'))
        if (!keys.includes('verified')) keys.push('verified')
        setMembersCollectionsFields(keys)
        // By default, hide id, church, and collection_code; show all others
        if (!mcVisibleCols.length) {
          const defaultVisible = keys.filter(k => !['id', 'church', 'collection_code'].includes(k))
          setMcVisibleCols(defaultVisible.length ? defaultVisible : keys)
        }
      }
      return enriched.length
    } catch (e) { setStatus('Failed to load collections: ' + e.message); setMembersCollections([]); return 0 }
  }

  function getMcAllColumns() {
    const fallback = ['id', 'collection_code', 'member_id', 'church', 's1', 's2', 's3', 's4', 's5', 'verified']
    return (membersCollectionsFields && membersCollectionsFields.length) ? membersCollectionsFields : fallback
  }

  function getMcDisplayColumns() {
    const all = getMcAllColumns()
    if (!mcVisibleCols.length) return all
    const cols = mcVisibleCols.filter(c => all.includes(c))
    return cols.length ? cols : all
  }

  function rowMatchesCurrentChurch(row) {
    if (currentUserChurchId === null || currentUserChurchId === undefined || Number.isNaN(Number(currentUserChurchId))) return true
    const rowChurchRaw = row?.church
    const rowChurchText = String(rowChurchRaw ?? '').trim().toLowerCase()
    if (Number(rowChurchRaw) === Number(currentUserChurchId)) return true
    const idInText = String(rowChurchRaw ?? '').match(/\d+/)
    if (idInText && Number(idInText[0]) === Number(currentUserChurchId)) return true
    const currentChurchName = String((churches || []).find(c => Number(c.id) === Number(currentUserChurchId))?.name || '').trim().toLowerCase()
    if (currentChurchName) {
      if (rowChurchText === currentChurchName) return true
      if (rowChurchText.includes(currentChurchName)) return true
    }
    return false
  }

  function hasActiveMcFilters() {
    return [
      mcApplied.text,
      mcApplied.code,
      mcApplied.memberId,
      mcApplied.memberName,
      mcApplied.phone,
      mcApplied.verified,
      mcApplied.s1,
      mcApplied.amountMin,
      mcApplied.amountMax,
      mcApplied.from,
      mcApplied.to,
    ].some(v => String(v ?? '').trim() !== '')
  }

  function getFilteredMembersCollectionsRows() {
    let rows = Array.isArray(membersCollections) ? membersCollections : []
    rows = rows.filter(r => rowMatchesCurrentChurch(r))
    const activeFilters = hasActiveMcFilters()
    rows = rows.slice().sort((a, b) => {
      if (!activeFilters) {
        const ad = a?.added_at ? new Date(a.added_at).getTime() : NaN
        const bd = b?.added_at ? new Date(b.added_at).getTime() : NaN
        if (!Number.isNaN(ad) && !Number.isNaN(bd) && ad !== bd) return bd - ad
        return Number(b?.id ?? 0) - Number(a?.id ?? 0)
      }
      const va = a && a[mcSortKey]
      const vb = b && b[mcSortKey]
      if (va == null && vb == null) return 0
      if (va == null) return mcSortDir === 'asc' ? -1 : 1
      if (vb == null) return mcSortDir === 'asc' ? 1 : -1
      if (typeof va === 'number' && typeof vb === 'number') return mcSortDir === 'asc' ? va - vb : vb - va
      return mcSortDir === 'asc' ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va))
    })
    return activeFilters ? rows : rows.slice(0, 10)
  }

  function applyMcColumns(cols) {
    setMcVisibleCols(cols)
    const prefKey = membersCollectionsPrefColumnsKey()
    if (prefKey) {
      try { localStorage.setItem(prefKey, JSON.stringify(cols)) } catch (e) { }
    }
    setShowMcColumnPicker(false)
  }
  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;')
  }

  function membersPrefUserKey() {
    return String(user?.id || user?.username || '').trim()
  }

  function membersPrefColumnsKey() {
    const k = membersPrefUserKey()
    return k ? `saypy.members.visibleCols.${k}` : ''
  }

  function membersCollectionsPrefColumnsKey() {
    const k = membersPrefUserKey()
    return k ? `saypy.membersCollections.visibleCols.${k}` : ''
  }

  function memberLabelForColumn(col) {
    if (!col) return ''
    const found = collectionCodeMeta(col)
    if (found) {
      return found.custom_collection_name || found.code || found.column_name || col
    }
    const map = {
      sno: 'S/N',
      MEMBER_NAME: 'Member Name',
      MEMBER_ID: 'Member ID',
      FAMILY_ID: 'Family',
      family_id: 'Family',
      DEFAULT_FAMILY_ID: 'Default Family',
      default_family_id: 'Default Family',
      OFFICIAL_MEMBER_ID: 'Unique Identity',
      official_member_id: 'Unique Identity',
      GROUP_NAME: 'Group Name',
      group_name: 'Group Name',
      GROUP_ALIAS: 'Group Alias',
      group_alias: 'Group Alias',
      DEFAULT_GROUP_ALIAS: 'Default Group Alias',
      default_group_alias: 'Default Group Alias',
      GROUP_LEADER_ID: 'Group Leader ID',
      group_leader_id: 'Group Leader ID',
      DEFAULT_GROUP_LEADER_ID: 'Default Group Leader ID',
      default_group_leader_id: 'Default Group Leader ID',
      PHONE: 'Phone',
      EMAIL: 'Email',
      email: 'Email',
      RESIDENCE: 'Residence',
      residence: 'Residence',
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

  function getMembersAllColumns() {
    return (membersFields && membersFields.length) ? membersFields : ['sno', 'MEMBER_NAME', 'MEMBER_ID', 'PHONE', 'church']
  }

  function getMembersDisplayColumns() {
    const all = getMembersAllColumns()
    if (!membersVisibleCols.length) {
      return ['sno', 'MEMBER_NAME', 'MEMBER_ID', 'PHONE', 'church'].filter(c => all.includes(c))
    }
    const cols = membersVisibleCols.filter(c => all.includes(c))
    return cols.length ? cols : all
  }

  function memberDisplayCellValue(k, row) {
    if (!row) return ''
    if (k === 'church') {
      return (churches || []).find(c => Number(c.id) === Number(row?.church))?.name || row?.church || ''
    }
    if (k === 'MEMBER_NAME' || k === 'member_name') {
      const name = row?.MEMBER_NAME || row?.member_name || ''
      const idVal = row?.MEMBER_ID ?? row?.member_id
      if (idVal !== null && idVal !== undefined && String(idVal).trim() !== '') {
        return `${name} (ID:${idVal})`
      }
      return String(name)
    }
    const v = row[k]
    return v === null || v === undefined ? '' : String(v)
  }

  function membersFilterableFields() {
    const all = getMembersAllColumns()
    const extras = ['church_name']
    return Array.from(new Set([...all, ...extras]))
  }

  function membersFieldLabel(field) {
    if (field === 'church_name') return 'Church Name'
    return memberLabelForColumn(field)
  }

  function membersFieldValue(row, field) {
    if (field === 'church_name') {
      return (churches || []).find(c => Number(c.id) === Number(row?.church))?.name || ''
    }
    return row?.[field]
  }

  function addMembersFilter() {
    const firstField = membersFilterableFields()[0] || 'MEMBER_NAME'
    setMembersExtraFilters(prev => [...prev, { id: Date.now() + Math.random(), field: firstField, op: 'contains', value: '' }])
  }

  function updateMembersFilter(id, patch) {
    setMembersExtraFilters(prev => prev.map(f => f.id === id ? { ...f, ...patch } : f))
  }

  function removeMembersFilter(id) {
    setMembersExtraFilters(prev => prev.filter(f => f.id !== id))
  }

  function clearMembersFilters() {
    setMembersExtraFilters([])
  }

  function openMembersColumnPicker() {
    const all = getMembersAllColumns()
    const current = getMembersDisplayColumns()
    setMembersPickerLeft(all.filter(c => !current.includes(c)))
    setMembersPickerRight(current)
    setMembersPickerLeftSelected(new Set())
    setMembersPickerRightSelected(new Set())
    setShowMembersColumnPicker(true)
  }

  function moveMembersColumnsToRight() {
    const selected = Array.from(membersPickerLeftSelected)
    if (!selected.length) return
    setMembersPickerLeft(membersPickerLeft.filter(c => !selected.includes(c)))
    setMembersPickerRight([...membersPickerRight, ...selected.filter(c => !membersPickerRight.includes(c))])
    setMembersPickerLeftSelected(new Set())
  }

  function moveMembersColumnsToLeft() {
    const selected = Array.from(membersPickerRightSelected)
    if (!selected.length) return
    setMembersPickerRight(membersPickerRight.filter(c => !selected.includes(c)))
    setMembersPickerLeft([...membersPickerLeft, ...selected.filter(c => !membersPickerLeft.includes(c))])
    setMembersPickerRightSelected(new Set())
  }

  function moveMembersColumnUp() {
    const selected = Array.from(membersPickerRightSelected)
    if (selected.length !== 1) return
    const col = selected[0]
    const idx = membersPickerRight.findIndex(c => c === col)
    if (idx <= 0) return
    const next = [...membersPickerRight]
    const swap = next[idx - 1]
    next[idx - 1] = next[idx]
    next[idx] = swap
    setMembersPickerRight(next)
    setMembersPickerRightSelected(new Set([col]))
  }

  function moveMembersColumnDown() {
    const selected = Array.from(membersPickerRightSelected)
    if (selected.length !== 1) return
    const col = selected[0]
    const idx = membersPickerRight.findIndex(c => c === col)
    if (idx < 0 || idx >= membersPickerRight.length - 1) return
    const next = [...membersPickerRight]
    const swap = next[idx + 1]
    next[idx + 1] = next[idx]
    next[idx] = swap
    setMembersPickerRight(next)
    setMembersPickerRightSelected(new Set([col]))
  }

  function applyMembersColumnSelection() {
    setMembersVisibleCols(membersPickerRight)
    setShowMembersColumnPicker(false)
  }

  function saveMembersColumnSelection() {
    const prefKey = membersPrefColumnsKey()
    if (!prefKey) { setStatus('Could not save members columns for this user'); return }
    try {
      localStorage.setItem(prefKey, JSON.stringify(getMembersDisplayColumns()))
      setStatus('Members column selection saved for this user')
    } catch (e) {
      setStatus('Failed to save members column selection: ' + (e?.message || String(e)))
    }
  }

  function exportMembersExcel() {
    try {
      const cols = getMembersDisplayColumns()
      const rows = filteredMembers.slice(0, membersMaxRows)
      const head = cols.map(c => `<th>${escapeHtml(memberLabelForColumn(c))}</th>`).join('')
      const body = rows.map(r => `<tr>${cols.map(c => `<td>${escapeHtml(memberDisplayCellValue(c, r))}</td>`).join('')}</tr>`).join('')
      const table = `<table border="1"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`
      const html = `<!doctype html><html><head><meta charset="utf-8" /></head><body>${table}</body></html>`
      const blob = new Blob([html], { type: 'application/vnd.ms-excel' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      const stamp = new Date().toISOString().slice(0, 19).replace(/[T:]/g, '-')
      a.href = url
      a.download = `members_${currentUserChurchId || 'all'}_${stamp}.xls`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } catch (e) { setStatus('Export Members Excel failed: ' + (e?.message || String(e))) }
  }

  function exportMembersPdf() {
    try {
      const cols = getMembersDisplayColumns()
      const rows = filteredMembers.slice(0, membersMaxRows)
      const head = cols.map(c => `<th style="border:1px solid #ccc;padding:6px;text-align:left">${escapeHtml(memberLabelForColumn(c))}</th>`).join('')
      const body = rows.map(r => `<tr>${cols.map(c => `<td style="border:1px solid #ddd;padding:6px">${escapeHtml(memberDisplayCellValue(c, r))}</td>`).join('')}</tr>`).join('')
      const win = window.open('', '_blank')
      if (!win) { setStatus('Pop-up blocked. Allow pop-ups to export PDF.'); return }
      win.document.write(`<!doctype html><html><head><title>Members</title><style>body{font-family:Arial,sans-serif;padding:12px}table{border-collapse:collapse;width:100%;font-size:12px}h3{margin:0 0 10px 0}</style></head><body><h3>Members (Church ${escapeHtml(currentUserChurchId || '-')})</h3><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></body></html>`)
      win.document.close()
      win.focus()
      win.print()
    } catch (e) { setStatus('Export Members PDF failed: ' + (e?.message || String(e))) }
  }

  function exportMembersCollectionsExcel() {
    try {
      const cols = getMcDisplayColumns()
      const rows = getFilteredMembersCollectionsRows()
      const head = cols.map(c => `<th>${escapeHtml(labelForColumn(c))}</th>`).join('')
      const body = rows.map(r => `<tr>${cols.map(c => `<td>${escapeHtml(c === 'verified' ? (r && r.__verified ? 'Yes' : 'No') : displayCellValue(c, r))}</td>`).join('')}</tr>`).join('')
      const table = `<table border="1"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`
      const html = `<!doctype html><html><head><meta charset="utf-8" /></head><body>${table}</body></html>`
      const blob = new Blob([html], { type: 'application/vnd.ms-excel' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      const stamp = new Date().toISOString().slice(0, 19).replace(/[T:]/g, '-')
      a.href = url
      a.download = `members_collections_church_${currentUserChurchId || 'all'}_${stamp}.xls`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } catch (e) {
      setStatus('Export Excel failed: ' + (e?.message || String(e)))
    }
  }

  function exportMembersCollectionsPdf() {
    try {
      const cols = getMcDisplayColumns()
      const rows = getFilteredMembersCollectionsRows()
      const head = cols.map(c => `<th style="border:1px solid #ccc;padding:6px;text-align:left">${escapeHtml(labelForColumn(c))}</th>`).join('')
      const body = rows.map(r => `<tr>${cols.map(c => `<td style="border:1px solid #ddd;padding:6px">${escapeHtml(c === 'verified' ? (r && r.__verified ? 'Yes' : 'No') : displayCellValue(c, r))}</td>`).join('')}</tr>`).join('')
      const win = window.open('', '_blank')
      if (!win) { setStatus('Pop-up blocked. Allow pop-ups to export PDF.'); return }
      win.document.write(`<!doctype html><html><head><title>Members Collections</title><style>body{font-family:Arial,sans-serif;padding:12px}table{border-collapse:collapse;width:100%;font-size:12px}h3{margin:0 0 10px 0}</style></head><body><h3>Members Collections (Church ${escapeHtml(currentUserChurchId || '-')})</h3><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></body></html>`)
      win.document.close()
      win.focus()
      win.print()
    } catch (e) {
      setStatus('Export PDF failed: ' + (e?.message || String(e)))
    }
  }

  // Simple Levenshtein distance for fuzzy matching
  function levenshtein(a, b) {
    if (!a) a = ''; if (!b) b = ''; a = String(a).toLowerCase(); b = String(b).toLowerCase();
    const m = a.length, n = b.length; const dp = Array.from({ length: m + 1 }, () => Array(n + 1).fill(0));
    for (let i = 0; i <= m; i++) dp[i][0] = i; for (let j = 0; j <= n; j++) dp[0][j] = j;
    for (let i = 1; i <= m; i++) for (let j = 1; j <= n; j++) dp[i][j] = a[i - 1] === b[j - 1] ? dp[i - 1][j - 1] : Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + 1);
    return dp[m][n];
  }

  function normalizeNameForMatch(name) {
    return String(name || '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim()
  }

  function normalizePhoneDigits(phone) {
    return String(phone || '').replace(/\D+/g, '')
  }

  function memberPrimaryId(member) {
    return Number(member?.MEMBER_ID ?? member?.member_id ?? member?.id ?? 0) || null
  }

  function buildMemberCandidatesForCollectionRow(row, historicalByName, historicalByPhone) {
    const rowName = normalizeNameForMatch(row?.s4)
    const rowPhone = normalizePhoneDigits(row?.phone)
    return (membersLocal || []).map(member => {
      const memberNameRaw = String(member?.MEMBER_NAME || member?.member_name || '')
      const memberName = normalizeNameForMatch(memberNameRaw)
      const memberPhone = normalizePhoneDigits(member?.PHONE || member?.phone)
      const baseScore = levenshtein(rowName, memberName)
      let score = baseScore

      if (rowPhone && memberPhone) {
        if (rowPhone === memberPhone) score -= 5
        else if (rowPhone.endsWith(memberPhone) || memberPhone.endsWith(rowPhone)) score -= 2
      }

      const primaryId = memberPrimaryId(member)
      if (primaryId) {
        const byName = historicalByName.get(rowName)
        const byPhone = rowPhone ? historicalByPhone.get(rowPhone) : null
        if (byName && Number(byName) === Number(primaryId)) score -= 3
        if (byPhone && Number(byPhone) === Number(primaryId)) score -= 4
      }

      return {
        ...member,
        _score: score,
        _rawScore: baseScore,
        _memberPrimaryId: primaryId,
      }
    }).sort((a, b) => a._score - b._score).slice(0, 8)
  }

  // Verify names and phones for current filtered rows, then open a mapping workspace.
  async function verifyNames() {
    const sourceRows = getFilteredMembersCollectionsRows()
    if (!Array.isArray(sourceRows) || sourceRows.length === 0) return;
    setStatus('Verifying names...')
    const historicalByName = new Map()
    const historicalByPhone = new Map()
      ; (membersCollections || []).forEach(r => {
        const linkedId = Number(r?.member_id || 0)
        if (!linkedId) return
        const keyName = normalizeNameForMatch(r?.s4)
        const keyPhone = normalizePhoneDigits(r?.phone)
        if (keyName && !historicalByName.has(keyName)) historicalByName.set(keyName, linkedId)
        if (keyPhone && !historicalByPhone.has(keyPhone)) historicalByPhone.set(keyPhone, linkedId)
      })

    const mappingRows = sourceRows.map(r => {
      const suggestions = buildMemberCandidatesForCollectionRow(r, historicalByName, historicalByPhone)
      const top = suggestions[0]
      const rowName = normalizeNameForMatch(r?.s4)
      const threshold = Math.max(2, Math.floor((rowName.length || 1) * 0.25))
      const hasServerLink = Number(r?.member_id || 0) > 0
      const autoVerified = hasServerLink || (!!top && top._score <= threshold)
      const autoSelected = hasServerLink
        ? (suggestions.find(s => Number(s._memberPrimaryId) === Number(r.member_id)) || null)
        : (autoVerified ? top : null)
      return {
        rowId: r.id,
        row: r,
        suggestions,
        selectedMemberId: autoSelected ? Number(autoSelected._memberPrimaryId || autoSelected.id || 0) : null,
        selectedLabel: autoSelected ? String(autoSelected?.MEMBER_NAME || '') : '',
        verified: hasServerLink || autoVerified,
      }
    })

    setVerifyMappingRows(mappingRows)
    setShowVerifyMapping(true)
    setStatus(`Verification complete: ${mappingRows.length} row(s) ready for mapping`)
  }

  async function saveVerifyMappingForRow(mappingRow) {
    const rowId = Number(mappingRow?.rowId || 0)
    const selectedId = Number(mappingRow?.selectedMemberId || 0)
    if (!rowId || !selectedId) {
      setStatus('Select a member before saving mapping')
      return false
    }
    const selected = (membersLocal || []).find(m => Number(memberPrimaryId(m)) === selectedId || Number(m?.id || 0) === selectedId)
    if (!selected) {
      setStatus('Selected member was not found')
      return false
    }
    const memberIdToSave = Number(memberPrimaryId(selected) || 0)
    if (!memberIdToSave) {
      setStatus('Selected member has no valid member id')
      return false
    }
    const payload = {
      member_id: memberIdToSave,
      s4: selected?.MEMBER_NAME || mappingRow?.row?.s4 || null,
      phone: mappingRow?.row?.phone || selected?.PHONE || null,
    }
    setVerifySavingRowId(rowId)
    try {
      const res = await authFetch(`http://localhost:8000/members_collection/${rowId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data?.detail || JSON.stringify(data) || 'Save failed')

      setMembersCollections(prev => (prev || []).map(r => Number(r?.id) === rowId
        ? {
          ...r,
          member_id: memberIdToSave,
          s4: payload.s4,
          phone: payload.phone,
          __verified: true,
        }
        : r
      ))
      setVerifyMappingRows(prev => (prev || []).map(v => v.rowId === rowId ? { ...v, verified: true } : v))
      setStatus(`Saved mapping for collection row ${rowId}`)
      return true
    } catch (e) {
      setStatus('Save mapping failed: ' + (e?.message || String(e)))
      return false
    } finally {
      setVerifySavingRowId(null)
    }
  }

  async function saveAllVerifyMappings() {
    if (verifySavingAll) return
    const candidates = (verifyMappingRows || []).filter(v => Number(v?.selectedMemberId || 0) > 0)
    if (!candidates.length) {
      setStatus('No selected mappings to save')
      return
    }
    setVerifySavingAll(true)
    let ok = 0
    for (const item of candidates) {
      const saved = await saveVerifyMappingForRow(item)
      if (saved) ok += 1
    }
    setVerifySavingAll(false)
    setStatus(`Saved ${ok}/${candidates.length} mapping(s)`)
  }

  function getOrderedCodes(codes) { if (!codes || !codes.length) return []; const filtered = codes.filter(c => c.code && String(c.code).toUpperCase() !== 'UNUSED'); const priority = ['Sno', 'Jina', 'Zaka', 'Sadaka']; const out = []; const used = new Set(); priority.forEach(p => { const found = filtered.find(c => String(c.code).toLowerCase() === p.toLowerCase()); if (found) { out.push(found); used.add(found.column_name) } }); const rest = filtered.filter(c => !used.has(c.column_name)).sort((a, b) => (a.code || '').toString().localeCompare((b.code || '').toString())); return out.concat(rest) }

  async function uploadFile() {
    if (!file) return;
    const fd = new FormData(); fd.append('batch', file, file.name);
    setStatus('Uploading file...');
    try {
      const headers = {};
      if (apiKey) headers['X-API-KEY'] = apiKey;
      const res = await authFetch('http://localhost:8000/upload/headers', { method: 'POST', body: fd, headers });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
      setHeaders(data.headers);
      setFullPreview(data.full_preview || []);
      // use full preview as working preview so all rows are editable
      setPreview(data.full_preview || []);
      setS1Column(data.s1_column || null);
      const m = {};
      data.headers.forEach(h => m[h] = data.suggestions && data.suggestions[h] ? data.suggestions[h] : '');
      setMapping(m);
      recomputeMappedPreview(m);
      setStatus('File uploaded');
      setStep(2);
    } catch (e) { setStatus('Upload failed: ' + e.message) }
  }

  function coerceValue(key, val) {
    if (val === null || val === undefined || val === '') return null;
    const sString = new Set(['s4', 's10', 's11', 's12', 'source', 'collection_code', 'notes']);
    const sNumeric = new Set(['s3', 's5', 's6', 's7', 's8', 's9', 's13']);
    const sval = String(val).trim();
    if (key === 's2') { const d = new Date(sval); if (!isNaN(d)) return d.toISOString(); return sval }
    if (key === 's1') return sval;
    if (sString.has(key)) return sval;
    if (sNumeric.has(key) || key.startsWith('c') || key.startsWith('l')) { const n = Number(sval.replace(/[^0-9.\-]/g, '')); return isNaN(n) ? null : n }
    return typeof val === 'number' ? val : sval;
  }

  function recomputeMappedPreview(mappingToUse = mapping) {
    const rows = (fullPreview || []).map(r => {
      const out = { collection_entry_method: 'import' };
      if (selectedChurch) out.church = selectedChurch;
      if (selectedDate) out.s2 = new Date(selectedDate).toISOString();
      if (uploaderName) out.source = uploaderName;
      Object.keys(r).forEach(h => {
        const mapped = mappingToUse[h];
        if (mapped) { out[mapped] = coerceValue(mapped, r[h]) }
      });
      try {
        const s2val = out.s2; let s2dt = null;
        if (typeof s2val === 'string') { const d = new Date(s2val); if (!isNaN(d)) s2dt = d }
        else if (s2val instanceof Date) s2dt = s2val;
        const s3val = out.s3; const s3int = s3val != null ? Number(s3val) : null;
        const church_id = out.church || selectedChurch || null;
        if (s2dt && s3int != null && !out.s1) { const ymd = s2dt.toISOString().slice(0, 10).replace(/-/g, ''); const cidn = String(church_id || '').padStart(3, '0'); const s3n = String(Number(s3int)).padStart(3, '0'); out.s1 = `${ymd}${cidn}${s3n}` }
      } catch (e) { }
      return out
    })
    setMappedPreview(rows);
    setValidationErrors([]);
    return rows
  }

  async function validateRows(rows) { try { const headers = { 'Content-Type': 'application/json' }; if (apiKey) headers['X-API-KEY'] = apiKey; const res = await authFetch('http://localhost:8000/members_collections/validate', { method: 'POST', headers, body: JSON.stringify(rows) }); const data = await res.json(); if (!res.ok) { if (res.status === 422 && data && data.detail && data.detail.validation_errors) { if (data.detail.rows) setMappedPreview(data.detail.rows); return data.detail.validation_errors || [] } throw new Error(data.detail || JSON.stringify(data)) } if (data.rows) setMappedPreview(data.rows); return data.validation_errors || [] } catch (err) { console.warn('Validation error', err); return [{ error: err.message }] } }

  async function submitMapped() {
    // ensure mappedPreview is up-to-date and try extraction
    const rows = mappedPreview.length ? mappedPreview : recomputeMappedPreview(mapping);
    const rows2 = recomputeMappedPreview(mapping, rows);
    // client-side pre-validation for missing required fields
    const pre = preValidateRows(rows2);
    if (pre && pre.length) { setValidationErrors(pre); setStep(4); setStatus('Pre-validation failed'); return }
    setStatus('Validating rows before submit...');
    const val = await validateRows(rows2);
    if (val && val.length) { setStatus('Validation failed'); setValidationErrors(val); setPage('collections'); setStep(4); return }
    setStatus('Submitting mapped rows...');
    try {
      const headers = { 'Content-Type': 'application/json' }; if (apiKey) headers['X-API-KEY'] = apiKey;
      const res = await authFetch('http://localhost:8000/members_collections/bulk', { method: 'POST', headers, body: JSON.stringify(rows2) });
      const data = await res.json(); if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
      setStatus(`Inserted ${data.inserted} rows`); setStep(5)
    } catch (err) { setStatus('Submit failed: ' + err.message) }
  }

  // ----- Reports -----
  const [reportRows, setReportRows] = useState([])
  const [reportFrom, setReportFrom] = useState('')
  const [reportTo, setReportTo] = useState('')
  const [selectedContributorName, setSelectedContributorName] = useState('')
  const [aggRows, setAggRows] = useState([])
  const [selectedReportBuilder, setSelectedReportBuilder] = useState('meeting_summary')
  const [builtReportType, setBuiltReportType] = useState(null)
  const [builtReportRows, setBuiltReportRows] = useState([])
  const [builtReportColumns, setBuiltReportColumns] = useState([])
  const [itemTotalsByDateRows, setItemTotalsByDateRows] = useState([])
  const [meetingSummaryRows, setMeetingSummaryRows] = useState([])
  const [meetingSummaryTotals, setMeetingSummaryTotals] = useState({ local: 0, conference: 0, total: 0 })
  const [periodSummaryRows, setPeriodSummaryRows] = useState([])
  const [periodSummaryTotals, setPeriodSummaryTotals] = useState({ local: 0, conference: 0, total: 0 })
  const [periodSummaryLoading, setPeriodSummaryLoading] = useState(false)
  const [periodSummaryFrom, setPeriodSummaryFrom] = useState('')
  const [periodSummaryTo, setPeriodSummaryTo] = useState('')
  const [reportShowOnlyDataColumns, setReportShowOnlyDataColumns] = useState(true)
  const [reportVisibleCols, setReportVisibleCols] = useState([])
  const [showReportColumnPicker, setShowReportColumnPicker] = useState(false)
  const [reportMaxRows, setReportMaxRows] = useState(30)
  const [reportCollectionsMaxRows, setReportCollectionsMaxRows] = useState(30)
  const reportBuilderOptions = [
    { id: 'details', title: 'Detailed Entries', description: 'Full row-level report with category and amount.' },
    { id: 'daily_totals', title: 'Daily Totals', description: 'Daily totals grouped into local, conference, and total.' },
    { id: 'item_totals', title: 'Collection Item Totals', description: 'Totals per collection item by category.' },
    { id: 'contributor_totals', title: 'Contributor Totals', description: 'Totals by contributor name (s4).' },
    { id: 'meeting_summary', title: 'Meeting Summary Form', description: 'Attached-form style statement for meeting use.' },
  ]

  async function fetchMembersCollectionReport() {
    try {
      const params = new URLSearchParams()
      if (reportFrom) params.set('start_date', String(reportFrom))
      if (reportTo) params.set('end_date', String(reportTo))
      // Reports should use full dataset; backend treats limit<=0 as unlimited.
      params.set('limit', '0')
      const qs = params.toString()
      const url = qs
        ? `http://localhost:8000/reports/members_collections?${qs}`
        : 'http://localhost:8000/reports/members_collections'
      const res = await authFetch(url);
      const data = await res.json();
      if (!res.ok) { setStatus('Report failed: ' + (data.detail || data.error || JSON.stringify(data))); setReportRows([]); return }
      if (!Array.isArray(data)) { setStatus('Report returned unexpected response'); setReportRows([]); return }
      setReportRows(data)
      setBuiltReportType(null)
      setBuiltReportRows([])
      setBuiltReportColumns([])
      setMeetingSummaryRows([])
      setMeetingSummaryTotals({ local: 0, conference: 0, total: 0 })
    } catch (e) { setStatus('Report failed: ' + e.message); setReportRows([]) }
  }

  function safeNumber(v) {
    const n = Number(v)
    return Number.isFinite(n) ? n : 0
  }

  function resetBuiltReportOutput() {
    setBuiltReportType(null)
    setBuiltReportRows([])
    setBuiltReportColumns([])
    setItemTotalsByDateRows([])
    setMeetingSummaryRows([])
    setMeetingSummaryTotals({ local: 0, conference: 0, total: 0 })
  }

  function hasReportCellData(v) {
    if (v === null || v === undefined) return false
    if (typeof v === 'string') return v.trim() !== ''
    if (typeof v === 'number') return Number.isFinite(v)
    if (typeof v === 'boolean') return true
    if (v instanceof Date) return !Number.isNaN(v.getTime())
    return String(v).trim() !== ''
  }

  function normalizeReportHeader(v) {
    return String(v || '')
      .toUpperCase()
      .replace(/[^A-Z0-9]+/g, ' ')
      .trim()
  }

  function isReportReceiptColumn(col) {
    const key = normalizeReportHeader(col)
    return key === 'S1' || key === 'SNO' || key.includes('NAMBARI YA RISITI') || key.includes('RECEIPT')
  }

  function isReportDateColumn(col) {
    const key = normalizeReportHeader(col)
    return key === 'S2' || key === 'TAREHE' || key === 'DATE'
  }

  function isReportSerialColumn(col) {
    const key = normalizeReportHeader(col)
    return key === 'S3' || key === 'NA' || key === 'NO' || key.includes('SERIAL')
  }

  function isReportNameColumn(col) {
    const key = normalizeReportHeader(col)
    return key === 'S4' || key === 'JINA' || key.includes('MEMBER NAME') || key === 'NAME'
  }

  function isReportTotalColumn(col) {
    const label = normalizeReportHeader(labelForColumn(col) || col)
    return label === 'JUMLA' || label === 'TOTAL' || label.includes('JUMLA') || label.includes('TOTAL')
  }

  function isReportLikelyAmountColumn(col) {
    const key = String(col || '').trim().toLowerCase()
    return /^c\d+$/.test(key) || /^l\d+$/.test(key) || /^s[5-9]$/.test(key)
  }

  function formatReportDate(v) {
    if (v === null || v === undefined || String(v).trim() === '') return ''
    const d = new Date(v)
    if (Number.isNaN(d.getTime())) return String(v)
    return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })
  }

  function reportCellValue(col, row) {
    if (!row) return ''
    if (isReportDateColumn(col)) return formatReportDate(row[col])
    const v = row[col]
    return v === null || v === undefined ? '' : String(v)
  }

  function shouldHideMembersReportColumn(col) {
    const key = String(col || '').trim().toLowerCase()
    return new Set([
      'id',
      'collection_code',
      'church',
      'is_locked',
      'locked_at',
      'unlocked_at',
      'collection_entry_method',
      'entry_method',
    ]).has(key)
  }

  function getReportVisibleColumns(rows) {
    const arr = Array.isArray(rows) ? rows : []
    if (!arr.length) return []
    const first = arr[0] || {}
    const ordered = Object.keys(first).filter((col) => !shouldHideMembersReportColumn(col))
    const visible = ordered.filter((col) => arr.some((row) => hasReportCellData(row?.[col])))
    return visible.length ? visible : ordered
  }

  function getReportAllColumns(rows) {
    const arr = Array.isArray(rows) ? rows : []
    if (!arr.length) return []
    const ordered = []
    const seen = new Set()
    arr.forEach((row) => {
      Object.keys(row || {}).forEach((col) => {
        if (!seen.has(col) && !shouldHideMembersReportColumn(col)) {
          seen.add(col)
          ordered.push(col)
        }
      })
    })
    return ordered
  }

  function buildDefaultReportColumns(allColumns) {
    const cols = (allColumns || []).filter((c) => !shouldHideMembersReportColumn(c))
    const receiptHiddenByDefault = cols.filter((c) => !isReportReceiptColumn(c))

    const dateCols = receiptHiddenByDefault.filter(isReportDateColumn)
    const serialCols = receiptHiddenByDefault.filter((c) => !dateCols.includes(c) && isReportSerialColumn(c))
    const nameCols = receiptHiddenByDefault.filter((c) => !dateCols.includes(c) && !serialCols.includes(c) && isReportNameColumn(c))

    const remaining = receiptHiddenByDefault.filter((c) => !dateCols.includes(c) && !serialCols.includes(c) && !nameCols.includes(c))

    const preferredCodeNames = ['ZAKA', 'SADAKA']
    const preferred = []
    preferredCodeNames.forEach((codeName) => {
      const found = remaining.find((c) => normalizeReportHeader(labelForColumn(c) || c) === codeName)
      if (found && !preferred.includes(found)) preferred.push(found)
    })

    const totalCols = remaining.filter((c) => isReportTotalColumn(c) && !preferred.includes(c))
    const amountOthers = remaining
      .filter((c) => !preferred.includes(c) && !totalCols.includes(c))
      .sort((a, b) => {
        const aAmount = isReportLikelyAmountColumn(a) ? 0 : 1
        const bAmount = isReportLikelyAmountColumn(b) ? 0 : 1
        if (aAmount !== bAmount) return aAmount - bAmount
        return String(labelForColumn(a) || a).localeCompare(String(labelForColumn(b) || b))
      })

    return [...dateCols, ...serialCols, ...nameCols, ...preferred, ...amountOthers, ...totalCols]
  }

  function getReportPickerCurrentColumns() {
    const all = getReportAllColumns(reportRows)
    return reportVisibleCols.length ? reportVisibleCols.filter((c) => all.includes(c)) : buildDefaultReportColumns(all)
  }

  function applyReportColumns(cols) {
    setReportVisibleCols(cols)
    setShowReportColumnPicker(false)
  }

  function normalizeDateText(v) {
    if (!v) return ''
    const raw = String(v).trim()
    const direct = raw.match(/^(\d{4})-(\d{2})-(\d{2})/)
    if (direct) return `${direct[1]}-${direct[2]}-${direct[3]}`
    const d = new Date(raw)
    if (Number.isNaN(d.getTime())) return String(v)
    const y = d.getFullYear()
    const m = String(d.getMonth() + 1).padStart(2, '0')
    const day = String(d.getDate()).padStart(2, '0')
    return `${y}-${m}-${day}`
  }

  function labelForCollectionCode(code) {
    const raw = String(code || '').trim()
    if (!raw) return 'Unknown'
    const found = (collectionCodes || []).find(c => String(c.column_name || '').toLowerCase() === raw.toLowerCase() || String(c.code || '').toLowerCase() === raw.toLowerCase())
    return String(found?.custom_collection_name || found?.code || found?.column_name || raw)
  }

  function inferRowAmount(row) {
    if (!row || typeof row !== 'object') return 0
    const code = String(row.collection_code || '').trim()
    if (code) {
      const dynamicAmount = safeNumber(row[code])
      if (dynamicAmount !== 0) return dynamicAmount
    }
    const fallbackKeys = ['amount', 'total_amount', 's5', 's6', 's7', 's8', 's9', 'c1', 'c2', 'c3']
    for (const key of fallbackKeys) {
      const amount = safeNumber(row[key])
      if (amount !== 0) return amount
    }
    return 0
  }

  function categorizeContribution(itemLabel) {
    const label = String(itemLabel || '').toLowerCase()
    const conferenceWords = ['conference', 'conf', 'union', 'field', 'mkoa', 'headquarter', 'hq', 'makao']
    const isConference = conferenceWords.some(w => label.includes(w))
    return isConference ? 'conference' : 'local'
  }

  function splitRuleForItem(itemLabel, itemCode = '') {
    const text = `${String(itemLabel || '').toLowerCase()} ${String(itemCode || '').toLowerCase()}`
    // Business rule: Sadaka and Shukrani are split 58% conference / 42% local.
    if (text.includes('sadaka') || text.includes('shukrani')) {
      return { conferencePct: 58, localPct: 42 }
    }
    return null
  }

  function buildReportEntries() {
    const rows = Array.isArray(reportRows) ? reportRows : []
    const defs = getScopedCollectionCodeDefs()
    const out = []

    rows.forEach((row, idx) => {
      const methodRaw = String(row?.collection_entry_method || '').trim().toLowerCase()
      const entryMethod = methodRaw === 'form_entry' || methodRaw === 'form'
        ? 'form entry'
        : (methodRaw === 'import' ? 'import' : '')

      const common = {
        index: idx + 1,
        date: normalizeDateText(row?.s2),
        receipt_no: String(row?.s1 || ''),
        contributor: String(row?.s4 || ''),
        entry_method: entryMethod,
      }

      let addedFromCodes = false
      defs.forEach((def) => {
        const col = String(def?.column_name || '').trim()
        if (!col) return
        const amount = safeNumber(row?.[col])
        if (amount === 0) return
        addedFromCodes = true
        const itemLabel = String(def?.custom_collection_name || def?.code || def?.column_name || col)
        const split = splitRuleForItem(itemLabel, col)
        if (split) {
          out.push({
            ...common,
            item_code: col,
            item_name: itemLabel,
            category: 'conference',
            amount: amount * (Number(split.conferencePct || 0) / 100),
          })
          out.push({
            ...common,
            item_code: col,
            item_name: itemLabel,
            category: 'local',
            amount: amount * (Number(split.localPct || 0) / 100),
          })
        } else {
          const category = categorizeContribution(itemLabel)
          out.push({
            ...common,
            item_code: col,
            item_name: itemLabel,
            category,
            amount,
          })
        }
      })

      // Fallback for data shapes where a single value is provided outside code columns.
      if (!addedFromCodes) {
        const itemCodeRaw = String(row?.collection_code || '').trim()
        const itemCode = itemCodeRaw.toLowerCase() === 'import' ? '' : itemCodeRaw
        const amount = inferRowAmount(row)
        if (amount !== 0) {
          const itemLabel = itemCode ? labelForCollectionCode(itemCode) : 'Unknown'
          const split = splitRuleForItem(itemLabel, itemCode)
          if (split) {
            out.push({
              ...common,
              item_code: itemCode,
              item_name: itemLabel,
              category: 'conference',
              amount: amount * (Number(split.conferencePct || 0) / 100),
            })
            out.push({
              ...common,
              item_code: itemCode,
              item_name: itemLabel,
              category: 'local',
              amount: amount * (Number(split.localPct || 0) / 100),
            })
          } else {
            const category = categorizeContribution(itemLabel)
            out.push({
              ...common,
              item_code: itemCode,
              item_name: itemLabel,
              category,
              amount,
            })
          }
        }
      }
    })

    return out.filter(entry => entry.amount !== 0)
  }

  function groupAndSum(entries, keyBuilder) {
    const grouped = {}
    entries.forEach(entry => {
      const key = keyBuilder(entry)
      if (!grouped[key]) grouped[key] = { local: 0, conference: 0, total: 0, count: 0 }
      grouped[key][entry.category] += safeNumber(entry.amount)
      grouped[key].total += safeNumber(entry.amount)
      grouped[key].count += 1
    })
    return grouped
  }

  function isContributionCollectionDef(def) {
    const text = [
      def?.custom_collection_name,
      def?.code,
      def?.column_name,
    ].map(v => String(v || '').trim().toLowerCase())

    const blocked = new Set([
      'sno',
      'nambari ya risiti',
      'details2',
      'jumla',
      'na',
      'total',
      'receipt',
      'receipt no',
      'receipt number',
      'serial',
      'serial no',
      'date',
      'tarehe',
      'name',
      'jina',
      'details',
    ])

    return !text.some((t) => t && blocked.has(t))
  }

  function getScopedCollectionCodeDefs() {
    const defs = Array.isArray(collectionCodes) ? collectionCodes : []
    const scoped = defs.filter((c) => c && c.column_name && isContributionCollectionDef(c))
    const preferredByColumn = new Map()
    scoped.forEach((c) => {
      const key = String(c.column_name || '').trim().toLowerCase()
      if (!key) return
      const current = preferredByColumn.get(key)
      const candidateChurch = c.church == null ? null : Number(c.church)
      const currentChurch = current && current.church != null ? Number(current.church) : null
      const isCandidateLocal = candidateChurch != null && Number(candidateChurch) === Number(currentUserChurchId)
      const isCurrentLocal = currentChurch != null && Number(currentChurch) === Number(currentUserChurchId)
      if (!current || (!isCurrentLocal && isCandidateLocal)) {
        preferredByColumn.set(key, c)
      }
    })
    return Array.from(preferredByColumn.values())
  }

  function normalizeScopeConfig(scopeRaw, splitPctRaw) {
    const scope = String(scopeRaw || '').trim().toLowerCase()
    const pctNum = Number(splitPctRaw)
    const splitPct = Number.isFinite(pctNum) ? Math.min(100, Math.max(0, pctNum)) : 0
    if (scope === 'conference') return { mode: 'conference', splitPct: 100 }
    if (scope === 'split') return { mode: 'split', splitPct }
    return { mode: 'local', splitPct: 0 }
  }

  function buildDailyTotalsFromCollectionFields(rows) {
    const defs = getScopedCollectionCodeDefs()
    const arr = Array.isArray(rows) ? rows : []
    if (!arr.length || !defs.length) return []

    const defByKey = new Map()
    defs.forEach((def) => {
      const byColumn = String(def?.column_name || '').trim().toLowerCase()
      const byCode = String(def?.code || '').trim().toLowerCase()
      if (byColumn) defByKey.set(byColumn, def)
      if (byCode) defByKey.set(byCode, def)
    })

    const grouped = {}
    arr.forEach((row) => {
      const dayKey = normalizeDateText(row?.s2) || 'Unknown date'
      if (!grouped[dayKey]) grouped[dayKey] = { date: dayKey, local: 0, conference: 0, total: 0, entries: 0 }

      let rowHasAmount = false
      defs.forEach((def) => {
        const col = String(def?.column_name || '').trim()
        if (!col) return
        const amount = safeNumber(row?.[col])
        if (amount === 0) return
        rowHasAmount = true
        const split = splitRuleForItem(def?.custom_collection_name || def?.code || def?.column_name, col)
        if (split) {
          grouped[dayKey].conference += amount * (Number(split.conferencePct || 0) / 100)
          grouped[dayKey].local += amount * (Number(split.localPct || 0) / 100)
          grouped[dayKey].total += amount
          return
        }
        const cfg = normalizeScopeConfig(def?.scope, def?.conference_split_pct)
        if (cfg.mode === 'conference') {
          grouped[dayKey].conference += amount
        } else if (cfg.mode === 'split') {
          const conferenceShare = amount * (cfg.splitPct / 100)
          grouped[dayKey].conference += conferenceShare
          grouped[dayKey].local += amount - conferenceShare
        } else {
          grouped[dayKey].local += amount
        }
        grouped[dayKey].total += amount
      })

      // Fallback for rows whose usable amount is stored in generic fields (for example s5)
      // instead of the configured collection amount column.
      if (!rowHasAmount) {
        const fallbackAmount = safeNumber(inferRowAmount(row))
        if (fallbackAmount !== 0) {
          const rowCode = String(row?.collection_code || '').trim().toLowerCase()
          const matchedDef = defByKey.get(rowCode)
          const split = splitRuleForItem(labelForCollectionCode(row?.collection_code), rowCode)
          if (split) {
            grouped[dayKey].conference += fallbackAmount * (Number(split.conferencePct || 0) / 100)
            grouped[dayKey].local += fallbackAmount * (Number(split.localPct || 0) / 100)
          } else if (matchedDef) {
            const cfg = normalizeScopeConfig(matchedDef?.scope, matchedDef?.conference_split_pct)
            if (cfg.mode === 'conference') {
              grouped[dayKey].conference += fallbackAmount
            } else if (cfg.mode === 'split') {
              const conferenceShare = fallbackAmount * (cfg.splitPct / 100)
              grouped[dayKey].conference += conferenceShare
              grouped[dayKey].local += fallbackAmount - conferenceShare
            } else {
              grouped[dayKey].local += fallbackAmount
            }
          } else {
            // If there is no scoped definition for this row code, infer category
            // from the collection label so conference-like offerings are not
            // misclassified into local totals.
            const itemLabel = labelForCollectionCode(row?.collection_code)
            const inferredCategory = categorizeContribution(itemLabel)
            if (inferredCategory === 'conference') grouped[dayKey].conference += fallbackAmount
            else grouped[dayKey].local += fallbackAmount
          }
          grouped[dayKey].total += fallbackAmount
          rowHasAmount = true
        }
      }

      if (rowHasAmount) grouped[dayKey].entries += 1
    })

    const orderedKeys = Object.keys(grouped).sort((a, b) => {
      if (a === 'Unknown date' && b !== 'Unknown date') return 1
      if (b === 'Unknown date' && a !== 'Unknown date') return -1
      return String(a).localeCompare(String(b))
    })
    return orderedKeys.map((k) => grouped[k]).filter((r) => safeNumber(r.total) !== 0)
  }

  function appendGroupedTotalsRow(rows, keyName, totalLabel = 'TOTAL') {
    const base = Array.isArray(rows) ? rows : []
    if (!base.length) return base
    const totals = base.reduce((acc, row) => ({
      entries: acc.entries + safeNumber(row.entries),
      local: acc.local + safeNumber(row.local),
      conference: acc.conference + safeNumber(row.conference),
      total: acc.total + safeNumber(row.total),
    }), { entries: 0, local: 0, conference: 0, total: 0 })
    return [
      ...base,
      {
        [keyName]: totalLabel,
        entries: totals.entries,
        local: totals.local,
        conference: totals.conference,
        total: totals.total,
      },
    ]
  }

  function buildDateGroupedCollectionTotals(entries) {
    const grouped = {}
      ; (Array.isArray(entries) ? entries : []).forEach((entry) => {
        const dayKey = String(entry?.date || 'Unknown date')
        if (!grouped[dayKey]) {
          grouped[dayKey] = {
            date: dayKey,
            entries: 0,
            local: 0,
            conference: 0,
            total: 0,
            itemSet: new Set(),
          }
        }
        grouped[dayKey].itemSet.add(String(entry?.item_name || '').trim().toLowerCase())
        grouped[dayKey].local += safeNumber(entry?.category === 'local' ? entry?.amount : 0)
        grouped[dayKey].conference += safeNumber(entry?.category === 'conference' ? entry?.amount : 0)
        grouped[dayKey].total += safeNumber(entry?.amount)
      })

    const rows = Object.keys(grouped)
      .sort((a, b) => {
        if (a === 'Unknown date' && b !== 'Unknown date') return 1
        if (b === 'Unknown date' && a !== 'Unknown date') return -1
        return String(a).localeCompare(String(b))
      })
      .map((k) => ({
        date: grouped[k].date,
        entries: grouped[k].itemSet.size,
        local: grouped[k].local,
        conference: grouped[k].conference,
        total: grouped[k].total,
      }))

    return rows.filter((r) => safeNumber(r.total) !== 0)
  }

  function isExcludedSummaryLabel(value) {
    const normalized = String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '')
    return normalized === 'na' || normalized === 'total' || normalized === 'totals'
  }

  function buildContributorDateItemMatrix(entries, contributorName) {
    const target = String(contributorName || '').trim().toLowerCase()
    if (!target) return { columns: [], rows: [], itemColumns: [] }

    const filtered = (Array.isArray(entries) ? entries : []).filter((entry) => {
      const sameContributor = String(entry?.contributor || '').trim().toLowerCase() === target
      if (!sameContributor) return false
      return !isExcludedSummaryLabel(entry?.item_name)
    })
    if (!filtered.length) return { columns: [], rows: [], itemColumns: [] }

    const itemSet = new Set()
    const byDate = {}
    filtered.forEach((entry) => {
      const date = String(entry?.date || 'Unknown date')
      const item = String(entry?.item_name || 'Unknown item')
      itemSet.add(item)
      if (!byDate[date]) byDate[date] = { date }
      byDate[date][item] = safeNumber(byDate[date][item]) + safeNumber(entry?.amount)
    })

    const itemColumns = Array.from(itemSet.values()).sort((a, b) => String(a).localeCompare(String(b)))
    const rows = Object.keys(byDate)
      .sort((a, b) => {
        if (a === 'Unknown date' && b !== 'Unknown date') return 1
        if (b === 'Unknown date' && a !== 'Unknown date') return -1
        return String(a).localeCompare(String(b))
      })
      .map((date) => {
        const out = { date }
        let rowTotal = 0
        itemColumns.forEach((item) => {
          const amount = safeNumber(byDate[date][item])
          out[item] = amount
          rowTotal += amount
        })
        out.row_total = rowTotal
        return out
      })

    return { columns: ['date', ...itemColumns, 'row_total'], rows, itemColumns }
  }

  async function fetchPeriodSummary() {
    setPeriodSummaryLoading(true)
    setPeriodSummaryRows([])
    setPeriodSummaryTotals({ local: 0, conference: 0, total: 0 })
    try {
      let url = 'http://localhost:8000/reports/period_summary'
      const params = []
      if (periodSummaryFrom) params.push(`start_date=${encodeURIComponent(periodSummaryFrom)}`)
      if (periodSummaryTo) params.push(`end_date=${encodeURIComponent(periodSummaryTo)}`)
      if (params.length) url += '?' + params.join('&')
      const res = await authFetch(url)
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setPeriodSummaryRows(Array.isArray(data.rows) ? data.rows : [])
      setPeriodSummaryTotals(data.totals || { local: 0, conference: 0, total: 0 })
    } catch (e) {
      const msg = String(e?.message || e || '')
      if (msg.toLowerCase().includes('failed to fetch')) {
        setStatus('Period summary failed: network error (Failed to fetch). Check backend is running and API base URL/protocol is correct.')
      } else {
        setStatus('Period summary failed: ' + msg)
      }
    } finally {
      setPeriodSummaryLoading(false)
    }
  }

  async function unlockCollectionRow(rowId) {
    try {
      const res = await authFetch(`http://localhost:8000/members_collection/${rowId}/unlock`, {
        method: 'POST',
        headers: authHeaders(),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus(data.already_unlocked ? 'Record already unlocked.' : 'Record unlocked for editing.')
      await fetchMembersCollections(hasActiveMcFilters() ? {
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
      } : { limit: getDefaultMcFetchLimit(mcPage, mcPageSize) })
    } catch (e) {
      setStatus('Unlock failed: ' + (e?.message || String(e)))
    }
  }

  async function requestCollectionDelete(rowId) {
    try {
      const reason = window.prompt('Reason for delete request (sent for head treasurer approval):', 'Correction')
      if (reason === null) return
      const res = await authFetch(`http://localhost:8000/members_collection/${rowId}/delete_request`, {
        method: 'POST',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ reason }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus(data.already_pending ? `Delete request already pending (#${data.request_id}).` : `Delete request submitted (#${data.request_id || '-'})`)
      if (hasRoleRight('can_delete_members_collections', false) || hasRoleRight('can_unlock_members_collections', false)) {
        await fetchDeleteRequests(deleteRequestStatusFilter)
        await fetchPendingDeleteRequestsCount()
      }
    } catch (e) {
      setStatus('Delete request failed: ' + (e?.message || String(e)))
    }
  }

  async function fetchPendingDeleteRequestsCount() {
    if (!(hasRoleRight('can_delete_members_collections', false) || hasRoleRight('can_unlock_members_collections', false))) {
      setPendingDeleteRequestsCount(0)
      return 0
    }
    try {
      const res = await authFetch('http://localhost:8000/members_collection/delete_requests?status=pending', {
        headers: authHeaders(),
      })
      const data = await res.json().catch(() => [])
      if (!res.ok) throw new Error(data?.detail || JSON.stringify(data))
      const count = Array.isArray(data) ? data.length : 0
      setPendingDeleteRequestsCount(count)
      return count
    } catch (e) {
      setPendingDeleteRequestsCount(0)
      return 0
    }
  }

  async function fetchDeleteRequests(statusFilter = deleteRequestStatusFilter) {
    if (!(hasRoleRight('can_delete_members_collections', false) || hasRoleRight('can_unlock_members_collections', false))) {
      setDeleteRequests([])
      return []
    }
    setDeleteRequestsLoading(true)
    try {
      const q = statusFilter ? `?status=${encodeURIComponent(statusFilter)}` : ''
      const res = await authFetch(`http://localhost:8000/members_collection/delete_requests${q}`, {
        headers: authHeaders(),
      })
      const data = await res.json().catch(() => [])
      if (!res.ok) throw new Error(data?.detail || JSON.stringify(data))
      const rows = Array.isArray(data) ? data : []
      setDeleteRequests(rows)
      if (String(statusFilter || '').toLowerCase() === 'pending') {
        setPendingDeleteRequestsCount(rows.length)
      }
      return rows
    } catch (e) {
      setDeleteRequests([])
      setStatus('Delete requests load failed: ' + (e?.message || String(e)))
      return []
    } finally {
      setDeleteRequestsLoading(false)
    }
  }

  async function decideDeleteRequest(requestId, action) {
    const verb = action === 'approve' ? 'approve' : 'reject'
    const isApprove = verb === 'approve'
    if (isApprove && !window.confirm('Approve this request and delete the target record now?')) return
    const note = window.prompt(`Optional decision note for ${verb}:`, '')
    if (note === null) return
    try {
      const res = await authFetch(`http://localhost:8000/members_collection/delete_requests/${requestId}/${verb}`, {
        method: 'POST',
        headers: authHeaders({ 'Content-Type': 'application/json' }),
        body: JSON.stringify({ decision_note: note || '' }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data?.detail || JSON.stringify(data))
      setStatus(`Delete request #${requestId} ${isApprove ? 'approved' : 'rejected'}.`)
      await fetchDeleteRequests(deleteRequestStatusFilter)
      await fetchPendingDeleteRequestsCount()
      await fetchMembersCollections(hasActiveMcFilters() ? {
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
      } : { limit: getDefaultMcFetchLimit(mcPage, mcPageSize) })
    } catch (e) {
      setStatus(`Delete request ${verb} failed: ` + (e?.message || String(e)))
    }
  }

  async function approveAndDeleteCollectionRow(rowId) {
    try {
      if (!window.confirm('Delete this record now? This action is audited.')) return
      const res = await authFetch(`http://localhost:8000/members_collection/${rowId}`, {
        method: 'DELETE',
        headers: authHeaders(),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data))
      setStatus('Record deleted successfully.')
      await fetchMembersCollections(hasActiveMcFilters() ? {
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
      } : { limit: getDefaultMcFetchLimit(mcPage, mcPageSize) })
    } catch (e) {
      setStatus('Delete failed: ' + (e?.message || String(e)))
    }
  }

  async function buildSelectedReport(builderOverride = null, contributorOverride = null) {
    const activeBuilder = builderOverride || selectedReportBuilder
    resetBuiltReportOutput()
    if (activeBuilder === 'daily_totals') {
      try {
        let url = 'http://localhost:8000/reports/daily_summary'
        const params = []
        if (reportFrom) params.push(`start_date=${encodeURIComponent(reportFrom)}`)
        if (reportTo) params.push(`end_date=${encodeURIComponent(reportTo)}`)
        if (params.length) url += `?${params.join('&')}`
        const res = await authFetch(url)
        const data = await res.json().catch(() => ({}))
        if (!res.ok) throw new Error(data?.detail || JSON.stringify(data))
        const rows = Array.isArray(data?.rows) ? data.rows : []
        const outWithTotal = appendGroupedTotalsRow(rows, 'date')
        setBuiltReportType(activeBuilder)
        setBuiltReportColumns(['date', 'conference', 'local', 'total'])
        setBuiltReportRows(outWithTotal)
        setStatus('Daily totals report built successfully.')
      } catch (e) {
        resetBuiltReportOutput()
        setStatus('Daily totals report failed: ' + (e?.message || String(e)))
      }
      return
    }

    const entries = buildReportEntries()
    if (!entries.length) {
      setStatus('No report data found for the selected filters.')
      return
    }

    if (activeBuilder === 'details') {
      const out = [...entries]
        .sort((a, b) => {
          const ad = a.date || '9999-12-31'
          const bd = b.date || '9999-12-31'
          if (ad !== bd) return ad.localeCompare(bd)
          return String(a.receipt_no || '').localeCompare(String(b.receipt_no || ''))
        })
        .map(e => (
          {
            date: e.date,
            receipt_no: e.receipt_no,
            contributor: e.contributor,
            item_name: e.item_name,
            category: e.category,
            amount: e.amount,
            entry_method: e.entry_method,
          }
        ))
      setBuiltReportType(activeBuilder)
      setBuiltReportColumns(['date', 'receipt_no', 'contributor', 'item_name', 'category', 'amount', 'entry_method'])
      setBuiltReportRows(out)
      setStatus(`Detailed entries report built successfully (${out.length} rows).`)
      return
    }

    if (activeBuilder === 'item_totals') {
      const grouped = groupAndSum(entries, e => e.item_name || 'Unknown item')
      const out = Object.keys(grouped).map(item => ({
        item,
        local: grouped[item].local,
        conference: grouped[item].conference,
        total: grouped[item].total,
        entries: grouped[item].count,
      }))
        .filter((row) => {
          return !isExcludedSummaryLabel(row?.item)
        })
        .sort((a, b) => {
          const diff = safeNumber(b.total) - safeNumber(a.total)
          if (diff !== 0) return diff
          return String(a.item || '').localeCompare(String(b.item || ''))
        })
      const byDateWithTotal = appendGroupedTotalsRow(buildDateGroupedCollectionTotals(entries), 'date')
      setBuiltReportType(activeBuilder)
      setBuiltReportColumns(['item', 'entries', 'local', 'conference', 'total'])
      setBuiltReportRows(out)
      setItemTotalsByDateRows(byDateWithTotal)
      setStatus('Collection item totals report built successfully.')
      return
    }

    if (activeBuilder === 'contributor_totals') {
      let contributor = String(contributorOverride != null ? contributorOverride : selectedContributorName || '').trim()
      if (!contributor) {
        const firstName = Array.from(new Set((Array.isArray(reportRows) ? reportRows : []).map((r) => String(r?.s4 || '').trim()).filter((name) => name && !isExcludedSummaryLabel(name)))).sort((a, b) => a.localeCompare(b))[0] || ''
        contributor = firstName
        if (contributor) setSelectedContributorName(contributor)
      }
      if (!contributor) {
        setStatus('No contributor names found in the current report rows.')
        return
      }
      const matrix = buildContributorDateItemMatrix(entries, contributor)
      if (!matrix.rows.length) {
        setStatus(`No contributions found for contributor: ${contributor}`)
        return
      }
      setBuiltReportType(activeBuilder)
      setBuiltReportColumns(matrix.columns)
      setBuiltReportRows(matrix.rows)
      setStatus(`Contributor totals report built successfully for ${contributor}.`)
      return
    }

    const groupedMeeting = groupAndSum(entries, e => e.item_name || 'Unknown item')
    const rows = Object.keys(groupedMeeting).sort().map(item => ({
      item,
      local: groupedMeeting[item].local,
      conference: groupedMeeting[item].conference,
      total: groupedMeeting[item].total,
    }))
    const totals = rows.reduce((acc, row) => ({
      local: acc.local + safeNumber(row.local),
      conference: acc.conference + safeNumber(row.conference),
      total: acc.total + safeNumber(row.total),
    }), { local: 0, conference: 0, total: 0 })

    setMeetingSummaryRows(rows)
    setMeetingSummaryTotals(totals)
    setBuiltReportType('meeting_summary')
    setStatus('Meeting summary form built successfully.')
  }

  function aggregateByCollectionCode() {
    const rows = reportRows || [];
    const map = {};
    rows.forEach(r => {
      const code = r.collection_code || 'unknown';
      if (!map[code]) map[code] = { collection_code: code, count: 0, s5: 0, s6: 0, s7: 0 };
      map[code].count += 1;
      map[code].s5 += Number(r.s5 || 0);
      map[code].s6 += Number(r.s6 || 0);
      map[code].s7 += Number(r.s7 || 0);
    })
    setAggRows(Object.values(map));
  }

  const reportAllColumns = getReportAllColumns(reportRows)
  const contributorNameOptions = Array.from(
    new Set(
      (Array.isArray(reportRows) ? reportRows : [])
        .map((r) => String(r?.s4 || '').trim())
        .filter((name) => name && !isExcludedSummaryLabel(name))
    )
  ).sort((a, b) => a.localeCompare(b))

  useEffect(() => {
    const all = getReportAllColumns(reportRows)
    if (!all.length) {
      setReportVisibleCols([])
      return
    }
    setReportVisibleCols((prev) => {
      const normalizedPrev = (Array.isArray(prev) ? prev : []).filter((c) => all.includes(c))
      if (normalizedPrev.length) return normalizedPrev
      return buildDefaultReportColumns(all)
    })
  }, [reportRows])

  const reportVisibleColumns = (() => {
    const base = reportVisibleCols.length ? reportVisibleCols.filter((c) => reportAllColumns.includes(c)) : buildDefaultReportColumns(reportAllColumns)
    if (!reportShowOnlyDataColumns) return base
    const filtered = base.filter((col) => (Array.isArray(reportRows) ? reportRows : []).some((row) => hasReportCellData(row?.[col])))
    return filtered.length ? filtered : base
  })()


  function formatNumber(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return '-'
    return new Intl.NumberFormat().format(Number(value))
  }

  function csvCell(value) {
    const text = value === null || value === undefined ? '' : String(value)
    const escaped = text.replace(/"/g, '""')
    return `"${escaped}"`
  }

  function downloadCsv(filename, rows) {
    const csv = rows.map(row => row.map(csvCell).join(',')).join('\r\n')
    const blob = new Blob([`\ufeff${csv}`], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = filename
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }

  function currentChurchName() {
    if (!currentUserChurchId) return 'Local Church'
    const found = (churches || []).find(c => Number(c?.id) === Number(currentUserChurchId))
    const name = String(found?.name || '').trim()
    return name || 'Local Church'
  }

  function periodSummaryPeriodText() {
    const fromText = periodSummaryFrom || 'All'
    const toText = periodSummaryTo || 'All'
    return `${fromText} to ${toText}`
  }

  function periodSummaryFileStamp() {
    const fromText = (periodSummaryFrom || 'all').replace(/[^0-9A-Za-z_-]/g, '_')
    const toText = (periodSummaryTo || 'all').replace(/[^0-9A-Za-z_-]/g, '_')
    return `${fromText}_${toText}`
  }

  function exportPeriodSummaryExcel() {
    try {
      if (!periodSummaryRows.length) {
        setStatus('No period summary data to export.')
        return
      }
      const churchLabel = currentChurchName()
      const localHeader = churchLabel
      const periodText = periodSummaryPeriodText()

      const rows = [
        ['Period Summary Report'],
        [`Church: ${churchLabel}`],
        [`Period: ${periodText}`],
        [],
        ['Collection Item', 'Conference', localHeader, 'Jumla / Total'],
      ]

      periodSummaryRows.forEach((row) => {
        rows.push([
          String(row?.item || ''),
          Number(row?.conference || 0),
          Number(row?.local || 0),
          Number(row?.total || 0),
        ])
      })

      rows.push([
        'TOTAL',
        Number(periodSummaryTotals?.conference || 0),
        Number(periodSummaryTotals?.local || 0),
        Number(periodSummaryTotals?.total || 0),
      ])

      downloadCsv(`period_summary_${periodSummaryFileStamp()}.csv`, rows)
      setStatus('Period summary exported to CSV.')
    } catch (e) {
      setStatus('CSV export failed: ' + (e?.message || String(e)))
    }
  }

  function exportPeriodSummaryPdf() {
    try {
      if (!periodSummaryRows.length) {
        setStatus('No period summary data to export.')
        return
      }
      const churchLabel = currentChurchName()
      const localHeader = churchLabel
      const periodText = periodSummaryPeriodText()

      const doc = new jsPDF({ orientation: 'portrait', unit: 'pt', format: 'a4' })
      doc.setFontSize(15)
      doc.setTextColor(15, 23, 42)
      doc.text('Period Summary Report', 40, 42)
      doc.setFontSize(11)
      doc.text(`Church: ${churchLabel}`, 40, 60)
      doc.text(`Period: ${periodText}`, 40, 76)

      const body = (periodSummaryRows || []).map((row) => ([
        String(row?.item || ''),
        formatNumber(row?.conference || 0),
        formatNumber(row?.local || 0),
        formatNumber(row?.total || 0),
      ]))
      body.push([
        'TOTAL',
        formatNumber(periodSummaryTotals?.conference || 0),
        formatNumber(periodSummaryTotals?.local || 0),
        formatNumber(periodSummaryTotals?.total || 0),
      ])

      autoTable(doc, {
        startY: 92,
        head: [['Collection Item', 'Conference', localHeader, 'Jumla / Total']],
        body,
        theme: 'grid',
        headStyles: { fillColor: [248, 180, 0], textColor: [0, 0, 0], fontStyle: 'bold' },
        styles: { textColor: [15, 23, 42], lineColor: [203, 213, 225], lineWidth: 0.5, fontSize: 10 },
        columnStyles: {
          0: { halign: 'left', cellWidth: 230 },
          1: { halign: 'right', cellWidth: 90 },
          2: { halign: 'right', cellWidth: 110 },
          3: { halign: 'right', cellWidth: 90 },
        },
        didParseCell: (data) => {
          if (data.row.index === body.length - 1) {
            data.cell.styles.fontStyle = 'bold'
            data.cell.styles.fillColor = [248, 250, 252]
          }
        }
      })

      doc.save(`period_summary_${periodSummaryFileStamp()}.pdf`)
      setStatus('Period summary exported to PDF.')
    } catch (e) {
      setStatus('PDF export failed: ' + (e?.message || String(e)))
    }
  }

  function currentBuiltReportDataset() {
    if (builtReportType && builtReportType !== 'meeting_summary' && Array.isArray(builtReportRows) && builtReportRows.length && Array.isArray(builtReportColumns) && builtReportColumns.length) {
      return {
        builderId: builtReportType,
        title: reportBuilderOptions.find(r => r.id === builtReportType)?.title || 'Built Report',
        columns: builtReportColumns,
        rows: builtReportRows,
      }
    }
    if (builtReportType === 'meeting_summary' && Array.isArray(meetingSummaryRows) && meetingSummaryRows.length) {
      const rows = [...meetingSummaryRows, {
        item: 'TOTAL',
        local: meetingSummaryTotals?.local || 0,
        conference: meetingSummaryTotals?.conference || 0,
        total: meetingSummaryTotals?.total || 0,
      }]
      return {
        builderId: 'meeting_summary',
        title: 'Meeting Summary Form',
        columns: ['item', 'local', 'conference', 'total'],
        rows,
      }
    }
    return null
  }

  function labelForBuiltReportColumn(col, datasetTitle = '', builderId = builtReportType) {
    const key = String(col || '').trim().toLowerCase()
    const isDailyTotals = builderId === 'daily_totals' || String(datasetTitle || '').toLowerCase().includes('daily totals')
    if (isDailyTotals && key === 'date') return 'Date'
    if (isDailyTotals && key === 'conference') return 'Conference Amount'
    if (isDailyTotals && key === 'local') return `${currentChurchName()} Amount`
    if (isDailyTotals && key === 'total') return 'Total Amount'
    if (builderId === 'item_totals' && key === 'item') return 'ITEM'
    if (builderId === 'item_totals' && key === 'entries') return 'ENTRIES'
    if (builderId === 'item_totals' && key === 'local') return 'LOCAL'
    if (builderId === 'item_totals' && key === 'conference') return 'CONFERENCE'
    if (builderId === 'item_totals' && key === 'total') return 'TOTAL'
    if (builderId === 'contributor_totals') return String(col || '').toUpperCase()
    return labelForColumn(col)
  }

  function isBuiltReportNumericColumn(col, builderId = builtReportType) {
    if (['amount', 'local', 'conference', 'total'].includes(String(col))) return true
    if (builderId === 'contributor_totals' && String(col) !== 'date') return true
    return false
  }

  function isBuiltReportRightAlignedColumn(col, builderId = builtReportType) {
    const key = String(col || '').trim().toLowerCase()
    if (builderId === 'item_totals' && ['entries', 'local', 'conference', 'total'].includes(key)) return true
    return isBuiltReportNumericColumn(col, builderId)
  }

  function exportBuiltReportExcel() {
    try {
      const dataset = currentBuiltReportDataset()
      if (!dataset) { setStatus('Build a report first before export.'); return }
      const prettyCols = dataset.columns.map(c => labelForBuiltReportColumn(c, dataset.title, dataset.builderId))
      const rows = [
        [dataset.title],
        [`Church: ${currentChurchName()}`],
        [`Period: ${reportFrom || 'All'} to ${reportTo || 'All'}`],
        [],
        prettyCols,
      ]
      dataset.rows.forEach(row => {
        rows.push(dataset.columns.map(col => row?.[col] ?? ''))
      })
      if (dataset.builderId === 'item_totals' && Array.isArray(itemTotalsByDateRows) && itemTotalsByDateRows.length) {
        rows.push([])
        rows.push(['Collection Item Totals By Date'])
        rows.push(['Date', 'Items With Values', 'Local', 'Conference', 'Total'])
        itemTotalsByDateRows.forEach((row) => {
          rows.push([
            String(row?.date || ''),
            Number(row?.entries || 0),
            Number(row?.local || 0),
            Number(row?.conference || 0),
            Number(row?.total || 0),
          ])
        })
      }
      const stamp = `${(reportFrom || 'all').replace(/[^0-9A-Za-z_-]/g, '_')}_${(reportTo || 'all').replace(/[^0-9A-Za-z_-]/g, '_')}`
      downloadCsv(`report_builder_${stamp}.csv`, rows)
      setStatus('Built report exported to CSV.')
    } catch (e) {
      setStatus('Built report CSV export failed: ' + (e?.message || String(e)))
    }
  }

  function exportBuiltReportPdf() {
    try {
      const dataset = currentBuiltReportDataset()
      if (!dataset) { setStatus('Build a report first before export.'); return }
      const doc = new jsPDF({ orientation: 'landscape', unit: 'pt', format: 'a4' })
      doc.setFontSize(14)
      doc.text(dataset.title, 36, 36)
      doc.setFontSize(10)
      doc.text(`Church: ${currentChurchName()}`, 36, 52)
      doc.text(`Period: ${reportFrom || 'All'} to ${reportTo || 'All'}`, 36, 66)
      const head = [dataset.columns.map(c => labelForBuiltReportColumn(c, dataset.title, dataset.builderId))]
      const body = dataset.rows.map(row => dataset.columns.map(col => {
        const v = row?.[col]
        return isBuiltReportNumericColumn(col, dataset.builderId) ? formatNumber(v) : String(v ?? '')
      }))
      autoTable(doc, {
        startY: 78,
        head,
        body,
        theme: 'grid',
        headStyles: { fillColor: [37, 99, 235], textColor: [255, 255, 255], fontStyle: 'bold' },
        styles: { fontSize: 9, textColor: [15, 23, 42] },
      })
      if (dataset.builderId === 'item_totals' && Array.isArray(itemTotalsByDateRows) && itemTotalsByDateRows.length) {
        const nextY = (doc.lastAutoTable && doc.lastAutoTable.finalY ? doc.lastAutoTable.finalY : 78) + 18
        doc.setFontSize(11)
        doc.text('Collection Item Totals By Date', 36, nextY)
        autoTable(doc, {
          startY: nextY + 8,
          head: [['Date', 'Items With Values', 'Local', 'Conference', 'Total']],
          body: itemTotalsByDateRows.map((row) => ([
            String(row?.date || ''),
            String(Number(row?.entries || 0)),
            formatNumber(row?.local || 0),
            formatNumber(row?.conference || 0),
            formatNumber(row?.total || 0),
          ])),
          theme: 'grid',
          headStyles: { fillColor: [30, 41, 59], textColor: [255, 255, 255], fontStyle: 'bold' },
          styles: { fontSize: 9, textColor: [15, 23, 42] },
        })
      }
      const stamp = `${(reportFrom || 'all').replace(/[^0-9A-Za-z_-]/g, '_')}_${(reportTo || 'all').replace(/[^0-9A-Za-z_-]/g, '_')}`
      doc.save(`report_builder_${stamp}.pdf`)
      setStatus('Built report exported to PDF.')
    } catch (e) {
      setStatus('Built report PDF export failed: ' + (e?.message || String(e)))
    }
  }

  function displayUserName(u) {
    const first = String(u?.first_name || '').trim()
    const middle = String(u?.middle_name || '').trim()
    const last = String(u?.last_name || '').trim()
    const full = [first, middle, last].filter(Boolean).join(' ')
    if (full) return full
    return String(u?.username || '')
  }

  function statusMeta() {
    const raw = String(status || '').trim()
    if (!raw) return null
    const lower = raw.toLowerCase()
    const isError = lower.includes('failed') || lower.includes('error') || lower.includes('not authorized')
    const title = isError ? 'Error' : 'Success'
    const body = raw.includes(':') ? raw.split(':').slice(1).join(':').trim() || raw : raw
    return { title, body, level: isError ? 'error' : 'success' }
  }

  function mainGridWrapStyle(maxHeight = 420) {
    if (gridModeMain === 'scrollable') {
      return { maxHeight, overflowX: 'auto', overflowY: 'auto', border: '1px solid #ddd', borderRadius: 6 }
    }
    return { maxHeight, overflow: 'auto' }
  }

  function mainGridTableStyle(fill = true) {
    if (gridModeMain === 'scrollable') {
      return { width: 'max-content', minWidth: fill ? '100%' : 'max-content', borderCollapse: 'collapse' }
    }
    return { width: fill ? '100%' : 'auto', borderCollapse: 'collapse' }
  }

  const filteredUsers = (usersList || []).filter((u) => {
    const q = String(usersSearchText || '').trim().toLowerCase()
    if (!q) return true
    const fullName = [u?.first_name, u?.middle_name, u?.last_name].filter(Boolean).join(' ')
    const churchName = (churches || []).find(c => Number(c.id) === Number(u?.church))?.name || u?.church || ''
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
    .filter((m) => {
      const q = String(membersSearchText || '').trim().toLowerCase()
      const churchName = (churches || []).find(c => Number(c.id) === Number(m?.church))?.name || m?.church || ''
      const bag = {
        name: m?.MEMBER_NAME || '',
        member_id: m?.MEMBER_ID ?? '',
        phone: m?.PHONE || '',
        church: String(churchName || ''),
        sno: m?.sno ?? '',
        all: [m?.sno, m?.MEMBER_NAME, m?.MEMBER_ID, m?.PHONE, churchName].join(' '),
      }
      const basicMatch = !q || String(bag[membersSearchField] ?? bag.all).toLowerCase().includes(q)
      if (!basicMatch) return false

      if (!membersExtraFilters.length) return true
      return membersExtraFilters.every(f => {
        const field = f?.field
        const op = String(f?.op || 'contains')
        const needle = String(f?.value ?? '').trim().toLowerCase()
        if (!field || !needle) return true
        const hay = String(membersFieldValue(m, field) ?? '').toLowerCase()
        if (op === 'equals') return hay === needle
        if (op === 'starts_with') return hay.startsWith(needle)
        if (op === 'ends_with') return hay.endsWith(needle)
        return hay.includes(needle)
      })
    })
    .slice()
    .sort((a, b) => {
      const av = membersFieldValue(a, membersSortKey)
      const bv = membersFieldValue(b, membersSortKey)
      const an = Number(av)
      const bn = Number(bv)
      let cmp = 0
      if (!Number.isNaN(an) && !Number.isNaN(bn) && String(av).trim() !== '' && String(bv).trim() !== '') {
        cmp = an - bn
      } else {
        cmp = String(av ?? '').localeCompare(String(bv ?? ''), undefined, { numeric: true, sensitivity: 'base' })
      }
      return membersSortDir === 'asc' ? cmp : -cmp
    })

  // Public self-registration — reachable whether or not anyone is logged in
  // (checked ahead of the `!user` gate below), and intentionally excluded
  // from the AdminKit CSS toggle (same as LoginScreen) since it's styled
  // simply, matching the pre-login look.
  if (page === 'self-register') {
    return (
      <>
        <ToastContainer position="top-right" autoClose={4000} />
        <SelfRegistrationPage />
      </>
    )
  }

  // If not authenticated, show a focused login screen (no menu)
  if (!user) {
    return (
      <>
        <ToastContainer position="top-right" autoClose={4000} />
        <LoginScreen
          systemName={systemName}
          loginUser={loginUser}
          setLoginUser={setLoginUser}
          loginPass={loginPass}
          setLoginPass={setLoginPass}
          doLogin={doLogin}
          statusInfo={statusMeta()}
        />
      </>
    )
  }

  // Member-provisioned accounts (self-registered or staff-created member
  // logins — anything with `member_id` set) get the self-service portal
  // instead of the admin CoreApp shell; staff/admin accounts (member_id
  // null) fall through to the existing shell below, unchanged.
  if (user.member_id) {
    return (
      <>
        <ToastContainer position="top-right" autoClose={4000} />
        <MemberPortalApp token={token} user={user} onLogout={logout} />
      </>
    )
  }

  return (
    <>
      <ToastContainer position="top-right" autoClose={4000} />
      <div className='wrapper'>
        <SidebarNav
          page={page}
          setPage={setPage}
          setupRoute={setupRoute}
          setSetupRoute={setSetupRoute}
          appName={appName}
          hasRoleRight={hasRoleRight}
          isAdmin={isAdmin}
          fetchUsers={fetchUsers}
          open={sidebarOpen}
          onNavigate={() => setSidebarOpen(false)}
          token={token}
        />

        <div className='main'>
          <AppHeader
            appName={appName}
            user={user}
            displayUserName={displayUserName}
            onLogout={logout}
            onToggleSidebar={() => setSidebarOpen(o => !o)}
            onOpenPreferences={() => setPage('preferences')}
          />

          <main className='content'>
            <div className='container-fluid p-0'>


              {page === 'preferences' && (
                <PreferencesPage
                  gridModeMain={gridModeMain} setGridModeMain={setGridModeMain}
                  uploadGridMode={uploadGridMode} setUploadGridMode={setUploadGridMode}
                  mappedPreviewMode={mappedPreviewMode} setMappedPreviewMode={setMappedPreviewMode}
                  showOwnPasswordForm={showOwnPasswordForm} setShowOwnPasswordForm={setShowOwnPasswordForm}
                  ownCurrentPassword={ownCurrentPassword} setOwnCurrentPassword={setOwnCurrentPassword}
                  ownNewPassword={ownNewPassword} setOwnNewPassword={setOwnNewPassword}
                  ownConfirmPassword={ownConfirmPassword} setOwnConfirmPassword={setOwnConfirmPassword}
                  submitOwnPasswordReset={submitOwnPasswordReset}
                  PASSWORD_HINT={PASSWORD_HINT}
                />
              )}

              {page === 'admin' && (
                <AdminUsersPage
                  fetchUsers={fetchUsers}
                  createUser={createUser}
                  churches={churches}
                  currentUserChurchId={currentUserChurchId}
                  canAccessChurch={canAccessChurch}
                  denyRestrictedChurchAccess={denyRestrictedChurchAccess}
                  roleOptionsForCurrentUser={roleOptionsForCurrentUser}
                  roleLabel={roleLabel}
                  usersSearchField={usersSearchField} setUsersSearchField={setUsersSearchField}
                  usersSearchText={usersSearchText} setUsersSearchText={setUsersSearchText}
                  usersMaxRows={usersMaxRows} setUsersMaxRows={setUsersMaxRows}
                  filteredUsers={filteredUsers}
                  beginEditUser={beginEditUser}
                  editingUser={editingUser} setEditingUser={setEditingUser}
                  userEditForm={userEditForm} setUserEditForm={setUserEditForm}
                  saveUserEdit={saveUserEdit}
                  resetUser={resetUser} setResetUser={setResetUser}
                  resetPassword={resetPassword} setResetPassword={setResetPassword}
                  submitResetPassword={submitResetPassword}
                  PASSWORD_HINT={PASSWORD_HINT}
                />
              )}

              {page === 'settings' && (
                <SettingsPage
                  user={user}
                  currentUserChurchId={currentUserChurchId}
                  systemName={systemName}
                  showEditSystemName={showEditSystemName} setShowEditSystemName={setShowEditSystemName}
                  editingSystemName={editingSystemName} setEditingSystemName={setEditingSystemName}
                  submitSystemName={submitSystemName}
                  appName={appName}
                  showEditAppName={showEditAppName} setShowEditAppName={setShowEditAppName}
                  editingAppName={editingAppName} setEditingAppName={setEditingAppName}
                  submitAppName={submitAppName}
                  rolesCatalog={rolesCatalog}
                  newRole={newRole} setNewRole={setNewRole}
                  createRolePolicy={createRolePolicy}
                  editingRole={editingRole} setEditingRole={setEditingRole}
                  saveRolePolicy={saveRolePolicy}
                  removeRolePolicy={removeRolePolicy}
                />
              )}

              {page === 'members' && (
                <MembersPage
                  membersQ={membersQ} setMembersQ={setMembersQ} fetchMembers={fetchMembers}
                  membersSearchField={membersSearchField} setMembersSearchField={setMembersSearchField}
                  membersSearchText={membersSearchText} setMembersSearchText={setMembersSearchText}
                  addMembersFilter={addMembersFilter} clearMembersFilters={clearMembersFilters}
                  setEditingMember={setEditingMember} setMemberForm={setMemberForm} setShowMemberGroupInfo={setShowMemberGroupInfo} setShowMembershipInfo={setShowMembershipInfo} setShowMemberForm={setShowMemberForm}
                  showMembersColumnPicker={showMembersColumnPicker} setShowMembersColumnPicker={setShowMembersColumnPicker}
                  getMembersAllColumns={getMembersAllColumns} getMembersDisplayColumns={getMembersDisplayColumns} memberLabelForColumn={memberLabelForColumn} memberDisplayCellValue={memberDisplayCellValue}
                  setMembersVisibleCols={setMembersVisibleCols} saveMembersColumnSelection={saveMembersColumnSelection}
                  exportMembersExcel={exportMembersExcel} exportMembersPdf={exportMembersPdf}
                  membersMaxRows={membersMaxRows} setMembersMaxRows={setMembersMaxRows}
                  filteredMembers={filteredMembers}
                  membersExtraFilters={membersExtraFilters} updateMembersFilter={updateMembersFilter} removeMembersFilter={removeMembersFilter}
                  membersFilterableFields={membersFilterableFields} membersFieldLabel={membersFieldLabel}
                  membersSortKey={membersSortKey} setMembersSortKey={setMembersSortKey} membersSortDir={membersSortDir} setMembersSortDir={setMembersSortDir}
                  showMemberForm={showMemberForm} editingMember={editingMember} memberForm={memberForm}
                  membersFields={membersFields}
                  showMemberGroupInfo={showMemberGroupInfo} showMembershipInfo={showMembershipInfo}
                  churches={churches} canAccessChurch={canAccessChurch} denyRestrictedChurchAccess={denyRestrictedChurchAccess} user={user}
                  showTransferChurchCreator={showTransferChurchCreator} setShowTransferChurchCreator={setShowTransferChurchCreator} newTransferChurchName={newTransferChurchName} setNewTransferChurchName={setNewTransferChurchName} createTransferChurchOption={createTransferChurchOption}
                  members={members}
                  authFetch={authFetch} setStatus={setStatus}
                  nextMemberNumericValue={nextMemberNumericValue}
                  mapMode={mapMode} setMapMode={setMapMode} mapTargetMember={mapTargetMember} setMapTargetMember={setMapTargetMember} mapFilter={mapFilter} setMapFilter={setMapFilter} mapRight={mapRight} setMapRight={setMapRight} mapDesignated={mapDesignated} setMapDesignated={setMapDesignated} mapSaveMessage={mapSaveMessage} setMapSaveMessage={setMapSaveMessage}
                  mainGridWrapStyle={mainGridWrapStyle} mainGridTableStyle={mainGridTableStyle}
                />
              )}

              {page === 'members_core' && (
                <CoreApp sharedToken={token} route={setupRoute} setSetupRoute={setSetupRoute} />
              )}

              {page === 'collections' && (
                <CollectionsPage
                  token={token} authFetch={authFetch} collectionCodes={collectionCodes} churches={churches} fetchCodes={fetchCodes} user={user} labelForColumn={labelForColumn}
                  currentUserChurchId={currentUserChurchId} denyRestrictedChurchAccess={denyRestrictedChurchAccess} uploadGridMode={uploadGridMode} mappedPreviewMode={mappedPreviewMode}
                  hasRoleRight={hasRoleRight} membersLocal={membersLocal}
                  localCollectionCodes={localCollectionCodes}
                  showCollectionCodesPanel={showCollectionCodesPanel} setShowCollectionCodesPanel={setShowCollectionCodesPanel}
                  collectionCodesMaxRows={collectionCodesMaxRows} setCollectionCodesMaxRows={setCollectionCodesMaxRows}
                  newCodeColumn={newCodeColumn} setNewCodeColumn={setNewCodeColumn}
                  newCodeLabel={newCodeLabel} setNewCodeLabel={setNewCodeLabel}
                  newCodeCustomName={newCodeCustomName} setNewCodeCustomName={setNewCodeCustomName}
                  newCodeScope={newCodeScope} setNewCodeScope={setNewCodeScope}
                  newCodeSplitPct={newCodeSplitPct} setNewCodeSplitPct={setNewCodeSplitPct}
                  createLocalCode={createLocalCode}
                  editingCodeId={editingCodeId} setEditingCodeId={setEditingCodeId}
                  editCodeForm={editCodeForm} setEditCodeForm={setEditCodeForm}
                  updateLocalCode={updateLocalCode} deleteLocalCode={deleteLocalCode}
                  mainGridWrapStyle={mainGridWrapStyle} mainGridTableStyle={mainGridTableStyle}
                />
              )}

              {page === 'members_collections' && (
                <MembersCollectionsPage
                  currentUserChurchId={currentUserChurchId}
                  fetchMembersCollections={fetchMembersCollections} hasActiveMcFilters={hasActiveMcFilters} getDefaultMcFetchLimit={getDefaultMcFetchLimit}
                  mcApplied={mcApplied} setMcApplied={setMcApplied}
                  mcPage={mcPage} setMcPage={setMcPage} mcPageSize={mcPageSize} setMcPageSize={setMcPageSize}
                  mcSearchField={mcSearchField} setMcSearchField={setMcSearchField} mcFilterText={mcFilterText} setMcFilterText={setMcFilterText}
                  collectionCodes={collectionCodes} mcFilterCode={mcFilterCode} setMcFilterCode={setMcFilterCode}
                  mcFilterMemberId={mcFilterMemberId} setMcFilterMemberId={setMcFilterMemberId} mcFilterMemberName={mcFilterMemberName} setMcFilterMemberName={setMcFilterMemberName} mcFilterPhone={mcFilterPhone} setMcFilterPhone={setMcFilterPhone}
                  mcFilterVerified={mcFilterVerified} setMcFilterVerified={setMcFilterVerified} mcFilterS1={mcFilterS1} setMcFilterS1={setMcFilterS1}
                  verifyNames={verifyNames}
                  mcFrom={mcFrom} setMcFrom={setMcFrom} mcTo={mcTo} setMcTo={setMcTo} mcFilterAmountMin={mcFilterAmountMin} setMcFilterAmountMin={setMcFilterAmountMin} mcFilterAmountMax={mcFilterAmountMax} setMcFilterAmountMax={setMcFilterAmountMax}
                  exportMembersCollectionsExcel={exportMembersCollectionsExcel} exportMembersCollectionsPdf={exportMembersCollectionsPdf}
                  getFilteredMembersCollectionsRows={getFilteredMembersCollectionsRows}
                  showVerifyMapping={showVerifyMapping} setShowVerifyMapping={setShowVerifyMapping}
                  verifyMappingRows={verifyMappingRows} setVerifyMappingRows={setVerifyMappingRows} saveAllVerifyMappings={saveAllVerifyMappings} verifySavingAll={verifySavingAll} verifySavingRowId={verifySavingRowId} saveVerifyMappingForRow={saveVerifyMappingForRow}
                  hasRoleRight={hasRoleRight}
                  pendingDeleteRequestsCount={pendingDeleteRequestsCount} deleteRequestStatusFilter={deleteRequestStatusFilter} setDeleteRequestStatusFilter={setDeleteRequestStatusFilter} fetchDeleteRequests={fetchDeleteRequests} fetchPendingDeleteRequestsCount={fetchPendingDeleteRequestsCount} deleteRequestsLoading={deleteRequestsLoading} deleteRequests={deleteRequests} decideDeleteRequest={decideDeleteRequest}
                  getMcDisplayColumns={getMcDisplayColumns} getMcAllColumns={getMcAllColumns} labelForColumn={labelForColumn} applyMcColumns={applyMcColumns}
                  showMcColumnPicker={showMcColumnPicker} setShowMcColumnPicker={setShowMcColumnPicker}
                  mcSortKey={mcSortKey} setMcSortKey={setMcSortKey} mcSortDir={mcSortDir} setMcSortDir={setMcSortDir}
                  displayCellValue={displayCellValue}
                  editingCollection={editingCollection} setEditingCollection={setEditingCollection}
                  unlockCollectionRow={unlockCollectionRow} approveAndDeleteCollectionRow={approveAndDeleteCollectionRow} requestCollectionDelete={requestCollectionDelete}
                  membersCollectionsFields={membersCollectionsFields}
                  canAccessChurch={canAccessChurch} denyRestrictedChurchAccess={denyRestrictedChurchAccess} churches={churches}
                  findMemberByCollectionFieldValue={findMemberByCollectionFieldValue}
                  normalizeCollectionFieldKey={normalizeCollectionFieldKey} isCollectionReferenceIdField={isCollectionReferenceIdField} collectionFieldIdOptions={collectionFieldIdOptions} collectionFieldTranslation={collectionFieldTranslation}
                  membersLocal={membersLocal}
                  authFetch={authFetch} setStatus={setStatus}
                  mainGridWrapStyle={mainGridWrapStyle} mainGridTableStyle={mainGridTableStyle}
                />
              )}

              {page === 'reports' && (
                <ReportsPage
                  periodSummaryFrom={periodSummaryFrom} setPeriodSummaryFrom={setPeriodSummaryFrom} periodSummaryTo={periodSummaryTo} setPeriodSummaryTo={setPeriodSummaryTo}
                  fetchPeriodSummary={fetchPeriodSummary} periodSummaryLoading={periodSummaryLoading}
                  exportPeriodSummaryPdf={exportPeriodSummaryPdf} exportPeriodSummaryExcel={exportPeriodSummaryExcel}
                  periodSummaryRows={periodSummaryRows} periodSummaryTotals={periodSummaryTotals}
                  currentChurchName={currentChurchName}
                  formatNumber={formatNumber}
                  mainGridWrapStyle={mainGridWrapStyle} mainGridTableStyle={mainGridTableStyle}
                  reportFrom={reportFrom} setReportFrom={setReportFrom} reportTo={reportTo} setReportTo={setReportTo}
                  fetchMembersCollectionReport={fetchMembersCollectionReport}
                  showReportColumnPicker={showReportColumnPicker} setShowReportColumnPicker={setShowReportColumnPicker}
                  reportVisibleColumns={reportVisibleColumns} getReportPickerCurrentColumns={getReportPickerCurrentColumns} getReportAllColumns={getReportAllColumns} reportRows={reportRows} applyReportColumns={applyReportColumns}
                  showCollectionsInReports={showCollectionsInReports} setShowCollectionsInReports={setShowCollectionsInReports} fetchMembersCollections={fetchMembersCollections}
                  reportMaxRows={reportMaxRows} setReportMaxRows={setReportMaxRows}
                  reportShowOnlyDataColumns={reportShowOnlyDataColumns} setReportShowOnlyDataColumns={setReportShowOnlyDataColumns}
                  labelForColumn={labelForColumn}
                  reportBuilderOptions={reportBuilderOptions} selectedReportBuilder={selectedReportBuilder} setSelectedReportBuilder={setSelectedReportBuilder}
                  buildSelectedReport={buildSelectedReport} exportBuiltReportPdf={exportBuiltReportPdf} exportBuiltReportExcel={exportBuiltReportExcel}
                  builtReportType={builtReportType}
                  selectedContributorName={selectedContributorName} setSelectedContributorName={setSelectedContributorName} contributorNameOptions={contributorNameOptions}
                  reportCellValue={reportCellValue}
                  aggregateByCollectionCode={aggregateByCollectionCode} aggRows={aggRows}
                  builtReportRows={builtReportRows} builtReportColumns={builtReportColumns} isBuiltReportRightAlignedColumn={isBuiltReportRightAlignedColumn} isBuiltReportNumericColumn={isBuiltReportNumericColumn} labelForBuiltReportColumn={labelForBuiltReportColumn}
                  itemTotalsByDateRows={itemTotalsByDateRows}
                  meetingSummaryRows={meetingSummaryRows} meetingSummaryTotals={meetingSummaryTotals}
                  membersCollections={membersCollections} membersCollectionsFields={membersCollectionsFields} displayCellValue={displayCellValue}
                  reportCollectionsMaxRows={reportCollectionsMaxRows} setReportCollectionsMaxRows={setReportCollectionsMaxRows}
                />
              )}
            </div>
          </main>
          <AppFooter systemName={systemName} />
        </div>
      </div>
    </>
  )
}

// ----- Subcomponents -----
