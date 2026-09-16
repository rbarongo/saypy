import { useEffect, useState } from 'react'
import { membersApi } from '../components/members-core/api'

/**
 * Flattened view of GET /auth/me/permissions — lets the frontend gate UI
 * (buttons, nav entries) without guessing at role names. Distinct from
 * `usePermissions.js`, which drives the legacy top-level App.jsx admin
 * shell's flat role-string checks (hasRoleRight/isAdmin) — this one talks
 * to the newer church-scoped permission-code system used inside
 * members-core (see rbac_service.PermissionScopes on the backend).
 *
 * `token` (optional): App.jsx's top-level session token. members-core's
 * api.js keeps its own, separate token store (bridged from `token` by
 * CoreApp's own effect) — a caller that mounts *outside* CoreApp, like
 * SidebarNav, has no guarantee that bridge has already run by the time this
 * hook's own fetch fires (both are independent effects, and React doesn't
 * order them for you). Passing `token` here bridges it synchronously first,
 * so the permissions fetch always carries a valid Authorization header
 * instead of racing CoreApp's own bridge and silently coming back empty.
 */
export default function useMyPermissions(token) {
  const [scopes, setScopes] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    if (token) membersApi.setSession(token, membersApi.getStoredUser())
    membersApi.getMyPermissions()
      .then(res => { if (!cancelled) setScopes(res.scopes || {}) })
      .catch(() => { if (!cancelled) setScopes({}) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [token])

  /** Is `code` granted at all, optionally scoped to one church (null in the
   * granted list means a global grant, matching every church). */
  function can(code, churchId = null) {
    if (!scopes) return false
    const churches = scopes[code]
    if (!churches || churches.length === 0) return false
    if (churches.includes(null)) return true
    if (churchId === null || churchId === undefined || churchId === '') return true
    return churches.includes(Number(churchId))
  }

  return { can, loading, scopes: scopes || {} }
}
