import { useMemo } from 'react'

const ROLE_LABELS = {
  system_admin: 'System Admin',
  admin: 'Church Admin',
  treasurer: 'Treasurer',
  head_treasurer: 'Head Treasurer',
  pastor: 'Pastor',
  elder: 'Elder',
  data_steward: 'Data Steward',
  uploader: 'Uploader',
  viewer: 'Viewer',
}

const COMPATIBILITY = {
  can_manage_members_collections: new Set(['admin','treasurer','head_treasurer','data_steward','uploader','viewer']),
  can_delete_members_collections: new Set(['admin','head_treasurer']),
  can_unlock_members_collections: new Set(['admin','head_treasurer']),
  can_view_collection_codes: new Set(['admin','treasurer','head_treasurer','uploader','viewer']),
  can_manage_collections: new Set(['admin','treasurer','head_treasurer','uploader']),
  can_manage_members: new Set(['admin','data_steward']),
  can_view_reports: new Set(['admin','treasurer','head_treasurer','pastor','elder','data_steward','viewer']),
}

export default function usePermissions(user, rolesCatalog){
  return useMemo(()=>{
    function currentRolePolicy(){
      const role = String(user?.role || '').toLowerCase()
      return (rolesCatalog || []).find(r => String(r.role || '').toLowerCase() === role) || null
    }

    function hasRoleRight(flag, fallback = false){
      const role = String(user?.role || '').toLowerCase()
      if(role === 'system_admin') return true
      const rp = currentRolePolicy()
      if(!rp) return fallback
      const explicit = !!rp[flag]
      if(explicit) return true
      if(!!rp.system_protected && COMPATIBILITY[flag] && COMPATIBILITY[flag].has(role)) return true
      return false
    }

    function isAdmin(){
      if(!user) return false
      const role = String(user.role || '').toLowerCase()
      const uname = String(user.username || '').toLowerCase()
      return role === 'admin' || role === 'system_admin' || uname === 'saypy_admin'
    }

    function isUploader(){ return user && user.role === 'uploader' }

    function roleLabel(role){
      const found = (rolesCatalog || []).find(r => String(r.role || '').toLowerCase() === String(role || '').toLowerCase())
      if(found && found.display_name) return String(found.display_name)
      return ROLE_LABELS[String(role || '').toLowerCase()] || String(role || '')
    }

    return { currentRolePolicy, hasRoleRight, isAdmin, isUploader, roleLabel }
  }, [user, rolesCatalog])
}
