import React, { useEffect, useState } from 'react'
import Alert from '../../Alert'
import { membersApi } from '../api'

export default function AccessMatrixTab(){
  const [roles, setRoles] = useState([])
  const [permissions, setPermissions] = useState([])
  const [matrix, setMatrix] = useState({}) // roleId -> Map(permissionId -> is_allowed)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [busyCell, setBusyCell] = useState(null)

  async function refresh(){
    setLoading(true); setError('')
    try{
      const [rolesList, permissionsList] = await Promise.all([membersApi.listRoles(), membersApi.listPermissions()])
      const entries = await Promise.all(rolesList.map(async r => [r.id, await membersApi.getRolePermissions(r.id)]))
      const nextMatrix = {}
      for(const [roleId, rows] of entries){
        nextMatrix[roleId] = new Map(rows.map(row => [row.permission_id, row.is_allowed]))
      }
      setRoles(rolesList)
      setPermissions(permissionsList)
      setMatrix(nextMatrix)
    }catch(e){
      setError(e.message || 'Failed to load the access matrix')
    }finally{
      setLoading(false)
    }
  }

  useEffect(() => { refresh() }, [])

  async function toggle(roleId, permissionId, current){
    const key = `${roleId}:${permissionId}`
    setBusyCell(key); setError('')
    try{
      await membersApi.setRolePermission(roleId, permissionId, !current)
      setMatrix(prev => {
        const next = { ...prev }
        next[roleId] = new Map(prev[roleId])
        next[roleId].set(permissionId, !current)
        return next
      })
    }catch(e){
      setError(e.message || 'Failed to update permission')
    }finally{
      setBusyCell(null)
    }
  }

  return (
    <div>
      <p className='small mb8'>Check a box to grant a role a permission, uncheck to revoke it. Changes apply immediately — no save button.</p>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <div className='table-wrap'>
        <table>
          <thead>
            <tr>
              <th>Permission</th>
              {roles.map(r => <th key={r.id} style={{ textAlign: 'center' }}>{r.name}</th>)}
            </tr>
          </thead>
          <tbody>
            {loading && <tr><td colSpan={1 + roles.length} className='small'>Loading…</td></tr>}
            {!loading && permissions.length === 0 && <tr><td colSpan={1 + roles.length} className='small'>No permissions defined yet.</td></tr>}
            {!loading && permissions.map(p => (
              <tr key={p.id}>
                <td>
                  <div style={{ fontWeight: 700 }}>{p.name}</div>
                  {p.description && <div className='small'>{p.description}</div>}
                  <div className='small'>{p.code}</div>
                </td>
                {roles.map(r => {
                  const checked = matrix[r.id]?.get(p.id) || false
                  const cellKey = `${r.id}:${p.id}`
                  return (
                    <td key={r.id} style={{ textAlign: 'center' }}>
                      <input
                        type='checkbox'
                        checked={checked}
                        disabled={busyCell === cellKey}
                        onChange={() => toggle(r.id, p.id, checked)}
                      />
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
