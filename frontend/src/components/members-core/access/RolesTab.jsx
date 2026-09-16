import React from 'react'
import LookupTablePanel from '../reference/LookupTablePanel'
import { membersApi } from '../api'

const rolesApi = {
  list: membersApi.listRoles,
  create: membersApi.createRole,
  update: membersApi.updateRole,
  remove: membersApi.deleteRole,
}

export default function RolesTab(){
  return (
    <div>
      <p className='small mb8'>What a role can actually do is configured on the Access Matrix tab.</p>
      <LookupTablePanel api={rolesApi} />
    </div>
  )
}
