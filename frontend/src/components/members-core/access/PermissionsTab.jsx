import React from 'react'
import LookupTablePanel from '../reference/LookupTablePanel'
import { membersApi } from '../api'

const permissionsApi = {
  list: membersApi.listPermissions,
  create: membersApi.createPermission,
  update: membersApi.updatePermission,
  remove: membersApi.deletePermission,
}

export default function PermissionsTab(){
  return (
    <div>
      <p className='small mb8'>The catalog of things a role can be granted. New permissions only take effect once real code checks for them.</p>
      <LookupTablePanel api={permissionsApi} extraFields={[{ key: 'description', label: 'Description' }]} />
    </div>
  )
}
