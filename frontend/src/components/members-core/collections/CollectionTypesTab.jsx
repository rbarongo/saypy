import React from 'react'
import { Tag } from 'react-feather'
import ChurchScopedLookupTab from '../reference/ChurchScopedLookupTab'
import { membersApi } from '../api'
import useMyPermissions from '../../../hooks/useMyPermissions'

const collectionTypesApi = {
  list: (params) => membersApi.listCollectionTypes(params),
  create: (payload) => membersApi.createCollectionType(payload),
  update: (id, payload) => membersApi.updateCollectionType(id, payload),
  remove: (id) => membersApi.deleteCollectionType(id),
}

export default function CollectionTypesTab() {
  const { can } = useMyPermissions()
  return (
    <ChurchScopedLookupTab
      api={collectionTypesApi}
      title='Collections'
      subtitle='Collection Types'
      entityLabel='collection type'
      newButtonLabel='+ New Type'
      readOnly={!can('manage_collection_types')}
      icon={Tag}
    />
  )
}
