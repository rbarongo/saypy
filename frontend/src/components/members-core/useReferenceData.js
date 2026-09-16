import { useEffect, useState } from 'react'
import { membersApi } from './api'

const EMPTY = {
  churches: [], householdRoles: [], membershipCategories: [], memberStatuses: [],
  nationalities: [], contactTypes: [],
}

/** Fetches churches + all reference-data lookups once; shared by the People tabs. */
export default function useReferenceData(){
  const [data, setData] = useState({ ...EMPTY, loading: true, error: '' })

  useEffect(() => {
    let cancelled = false
    async function load(){
      try{
        const [churches, householdRoles, membershipCategories, memberStatuses, nationalities, contactTypes] = await Promise.all([
          membersApi.listChurches(),
          membersApi.householdRoles.list(),
          membersApi.membershipCategories.list(),
          membersApi.memberStatuses.list(),
          membersApi.nationalities.list(),
          membersApi.contactTypes.list(),
        ])
        if(!cancelled) setData({ churches, householdRoles, membershipCategories, memberStatuses, nationalities, contactTypes, loading: false, error: '' })
      }catch(e){
        if(!cancelled) setData(prev => ({ ...prev, loading: false, error: e.message || 'Failed to load reference data' }))
      }
    }
    load()
    return () => { cancelled = true }
  }, [])

  return data
}
