import React from 'react'
import { Settings } from 'react-feather'
import LookupTablePanel from './LookupTablePanel'
import ChurchScopedLookupTab from './ChurchScopedLookupTab'
import PageHeader from '../PageHeader'
import TabNav from '../TabNav'
import { membersApi } from '../api'

const SUB_TABS = {
  household_roles: { label: 'Household Roles', description: "A member's position within their family (head of family, spouse, child, ...).", api: membersApi.householdRoles },
  contact_types: { label: 'Contact Types', description: "Kinds of contact a member's phone/email entries can be (mobile, email, ...).", api: membersApi.contactTypes },
  membership_categories: { label: 'Membership Categories', description: "A member's standing in the church (full member, associate, visitor, ...).", api: membersApi.membershipCategories },
  member_statuses: { label: 'Member Statuses', description: 'Current status of a member (active, inactive, transferred, deceased).', api: membersApi.memberStatuses },
  nationalities: { label: 'Nationalities', description: 'Used on member records and for formatting phone contacts.', api: membersApi.nationalities, extraFields: [{ key: 'dial_code', label: 'Dial Code', placeholder: '+255' }] },
}

// Church-scoped (not pure global) lookups — {code, name, church_id, status}
// — rendered via ChurchScopedLookupTab instead of LookupTablePanel.
const CHURCH_SCOPED_SUB_TABS = {
  organisations: { label: 'Organisations', entityLabel: 'organisation', newButtonLabel: '+ New Organisation', api: membersApi.organisations },
  sabbath_school_groups: { label: 'Sabbath School Groups', entityLabel: 'Sabbath School group', newButtonLabel: '+ New Group', api: membersApi.sabbathSchoolGroups },
  church_roles: { label: 'Church Roles', entityLabel: 'church role', newButtonLabel: '+ New Role', api: membersApi.churchRoles },
}

export default function ReferenceDataTab({ sub, setSetupRoute }){
  const activeTab = sub || 'household_roles'
  const config = SUB_TABS[activeTab]
  const churchScopedConfig = CHURCH_SCOPED_SUB_TABS[activeTab]

  function handleSelectTab(tabId) {
    if (setSetupRoute) {
      setSetupRoute({ menu: 'configuration', sub: tabId })
    }
  }

  const tabs = [
    { id: 'household_roles', label: 'Household Roles' },
    { id: 'contact_types', label: 'Contact Types' },
    { id: 'membership_categories', label: 'Membership Categories' },
    { id: 'member_statuses', label: 'Member Statuses' },
    { id: 'nationalities', label: 'Nationalities' },
    { id: 'organisations', label: 'Organisations' },
    { id: 'sabbath_school_groups', label: 'Sabbath School Groups' },
    { id: 'church_roles', label: 'Church Roles' },
  ]

  return (
    <div>
      <PageHeader title='Configuration' subtitle='System Settings' icon={Settings} />

      <TabNav tabs={tabs} activeId={activeTab} onSelect={handleSelectTab} />

      {churchScopedConfig ? (
        <ChurchScopedLookupTab
          api={churchScopedConfig.api}
          title={null}
          subtitle={null}
          entityLabel={churchScopedConfig.entityLabel}
          newButtonLabel={churchScopedConfig.newButtonLabel}
        />
      ) : (
        <>
          <p className='small mb8'>{(config || SUB_TABS.household_roles).description}</p>
          <LookupTablePanel api={(config || SUB_TABS.household_roles).api} extraFields={(config || SUB_TABS.household_roles).extraFields} />
        </>
      )}
    </div>
  )
}
