// Declarative catalogue of the 7 ToR Member Management reports — each is
// rendered by the single generic <ReportView> using this config, instead of
// 7 near-duplicate components. `filters` lists which <ReportFilters> tokens
// apply; `apiFn` names the matching function under membersApi.reports.
export const REPORTS = [
  {
    key: 'membership-register',
    label: 'Membership Register',
    description: 'Full roster of approved members with personal, church, and contact details.',
    apiFn: 'membershipRegister',
    filters: ['church', 'status', 'membershipCategory', 'householdRole', 'dateRange', 'search'],
  },
  {
    key: 'membership-movements',
    label: 'Membership Movements',
    description: 'Joins, registration decisions, and transfers over a date range.',
    apiFn: 'membershipMovements',
    filters: ['church', 'dateRange', 'actions'],
  },
  {
    key: 'membership-growth',
    label: 'Membership Growth',
    description: 'New members per month and the running cumulative total (defaults to the trailing 12 months).',
    apiFn: 'membershipGrowth',
    filters: ['church', 'membershipCategory', 'dateRange'],
  },
  {
    key: 'sabbath-school-membership',
    label: 'Sabbath School Membership',
    description: 'Members grouped by their assigned Sabbath School group.',
    apiFn: 'sabbathSchoolMembership',
    filters: ['church', 'sabbathSchoolGroup'],
  },
  {
    key: 'ministry-participation',
    label: 'Ministry & Organisation Participation',
    description: 'Members and the organisations / church roles they belong to.',
    apiFn: 'ministryParticipation',
    filters: ['church', 'organisation', 'churchRole'],
  },
  {
    key: 'demographic-summary',
    label: 'Demographic Summary',
    description: 'Breakdown by gender, age bracket, nationality, household role, and membership category.',
    apiFn: 'demographicSummary',
    filters: ['church'],
  },
  {
    key: 'contact-completeness',
    label: 'Contact-Information Completeness',
    description: 'Which members are missing a phone number or email address.',
    apiFn: 'contactCompleteness',
    filters: ['church', 'gap'],
  },
]
