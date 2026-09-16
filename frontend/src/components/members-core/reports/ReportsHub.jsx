import React, { useState } from 'react'
import { BarChart2 } from 'react-feather'
import PageHeader from '../PageHeader'
import TabNav from '../TabNav'
import { REPORTS } from './reportsConfig'
import ReportView from './ReportView'

/** Mounted at Member Management → Reports. A self-contained tile picker
 * over the 7 reports (no sidebar/URL changes needed for this level) plus
 * the generic <ReportView> for whichever one is active. */
export default function ReportsHub() {
  const [activeKey, setActiveKey] = useState(REPORTS[0].key)
  const active = REPORTS.find(r => r.key === activeKey) || REPORTS[0]

  return (
    <div>
      <PageHeader title='Member Management' subtitle='Reports' icon={BarChart2} />

      <TabNav tabs={REPORTS.map(r => ({ id: r.key, label: r.label }))} activeId={activeKey} onSelect={setActiveKey} />

      <ReportView key={activeKey} report={active} />
    </div>
  )
}
