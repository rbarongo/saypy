import React, { useEffect, useState } from 'react'
import 'chart.js/auto'
import { Doughnut } from 'react-chartjs-2'
import { MapPin, Users, Home } from 'react-feather'
import LookupTablePanel from '../reference/LookupTablePanel'
import PageHeader from '../PageHeader'
import StatCard, { StatGrid } from '../StatCard'
import { membersApi } from '../api'
import { baseChartOptions, categoricalColors, cssVar } from '../chartTheme'

const churchesApi = {
  list: membersApi.listChurches,
  create: membersApi.createChurch,
  update: membersApi.updateChurch,
  remove: membersApi.deleteChurch,
}

export default function ChurchesTab() {
  const [stats, setStats] = useState(null)

  useEffect(() => { membersApi.getStatsOverview().then(setStats).catch(() => { }) }, [])

  const base = baseChartOptions()
  const gridless = { ...base, scales: undefined }
  const churchData = stats && {
    labels: stats.members_by_church.map(b => b.name),
    datasets: [{
      data: stats.members_by_church.map(b => b.count),
      backgroundColor: stats.members_by_church.map((_, i) => categoricalColors()[i % categoricalColors().length]),
      borderColor: cssVar('--surface', '#fff'),
      borderWidth: 2,
    }],
  }

  return (
    <div>
      <PageHeader title='Church Management' subtitle='Church Management' icon={MapPin} />

      {stats && (
        <>
          <StatGrid>
            <StatCard label='Churches' value={stats.churches_total} icon={MapPin} color='primary' />
            <StatCard label='Total Members' value={stats.members_total} icon={Users} color='info' />
            <StatCard label='Families' value={stats.families_total} icon={Home} color='success' />
          </StatGrid>
          {/* {stats.members_by_church.length > 0 && (
            <div className='card mb-1'>
              <div className='card-header'><h5 className='card-title mb-0'>Members per Church</h5></div>
              <div className='card-body'>
                <div className='chart chart-xs'>
                  <Doughnut data={churchData} options={{ ...gridless, plugins: { ...gridless.plugins, legend: { ...gridless.plugins.legend, position: 'bottom' } } }} />
                </div>
              </div>
            </div>
          )} */}
        </>
      )}

      <LookupTablePanel
        api={churchesApi}
        extraFields={[{ key: 'app_name', label: 'App Name', placeholder: 'StewardHub' }]}
      />
    </div>
  )
}
