import React, { useEffect, useState } from 'react'
import 'chart.js/auto'
import { Bar, Doughnut, Line } from 'react-chartjs-2'
import { Users, Home, MapPin, UserCheck, Shield, LogOut, Clock } from 'react-feather'
import Alert from '../Alert'
import Card from '../Card'
import PageHeader from './PageHeader'
import StatCard, { StatGrid } from './StatCard'
import { membersApi } from './api'
import { baseChartOptions, categoricalColors, cssVar, seriesColor, statusColorForCode } from './chartTheme'

// `width` is a Bootstrap grid span out of 12 (applied from md breakpoint up —
// every card still stacks full-width below that) — pass whatever fits the
// chart best per call site instead of a fixed one-size-fits-all layout.
function ChartCard({ title, size = 'chart-sm', width = 6, children, variant = 'primary' }) {
  return (
    <div className={`col-12 col-md-${width}`}>
      <Card
        title={title}
        outline
        variant={variant}
        collapsible
        className='h-100 flex-fill w-100'
        bodyStyle={{ padding: '1.25rem' }}
      >
        <div className={`chart ${size}`}>
          {children}
        </div>
      </Card>
    </div>
  )
}

export default function DashboardTab() {
  const [stats, setStats] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false
    membersApi.getStatsOverview()
      .then(data => { if (!cancelled) setStats(data) })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load statistics') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [])

  if (loading) return <p className='text-muted small'>Loading dashboard…</p>
  if (error) return <Alert level='error' title='Failed to load dashboard'>{error}</Alert>
  if (!stats) return null

  const base = baseChartOptions()
  const gridless = { ...base, scales: undefined }

  const statusData = {
    labels: stats.members_by_status.map(b => b.name),
    datasets: [{
      data: stats.members_by_status.map(b => b.count),
      backgroundColor: stats.members_by_status.map(b => statusColorForCode(b.code)),
      borderColor: cssVar('--surface', '#fff'),
      borderWidth: 2,
    }],
  }

  const roleData = {
    labels: stats.members_by_household_role.map(b => b.name),
    datasets: [{ label: 'Members', data: stats.members_by_household_role.map(b => b.count), backgroundColor: seriesColor(), borderRadius: 4 }],
  }

  const categoryData = {
    labels: stats.members_by_membership_category.map(b => b.name),
    datasets: [{ label: 'Members', data: stats.members_by_membership_category.map(b => b.count), backgroundColor: seriesColor(), borderRadius: 4 }],
  }

  const churchData = {
    labels: stats.members_by_church.map(b => b.name),
    datasets: [{
      data: stats.members_by_church.map(b => b.count),
      backgroundColor: stats.members_by_church.map((_, i) => categoricalColors()[i % categoricalColors().length]),
      borderColor: cssVar('--surface', '#fff'),
      borderWidth: 2,
    }],
  }

  const trendData = {
    labels: stats.members_recent_trend.map(p => p.day),
    datasets: [{
      label: 'New members',
      data: stats.members_recent_trend.map(p => p.count),
      borderColor: seriesColor(),
      backgroundColor: seriesColor(),
      tension: 0.3,
      fill: false,
      pointRadius: 4,
    }],
  }

  const pendingRegistrations = (stats.members_by_registration_status || []).find(b => b.code === 'pending')?.count || 0

  const growthData = {
    labels: (stats.members_growth_monthly || []).map(p => p.month),
    datasets: [{
      label: 'Cumulative members',
      data: (stats.members_growth_monthly || []).map(p => p.cumulative),
      borderColor: seriesColor(),
      backgroundColor: seriesColor(),
      tension: 0.3,
      fill: false,
      pointRadius: 4,
    }],
  }

  const completeness = stats.contact_completeness
  const completenessData = completeness && {
    labels: ['With phone', 'With email', 'No contact on file'],
    datasets: [{ label: 'Members', data: [completeness.with_phone, completeness.with_email, completeness.without_any_contact], backgroundColor: seriesColor(), borderRadius: 4 }],
  }

  return (
    <div>
      <PageHeader
        title='Dashboard'
        subtitle='System overview'
        icon={Home}
        stats={[
          { label: 'Members', value: stats.members_total },
          { label: 'Families', value: stats.families_total },
          { label: 'Churches', value: stats.churches_total },
          { label: 'Pending', value: pendingRegistrations },
        ]}
      />

      <StatGrid>
        {/* <StatCard label='Members' value={stats.members_total} icon={Users} color='info' />
        <StatCard label='Families' value={stats.families_total} icon={Home} color='success' />
        <StatCard label='Churches' value={stats.churches_total} icon={MapPin} color='warning' /> */}
        <StatCard label='User Accounts' value={stats.users_total} icon={UserCheck} color='primary' />
        <StatCard label='Roles' value={stats.roles_total} icon={Shield} color='danger' />
        {/* </StatGrid>

      <StatGrid> */}
        <StatCard label='Pending Registrations' value={pendingRegistrations} icon={Clock} color='warning' />
        <StatCard label='Transferred Out' value={stats.members_transferred_count} icon={LogOut} color='danger' />
      </StatGrid>

      <div className='row g-3'>
        <ChartCard title='Members by Status' variant='info' width={3}>
          {stats.members_by_status.length
            ? <Doughnut data={statusData} options={{ ...gridless, plugins: { ...gridless.plugins, legend: { ...gridless.plugins.legend, position: 'bottom' } } }} />
            : <p className='text-muted small'>No members yet.</p>}
        </ChartCard>

        <ChartCard title='Members by Household Role' variant='success' width={3}>
          {stats.members_by_household_role.length
            ? <Bar data={roleData} options={{ ...base, plugins: { ...base.plugins, legend: { display: false } } }} />
            : <p className='text-muted small'>No members yet.</p>}
        </ChartCard>

        <ChartCard title='Members by Membership Category' variant='warning' width={3}>
          {stats.members_by_membership_category.length
            ? <Bar data={categoryData} options={{ ...base, plugins: { ...base.plugins, legend: { display: false } } }} />
            : <p className='text-muted small'>No members yet.</p>}
        </ChartCard>

        <ChartCard title='Members by Church' variant='primary' width={3}>
          {stats.members_by_church.length
            ? <Doughnut data={churchData} options={{ ...gridless, plugins: { ...gridless.plugins, legend: { ...gridless.plugins.legend, position: 'bottom' } } }} />
            : <p className='text-muted small'>No members yet.</p>}
        </ChartCard>

        <ChartCard title='Registration Trend' variant='primary' width={6}>
          {stats.members_recent_trend.length
            ? <Line data={trendData} options={{ ...base, plugins: { ...base.plugins, legend: { display: false } } }} />
            : <p className='text-muted small'>No registrations recorded yet.</p>}
        </ChartCard>

        <ChartCard title='Membership Growth (Cumulative, Monthly)' variant='success' width={6}>
          {(stats.members_growth_monthly || []).length
            ? <Line data={growthData} options={{ ...base, plugins: { ...base.plugins, legend: { display: false } } }} />
            : <p className='text-muted small'>No members yet.</p>}
        </ChartCard>

        <ChartCard title='Contact Completeness' variant='info' width={12}>
          {completeness
            ? <Bar data={completenessData} options={{ ...base, indexAxis: 'y', plugins: { ...base.plugins, legend: { display: false } } }} />
            : <p className='text-muted small'>No members yet.</p>}
        </ChartCard>
      </div>
    </div>
  )
}
