import React from 'react'

const COLOR_MAP = {
  primary: { fg: 'var(--accent)', bg: 'var(--accent-soft)' },
  info: { fg: 'var(--info)', bg: 'var(--info-soft)' },
  success: { fg: 'var(--success)', bg: 'var(--success-soft)' },
  warning: { fg: 'var(--warning)', bg: 'var(--warning-soft)' },
  danger: { fg: 'var(--danger)', bg: 'var(--danger-soft)' },
}

/** A single KPI tile — icon-left, value-over-label stacked to its right,
 * the same arrangement as PageHeader's icon + title/subtitle — with a
 * matching accent bar along the top edge. One shared design used everywhere
 * a page shows a row of headline counts (dashboard, members, users,
 * churches, ...) instead of each page inventing its own. */
export default function StatCard({ label, value, icon: Icon, color = 'primary' }) {
  const { fg, bg } = COLOR_MAP[color] || COLOR_MAP.primary
  return (
    <div className='stat-tile' style={{ '--stat-fg': fg, '--stat-bg': bg }}>
      <div className='stat-tile-row'>
        {Icon && <span className='stat-tile-icon'><Icon size={18} /></span>}
        <div className='stat-tile-text'>
          <span className='stat-tile-value'>{value}</span>
          <span className='stat-tile-label'>{label}</span>
        </div>
      </div>
    </div>
  )
}

/** Responsive grid wrapper — plain flex-wrap of equal-ish tiles. */
export function StatGrid({ children }) {
  return <div className='stat-tile-grid'>{children}</div>
}
