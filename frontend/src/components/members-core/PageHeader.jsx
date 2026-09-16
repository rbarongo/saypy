import React from 'react'

/** Consistent header for admin pages with optional action buttons and compact
 * data summaries. `icon` (a react-feather component) is optional — pages
 * that don't pass one just get a plain title, no layout shift. */
export default function PageHeader({ title, subtitle, actions, stats = [], icon: Icon }){
  return (
    <div className='core-page-header'>
      <div className='core-page-heading'>
        <div className='core-page-heading-row'>
          {Icon && <span className='core-page-icon'><Icon size={20} /></span>}
          <div>
            <h1 className='core-page-title'>{title}</h1>
            {subtitle && <div className='core-page-subtitle'>{subtitle}</div>}
          </div>
        </div>
        {stats.length > 0 && (
          <div className='page-stat-row'>
            {stats.map((stat, index) => (
              <div className='page-stat' key={`${stat.label}-${index}`}>
                <span className='page-stat-value'>{stat.value}</span>
                <span className='page-stat-label'>{stat.label}</span>
              </div>
            ))}
          </div>
        )}
      </div>
      {actions && <div className='core-page-actions'>{actions}</div>}
    </div>
  )
}
