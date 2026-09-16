import React from 'react'

/** A modern segmented tab bar — replaces the ad-hoc Bootstrap `nav-tabs` +
 * inline-style pattern that used to be hand-rolled per page.
 * `tabs`: [{ id, label, icon? }]. */
export default function TabNav({ tabs, activeId, onSelect }) {
  return (
    <div className='tab-nav'>
      {tabs.map(tab => {
        const Icon = tab.icon
        return (
          <button
            key={tab.id}
            type='button'
            className={`tab-nav-item${tab.id === activeId ? ' tab-nav-item--active' : ''}`}
            onClick={() => onSelect(tab.id)}
          >
            {Icon && <Icon size={14} />}
            {tab.label}
          </button>
        )
      })}
    </div>
  )
}
