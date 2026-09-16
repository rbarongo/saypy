import React, { useState } from 'react'
import { ChevronDown, ChevronUp, X, RefreshCw } from 'react-feather'

export default function Card({
  title,
  outline = false,
  variant, // 'primary', 'success', 'warning', 'danger', 'info'
  collapsible = false,
  removable = false,
  loading = false,
  onRemove,
  onRefresh,
  tools, // custom tools (React element)
  footer,
  children,
  className = '',
  bodyStyle = {},
  style = {}
}) {
  const [collapsed, setCollapsed] = useState(false)
  const [removed, setRemoved] = useState(false)

  if (removed) return null

  // Build card class list
  let cardClass = `card ${className}`
  if (outline) cardClass += ' card-outline'
  if (variant) cardClass += ` card-${variant}`

  return (
    <div className={cardClass} style={style}>
      {loading && (
        <div className="card-loading-overlay">
          <div className="card-loading-spinner" aria-label="Loading"></div>
        </div>
      )}
      
      {(title || collapsible || removable || tools || onRefresh) && (
        <div className="card-header">
          {title && (typeof title === 'string' ? <h3 className="card-title">{title}</h3> : title)}
          
          <div className="card-tools">
            {tools}
            {onRefresh && (
              <button 
                type="button" 
                className="btn btn-ghost btn-sm p-1 rounded-circle" 
                onClick={onRefresh}
                title="Refresh"
                style={{ border: 'none', background: 'transparent' }}
              >
                <RefreshCw size={14} />
              </button>
            )}
            {collapsible && (
              <button 
                type="button" 
                className="btn btn-ghost btn-sm p-1 rounded-circle" 
                onClick={() => setCollapsed(!collapsed)}
                title={collapsed ? 'Expand' : 'Collapse'}
                style={{ border: 'none', background: 'transparent' }}
              >
                {collapsed ? <ChevronDown size={14} /> : <ChevronUp size={14} />}
              </button>
            )}
            {removable && (
              <button 
                type="button" 
                className="btn btn-ghost btn-sm p-1 rounded-circle" 
                onClick={() => {
                  setRemoved(true)
                  if (onRemove) onRemove()
                }}
                title="Remove"
                style={{ border: 'none', background: 'transparent' }}
              >
                <X size={14} />
              </button>
            )}
          </div>
        </div>
      )}
      
      <div 
        className="card-body" 
        style={{ 
          display: collapsed ? 'none' : 'block', 
          ...bodyStyle 
        }}
      >
        {children}
      </div>
      
      {footer && !collapsed && (
        <div className="card-footer">
          {footer}
        </div>
      )}
    </div>
  )
}
