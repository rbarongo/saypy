import React from 'react'

// Callers use our own 'error' level (not Bootstrap's 'danger') — map it here
// so call sites don't all need touching.
const LEVEL_TO_BOOTSTRAP = { error: 'danger', success: 'success', warning: 'warning', info: 'info' }

export default function Alert({ level = 'info', title, children, style }){
  const bsLevel = LEVEL_TO_BOOTSTRAP[level] || 'info'
  return (
    <div className={`alert alert-${bsLevel}`} style={style}>
      {title && <div className='fw-bold'>{title}</div>}
      {children}
    </div>
  )
}
