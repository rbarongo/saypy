import React, { useId } from 'react'

// Bootstrap's `.form-floating` pattern (see bootstrap-forms.scss for the
// scoped Bootstrap import + bootstrap-theme.css for theming it to match the
// app). Requires: a `placeholder` on the control (even though it's visually
// replaced by the label) and the <label> as the control's next sibling.

export function FloatingInput({ id, label, className = '', ...rest }){
  const autoId = useId()
  const fieldId = id || autoId
  return (
    <div className={`form-floating ${className}`}>
      <input id={fieldId} className='form-control' placeholder={label} {...rest} />
      <label htmlFor={fieldId}>{label}</label>
    </div>
  )
}

export function FloatingSelect({ id, label, className = '', children, ...rest }){
  const autoId = useId()
  const fieldId = id || autoId
  return (
    <div className={`form-floating ${className}`}>
      <select id={fieldId} className='form-select' {...rest}>{children}</select>
      <label htmlFor={fieldId}>{label}</label>
    </div>
  )
}

export function FloatingTextarea({ id, label, className = '', style, ...rest }){
  const autoId = useId()
  const fieldId = id || autoId
  return (
    <div className={`form-floating ${className}`}>
      <textarea id={fieldId} className='form-control' placeholder={label} style={{ height: 100, ...style }} {...rest} />
      <label htmlFor={fieldId}>{label}</label>
    </div>
  )
}

export function FormSwitch({ id, label, checked, onChange, className = '', disabled }){
  const autoId = useId()
  const fieldId = id || autoId
  return (
    <div className={`form-check form-switch ${className}`}>
      <input className='form-check-input' type='checkbox' role='switch' id={fieldId} checked={checked} onChange={onChange} disabled={disabled} />
      <label className='form-check-label' htmlFor={fieldId}>{label}</label>
    </div>
  )
}
