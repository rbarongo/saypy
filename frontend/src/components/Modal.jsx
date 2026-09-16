import React from 'react'

export default function Modal({ title, onClose, children, maxWidth }){
  return (
    <>
      <div className='modal-backdrop show' />
      <div
        className='modal show d-block'
        tabIndex={-1}
        role='dialog'
      >
        <div className='modal-dialog modal-dialog-scrollable' style={maxWidth ? { maxWidth } : undefined}>
          <div className='modal-content'>
            {(title || onClose) && (
              <div className='modal-header'>
                {title && <h5 className='modal-title'>{title}</h5>}
                {onClose && <button type='button' className='btn-close' aria-label='Close' onClick={onClose} />}
              </div>
            )}
            <div className='modal-body'>{children}</div>
          </div>
        </div>
      </div>
    </>
  )
}
