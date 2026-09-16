import React from 'react'
import { useEffect, useRef, useState } from 'react'
import { LogOut, Sliders } from 'react-feather'

function initialsFor(name){
  const parts = String(name || '').trim().split(/\s+/).filter(Boolean)
  if(!parts.length) return '?'
  if(parts.length === 1) return parts[0].slice(0, 2).toUpperCase()
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase()
}

export default function UserMenu({ user, displayUserName, onLogout, onOpenPreferences }){
  const [open, setOpen] = useState(false)
  const ref = useRef(null)
  const name = displayUserName(user)

  useEffect(()=>{
    function onDocClick(e){
      if(ref.current && !ref.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', onDocClick)
    return ()=> document.removeEventListener('mousedown', onDocClick)
  }, [])

  return (
    <li className='nav-item dropdown' ref={ref}>
      <a className='nav-link dropdown-toggle d-inline-flex align-items-center' href='#' onClick={e => { e.preventDefault(); setOpen(v => !v) }}>
        <span className='avatar-initials rounded-circle me-1'>{initialsFor(name)}</span>
        <span className='text-dark'>{name}</span>
      </a>
      <div className={`dropdown-menu dropdown-menu-end${open ? ' show' : ''}`}>
        <button type='button' className='dropdown-item' onClick={()=>{ setOpen(false); onOpenPreferences() }}>
          <Sliders size={16} className='align-middle me-1' /> Preferences
        </button>
        <div className='dropdown-divider' />
        <button type='button' className='dropdown-item text-danger' onClick={onLogout}>
          <LogOut size={16} className='align-middle me-1' /> Log out
        </button>
      </div>
    </li>
  )
}
