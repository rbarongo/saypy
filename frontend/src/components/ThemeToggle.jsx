import React from 'react'
import { useEffect, useState } from 'react'
import { Moon, Sun } from 'react-feather'

function getInitialTheme(){
  const saved = localStorage.getItem('theme')
  if(saved === 'light' || saved === 'dark') return saved
  return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

export default function ThemeToggle(){
  const [theme, setTheme] = useState(getInitialTheme)

  useEffect(()=>{
    document.documentElement.setAttribute('data-theme', theme)
    // AdminKit's own CSS reacts to Bootstrap's `data-bs-theme` attribute for
    // dark mode (sidebar/navbar/cards/etc.) — keep it in sync with our own
    // theme flag so both stylesheets agree.
    document.documentElement.setAttribute('data-bs-theme', theme)
    localStorage.setItem('theme', theme)
  }, [theme])

  return (
    <button
      type='button'
      className='theme-toggle'
      title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
      onClick={()=>setTheme(t => t === 'dark' ? 'light' : 'dark')}
    >
      {theme === 'dark' ? <Sun size={17} /> : <Moon size={17} />}
    </button>
  )
}
