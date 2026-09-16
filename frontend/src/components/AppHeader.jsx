import React from 'react'
import { Menu } from 'react-feather'
import ThemeToggle from './ThemeToggle'
import UserMenu from './UserMenu'

export default function AppHeader({ appName, user, displayUserName, onLogout, onToggleSidebar, onOpenPreferences }){
  return (
    <nav className='navbar navbar-expand navbar-light navbar-bg app-topbar'>
      <button type='button' className='sidebar-toggle js-sidebar-toggle app-topbar-toggle' onClick={onToggleSidebar} aria-label='Toggle menu'>
        <Menu size={19} />
      </button>
      <div className='app-topbar-title'>
        <span className='app-topbar-kicker'>Command center</span>
        <span className='app-topbar-name'>{appName}</span>
      </div>

      <div className='navbar-collapse collapse d-flex justify-content-end'>
        <ul className='navbar-nav navbar-align align-items-center'>
          <li className='nav-item me-2'>
            <ThemeToggle />
          </li>
          <UserMenu user={user} displayUserName={displayUserName} onLogout={onLogout} onOpenPreferences={onOpenPreferences} />
        </ul>
      </div>
    </nav>
  )
}
