import React from 'react'

const base = {
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 2,
  strokeLinecap: 'round',
  strokeLinejoin: 'round',
}

export function IconDashboard(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <line x1='4' y1='6' x2='14' y2='6' /><circle cx='17' cy='6' r='2' />
      <line x1='10' y1='12' x2='20' y2='12' /><circle cx='7' cy='12' r='2' />
      <line x1='4' y1='18' x2='14' y2='18' /><circle cx='17' cy='18' r='2' />
    </svg>
  )
}

export function IconSliders(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <path d='M4 21v-7' /><path d='M4 10V3' /><circle cx='4' cy='13' r='2.2' />
      <path d='M12 21v-9' /><path d='M12 8V3' /><circle cx='12' cy='10' r='2.2' />
      <path d='M20 21v-5' /><path d='M20 12V3' /><circle cx='20' cy='14' r='2.2' />
    </svg>
  )
}

export function IconGift(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <rect x='3' y='9' width='18' height='4' rx='1' /><rect x='5' y='13' width='14' height='8' rx='1' />
      <line x1='12' y1='9' x2='12' y2='21' />
      <path d='M12 9C12 9 9.5 4 7 5.5C4.8 6.8 6.5 9 12 9Z' />
      <path d='M12 9C12 9 14.5 4 17 5.5C19.2 6.8 17.5 9 12 9Z' />
    </svg>
  )
}

export function IconUsers(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <circle cx='9' cy='8' r='3.2' /><path d='M2.8 20c1-3.6 3.4-5.5 6.2-5.5s5.2 1.9 6.2 5.5' />
      <circle cx='17.5' cy='8.5' r='2.4' /><path d='M15.5 14.7c2.3.2 4 1.9 5 5.3' />
    </svg>
  )
}

export function IconClipboard(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <rect x='5' y='4' width='14' height='17' rx='2' />
      <rect x='9' y='2.5' width='6' height='3.5' rx='1' />
      <line x1='8' y1='11' x2='16' y2='11' /><line x1='8' y1='15' x2='16' y2='15' /><line x1='8' y1='19' x2='13' y2='19' />
    </svg>
  )
}

export function IconBarChart(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <line x1='4' y1='20' x2='20' y2='20' />
      <rect x='6' y='13' width='3' height='7' /><rect x='11' y='9' width='3' height='11' /><rect x='16' y='5' width='3' height='15' />
    </svg>
  )
}

export function IconShield(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <path d='M12 3l7 3v6c0 4.5-3 7.5-7 9-4-1.5-7-4.5-7-9V6l7-3Z' />
      <path d='M9 12l2 2 4-4' />
    </svg>
  )
}

export function IconSettings(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <circle cx='12' cy='12' r='3' />
      <path d='M19.4 13.5a1.7 1.7 0 0 0 .34 1.87l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.7 1.7 0 0 0-1.87-.34 1.7 1.7 0 0 0-1.03 1.56V19.5a2 2 0 1 1-4 0v-.09A1.7 1.7 0 0 0 8.98 17.85a1.7 1.7 0 0 0-1.87.34l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.7 1.7 0 0 0 .34-1.87A1.7 1.7 0 0 0 3.06 12.4H3a2 2 0 1 1 0-4h.09A1.7 1.7 0 0 0 4.65 7.02a1.7 1.7 0 0 0-.34-1.87l-.06-.06A2 2 0 1 1 7.08 2.26l.06.06a1.7 1.7 0 0 0 1.87.34h.06A1.7 1.7 0 0 0 10.1 1.1V1a2 2 0 1 1 4 0v.09c0 .68.4 1.29 1.03 1.56h.06a1.7 1.7 0 0 0 1.87-.34l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.7 1.7 0 0 0-.34 1.87v.06c.27.63.88 1.03 1.56 1.03H21a2 2 0 1 1 0 4h-.09a1.7 1.7 0 0 0-1.56 1.03Z' />
    </svg>
  )
}

export function IconChevronDown(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <polyline points='6 9 12 15 18 9' />
    </svg>
  )
}

export function IconLogOut(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <path d='M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4' />
      <polyline points='16 17 21 12 16 7' /><line x1='21' y1='12' x2='9' y2='12' />
    </svg>
  )
}

export function IconUser(props){
  return (
    <svg viewBox='0 0 24 24' {...base} {...props}>
      <circle cx='12' cy='8' r='3.5' /><path d='M4.5 20c1.4-4 4.2-6 7.5-6s6.1 2 7.5 6' />
    </svg>
  )
}
