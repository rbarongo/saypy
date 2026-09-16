import React from 'react'
import { useState } from 'react'
import ThemeToggle from './ThemeToggle'

function ChurchMark(){
  return (
    <svg viewBox='0 0 24 24' fill='none' xmlns='http://www.w3.org/2000/svg'>
      <path d='M12 2v3' stroke='#fff' strokeWidth='1.6' strokeLinecap='round'/>
      <path d='M10.5 3.5h3' stroke='#fff' strokeWidth='1.6' strokeLinecap='round'/>
      <path d='M12 5l8 6v10H4V11l8-6Z' stroke='#fff' strokeWidth='1.6' strokeLinejoin='round'/>
      <path d='M9.5 21v-6a2.5 2.5 0 0 1 5 0v6' stroke='#fff' strokeWidth='1.6' strokeLinejoin='round'/>
    </svg>
  )
}

function HeroIllustration(){
  return (
    <svg viewBox='0 0 420 320' fill='none' xmlns='http://www.w3.org/2000/svg'>
      <rect x='36' y='24' width='330' height='250' rx='20' fill='rgba(255,255,255,0.97)' />
      <rect x='36' y='24' width='330' height='40' rx='20' fill='rgba(15,23,42,0.06)' />
      <circle cx='58' cy='44' r='5' fill='#f87171' />
      <circle cx='76' cy='44' r='5' fill='#fbbf24' />
      <circle cx='94' cy='44' r='5' fill='#34d399' />
      <rect x='250' y='36' width='96' height='16' rx='8' fill='rgba(41,82,196,0.14)' />

      <rect x='58' y='84' width='140' height='18' rx='4' fill='#0f172a' opacity='0.85' />
      <rect x='58' y='108' width='96' height='12' rx='4' fill='#94a3b8' />

      <rect x='58' y='140' width='142' height='108' rx='12' fill='#eef2fd' />
      <rect x='76' y='218' width='16' height='20' rx='3' fill='#2952c4' />
      <rect x='100' y='198' width='16' height='40' rx='3' fill='#4f7cff' />
      <rect x='124' y='178' width='16' height='60' rx='3' fill='#2952c4' />
      <rect x='148' y='158' width='16' height='80' rx='3' fill='#7ca0ff' />
      <rect x='172' y='188' width='16' height='50' rx='3' fill='#4f7cff' />

      <rect x='216' y='140' width='150' height='34' rx='10' fill='#f8fafc' stroke='#e2e8f0' />
      <circle cx='236' cy='157' r='11' fill='#c7d7ff' />
      <rect x='254' y='150' width='90' height='7' rx='3.5' fill='#0f172a' opacity='0.55' />
      <rect x='254' y='161' width='60' height='6' rx='3' fill='#94a3b8' />

      <rect x='216' y='182' width='150' height='34' rx='10' fill='#f8fafc' stroke='#e2e8f0' />
      <circle cx='236' cy='199' r='11' fill='#bcecd2' />
      <rect x='254' y='192' width='90' height='7' rx='3.5' fill='#0f172a' opacity='0.55' />
      <rect x='254' y='203' width='70' height='6' rx='3' fill='#94a3b8' />

      <rect x='216' y='224' width='150' height='34' rx='10' fill='#f8fafc' stroke='#e2e8f0' />
      <circle cx='236' cy='241' r='11' fill='#ffe3b3' />
      <rect x='254' y='234' width='90' height='7' rx='3.5' fill='#0f172a' opacity='0.55' />
      <rect x='254' y='245' width='50' height='6' rx='3' fill='#94a3b8' />

      <g transform='translate(300,4)'>
        <circle cx='30' cy='30' r='30' fill='#ffffff' />
        <circle cx='30' cy='30' r='30' fill='none' stroke='rgba(255,255,255,0.5)' strokeWidth='2' />
        <text x='30' y='38' fontSize='26' textAnchor='middle'>&#128149;</text>
      </g>
      <g transform='translate(2,250)'>
        <circle cx='28' cy='28' r='28' fill='#ffffff' />
        <path d='M17 28l7 7 15-15' stroke='#2952c4' strokeWidth='3.5' strokeLinecap='round' strokeLinejoin='round' fill='none' />
      </g>
    </svg>
  )
}

function UserIcon(){
  return (
    <svg viewBox='0 0 24 24' fill='none' xmlns='http://www.w3.org/2000/svg'>
      <circle cx='12' cy='8' r='3.5' stroke='currentColor' strokeWidth='1.7'/>
      <path d='M4.5 20c1.4-4 4.2-6 7.5-6s6.1 2 7.5 6' stroke='currentColor' strokeWidth='1.7' strokeLinecap='round'/>
    </svg>
  )
}

function LockIcon(){
  return (
    <svg viewBox='0 0 24 24' fill='none' xmlns='http://www.w3.org/2000/svg'>
      <rect x='5' y='10.5' width='14' height='9.5' rx='2' stroke='currentColor' strokeWidth='1.7'/>
      <path d='M8 10.5V7.5a4 4 0 0 1 8 0v3' stroke='currentColor' strokeWidth='1.7' strokeLinecap='round'/>
    </svg>
  )
}

function EyeIcon({ open }){
  return open ? (
    <svg viewBox='0 0 24 24' fill='none' xmlns='http://www.w3.org/2000/svg'>
      <path d='M2.5 12S6 5.5 12 5.5 21.5 12 21.5 12 18 18.5 12 18.5 2.5 12 2.5 12Z' stroke='currentColor' strokeWidth='1.6' strokeLinejoin='round'/>
      <circle cx='12' cy='12' r='3' stroke='currentColor' strokeWidth='1.6'/>
    </svg>
  ) : (
    <svg viewBox='0 0 24 24' fill='none' xmlns='http://www.w3.org/2000/svg'>
      <path d='M3.5 3.5l17 17' stroke='currentColor' strokeWidth='1.6' strokeLinecap='round'/>
      <path d='M6.2 6.6C3.9 8.2 2.5 12 2.5 12S6 18.5 12 18.5c1.9 0 3.5-.5 4.9-1.3M10.3 5.7c.6-.1 1.1-.2 1.7-.2 6 0 9.5 6.5 9.5 6.5s-.7 1.4-2.1 2.9' stroke='currentColor' strokeWidth='1.6' strokeLinecap='round'/>
      <path d='M9.6 9.6a3 3 0 0 0 4.2 4.2' stroke='currentColor' strokeWidth='1.6' strokeLinecap='round'/>
    </svg>
  )
}

const FEATURES = [
  { icon: '💝', title: 'Collections', desc: 'Record and track offerings by member and service.' },
  { icon: '👥', title: 'Membership', desc: 'Keep member and family records organized.' },
  { icon: '📊', title: 'Reports', desc: 'Generate summaries for any period, ready to export.' },
]

export default function LoginScreen({ systemName, loginUser, setLoginUser, loginPass, setLoginPass, doLogin, statusInfo }){
  const [showPassword, setShowPassword] = useState(false)

  return (
    <div className='login-page'>
      <section className='login-hero'>
        <div className='login-hero-overlay' />
        <span className='login-hero-eyebrow'>StewardHub Portal</span>
        <h1>We make church offerings simple to manage</h1>
        <p>Record collections, keep membership up to date, and generate reports your leadership can trust — all in one place.</p>
        <div className='login-hero-art'><HeroIllustration /></div>
        <div className='login-features'>
          {FEATURES.map(f => (
            <div className='login-feature' key={f.title}>
              <div className='login-feature-icon'>{f.icon}</div>
              <div className='login-feature-title'>{f.title}</div>
              <div className='login-feature-desc'>{f.desc}</div>
            </div>
          ))}
        </div>
      </section>

      <section className='login-panel'>
        <div style={{position:'absolute',top:16,right:16}}>
          <ThemeToggle />
        </div>
        <div className='login-panel-inner'>
          <div className='login-brand'>
            <div className='login-brand-mark'><ChurchMark /></div>
            <div className='login-title'>{systemName}</div>
            <div className='login-subtitle'>Welcome back. Please sign in with your username and password.</div>
          </div>

          <div className='login-form'>
            <div className='input-icon-group'>
              <UserIcon />
              <input
                placeholder='Username'
                value={loginUser}
                style={{paddingLeft:'50px',borderLeft:'1px solid #cbd5e1'}}
                onChange={e=>setLoginUser(e.target.value)}
                onKeyDown={e=>{ if(e.key==='Enter') doLogin() }}
                autoComplete='username'
              />
            </div>
            <div className='input-icon-group'>
              <LockIcon />
              <input
                placeholder='Password'
                type={showPassword ? 'text' : 'password'}
                value={loginPass}
                style={{paddingLeft:'50px'}}
                onChange={e=>setLoginPass(e.target.value)}
                onKeyDown={e=>{ if(e.key==='Enter') doLogin() }}
                autoComplete='current-password'
              />
              <button
                type='button'
                className='input-icon-toggle'
                onClick={()=>setShowPassword(v=>!v)}
                aria-label={showPassword ? 'Hide password' : 'Show password'}
                title={showPassword ? 'Hide password' : 'Show password'}
                aria-pressed={showPassword}
              >
                <EyeIcon open={showPassword} />
              </button>
            </div>
            <button type='button' className='btn btn-primary' onClick={doLogin}>Sign In</button>
          </div>



          <p className='login-security-note'>
            For your security, never share your password. {systemName} staff will never ask for it by phone, SMS, or email.
          </p>

          <div className='login-help-box'>
            <div className='login-help-title'>Need help signing in?</div>
            <div className='login-help-line'>Contact your church administrator or data steward for access.</div>
          </div>

          <p className='login-security-note'>New here? <a href='/self-register'>Register as a member</a>.</p>
        </div>
      </section>
    </div>
  )
}
