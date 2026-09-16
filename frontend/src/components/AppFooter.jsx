import React from 'react'

export default function AppFooter({ systemName }){
  return (
    <footer className='footer'>
      <div className='container-fluid'>
        <div className='row text-muted'>
          <div className='col-6 text-start'>
            <p className='mb-0'>{systemName} &copy; {new Date().getFullYear()}</p>
          </div>
          <div className='col-6 text-end'>
            <span className='text-muted'>StewardHub Portal</span>
          </div>
        </div>
      </div>
    </footer>
  )
}
