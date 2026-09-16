import React from 'react'
import { Lock } from 'react-feather'
import Modal from './Modal'
import PageHeader from './members-core/PageHeader'

function GridModeGroup({ label, name, value, onChange }){
  return (
    <div className='subpanel'>
      <div className='card-title' style={{marginBottom:8}}>{label}</div>
      <label className='flex gap8' style={{alignItems:'center',fontWeight:600,color:'var(--text)'}}>
        <input type='radio' name={name} checked={value==='scrollable'} onChange={()=>onChange('scrollable')} />
        Scrollable
      </label>
      <label className='flex gap8 mt8' style={{alignItems:'center',fontWeight:600,color:'var(--text)'}}>
        <input type='radio' name={name} checked={value==='classic'} onChange={()=>onChange('classic')} />
        Classic
      </label>
    </div>
  )
}

export default function PreferencesPage({
  gridModeMain, setGridModeMain,
  uploadGridMode, setUploadGridMode,
  mappedPreviewMode, setMappedPreviewMode,
  showOwnPasswordForm, setShowOwnPasswordForm,
  ownCurrentPassword, setOwnCurrentPassword,
  ownNewPassword, setOwnNewPassword,
  ownConfirmPassword, setOwnConfirmPassword,
  submitOwnPasswordReset,
  PASSWORD_HINT,
}){
  return (
    <div>
      <PageHeader
        title='Preferences'
        subtitle='Personal display and security settings'
        stats={[
          { label: 'Main grid', value: gridModeMain },
          { label: 'Upload grid', value: uploadGridMode },
          { label: 'Preview', value: mappedPreviewMode },
        ]}
        actions={<button type='button' className='btn' onClick={()=>setShowOwnPasswordForm(true)}><Lock size={15} />Change Password</button>}
      />

      <div className='panel-card'>
        <div className='card-title'>My Grid Preferences</div>
        <div className='small mb8'>Choose how dense data tables should feel in your workspace.</div>
        <div className='form-grid'>
          <GridModeGroup label='Main Grids Mode' name='main-grid-mode-preferences' value={gridModeMain} onChange={setGridModeMain} />
          <GridModeGroup label='Upload Grids Mode' name='upload-grid-mode-preferences' value={uploadGridMode} onChange={setUploadGridMode} />
          <GridModeGroup label='Mapped Preview Mode' name='mapped-preview-mode-preferences' value={mappedPreviewMode} onChange={setMappedPreviewMode} />
        </div>
      </div>

      {showOwnPasswordForm && (
        <Modal title='Change my password' onClose={()=>setShowOwnPasswordForm(false)} maxWidth={560}>
          <div className='form-grid'>
            <input type='password' placeholder='Current password' value={ownCurrentPassword} onChange={e=>setOwnCurrentPassword(e.target.value)} />
            <input type='password' placeholder='New password' value={ownNewPassword} onChange={e=>setOwnNewPassword(e.target.value)} />
            <input type='password' placeholder='Confirm new password' value={ownConfirmPassword} onChange={e=>setOwnConfirmPassword(e.target.value)} />
          </div>
          <div className='hint mb8'>{PASSWORD_HINT}</div>
          <div className='modal-actions'>
            <button type='button' className='btn btn-primary' onClick={submitOwnPasswordReset}>Update Password</button>
            <button type='button' className='btn btn-ghost' onClick={()=>setShowOwnPasswordForm(false)}>Cancel</button>
          </div>
        </Modal>
      )}
    </div>
  )
}
