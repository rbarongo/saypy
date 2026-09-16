import React from 'react'
import { useState, useEffect } from 'react'

export default function CreateUserForm({onCreate, churches, scopedChurchId, canAccessChurch, onRestrictedChurchAttempt, roleOptions = ['uploader'], roleLabel = (r)=>String(r)}){
  const [u, setU] = useState('')
  const [p, setP] = useState('')
  const [firstName, setFirstName] = useState('')
  const [middleName, setMiddleName] = useState('')
  const [lastName, setLastName] = useState('')
  const [email, setEmail] = useState('')
  const [phone, setPhone] = useState('')
  const [c, setC] = useState(churches[0]?.id || null)
  const [r, setR] = useState((roleOptions && roleOptions.length ? roleOptions[0] : 'uploader'))
  const passwordHint = 'Password: minimum 8 characters, include at least 1 letter, 1 number, and 1 special character.'
  useEffect(()=>{
    if(scopedChurchId !== null && scopedChurchId !== undefined && !Number.isNaN(Number(scopedChurchId))){
      setC(String(scopedChurchId))
    }
  }, [scopedChurchId])
  return (
    <div>
      <div className='form-row'>
        <input placeholder='username' value={u} onChange={e=>setU(e.target.value)} />
        <input placeholder='password' value={p} onChange={e=>setP(e.target.value)} />
        <input placeholder='first name' value={firstName} onChange={e=>setFirstName(e.target.value)} />
        <input placeholder='middle name' value={middleName} onChange={e=>setMiddleName(e.target.value)} />
        <input placeholder='last name' value={lastName} onChange={e=>setLastName(e.target.value)} />
        <input placeholder='email' value={email} onChange={e=>setEmail(e.target.value)} />
        <input placeholder='phone' value={phone} onChange={e=>setPhone(e.target.value)} />
        <select value={c||''} onChange={e=>{
          const val = e.target.value
          if(typeof canAccessChurch === 'function' && !canAccessChurch(val)){
            if(typeof onRestrictedChurchAttempt === 'function') onRestrictedChurchAttempt('user creation')
            return
          }
          setC(val)
        }}>
          <option value=''>-- church --</option>
          {churches.map(ch=> <option key={ch.id} value={ch.id} disabled={typeof canAccessChurch === 'function' ? !canAccessChurch(ch.id) : false}>{ch.name}</option>)}
        </select>
        <select value={r} onChange={e=>setR(e.target.value)}>
          {(roleOptions && roleOptions.length ? roleOptions : ['uploader']).map(opt => <option key={opt} value={opt}>{roleLabel(opt)}</option>)}
        </select>
        <button type='button' className='btn btn-primary' onClick={()=> onCreate(u,p,c,r,firstName,middleName,lastName,email,phone)}>Create</button>
      </div>
      <div className='hint'>{passwordHint}</div>
    </div>
  )
}
