import React, { useState, useEffect } from 'react'
import './styles.css'

// Single-file frontend with Login, RBAC, and pages for Admin/Members/Collections/Reports
export default function App(){
  const [page, setPage] = useState('collections') // default landing
  const [token, setToken] = useState(localStorage.getItem('token') || '')
  const [user, setUser] = useState(JSON.parse(localStorage.getItem('user')||'null'))
  const [status, setStatus] = useState('')

  // ----- Shared data -----
  const [churches, setChurches] = useState([])

  useEffect(()=>{
    fetchChurches()
  }, [])

  useEffect(()=>{
    if(token){ localStorage.setItem('token', token) }
    else { localStorage.removeItem('token') }
    if(user){ localStorage.setItem('user', JSON.stringify(user)) }
    else { localStorage.removeItem('user') }
  }, [token, user])

  function authHeaders(extra={}){
    const h = { ...extra }
    if(token) h['Authorization'] = `Bearer ${token}`
    return h
  }

  async function fetchChurches(){
    try{ const res = await fetch('http://localhost:8000/churches'); const data = await res.json(); setChurches(data) }catch(e){}
  }

  // ----- Login / token management -----
  const [loginUser, setLoginUser] = useState('')
  const [loginPass, setLoginPass] = useState('')
  async function doLogin(){
    try{
      const res = await fetch('http://localhost:8000/users/login',{method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({username: loginUser, password: loginPass})})
      const data = await res.json()
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setToken(data.token)
      setUser(data.user)
      setStatus('Logged in')
      setPage('collections')
    }catch(e){ setStatus('Login failed: '+e.message) }
  }
  function logout(){ setToken(''); setUser(null); setStatus('Logged out'); }

  // ----- Admin: users -----
  const [usersList, setUsersList] = useState([])
  async function fetchUsers(){
    try{
      const res = await fetch('http://localhost:8000/users', { headers: authHeaders() })
      if(!res.ok) throw new Error('Not authorized')
      const data = await res.json()
      setUsersList(data)
    }catch(e){ setStatus('Fetch users failed: '+e.message) }
  }
  async function createUser(username, password, church, role){
    try{
      const body = { username, password, church, role }
      const res = await fetch('http://localhost:8000/users/register', { method:'POST', headers: authHeaders({'Content-Type':'application/json'}), body: JSON.stringify(body) })
      const data = await res.json()
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data))
      setStatus('User created')
      await fetchUsers()
    }catch(e){ setStatus('Create user failed: '+e.message) }
  }

  // ----- Members (list + update) -----
  const [members, setMembers] = useState([])
  const [membersQ, setMembersQ] = useState('')
  async function fetchMembers(q=''){
    try{
      const url = q? `http://localhost:8000/members?q=${encodeURIComponent(q)}` : 'http://localhost:8000/members'
      const res = await fetch(url, { headers: authHeaders() })
      const data = await res.json()
      setMembers(data)
      if(data && data.length && (!membersFields || membersFields.length===0)){
        // derive fields from first row, exclude internal timestamps
        const keys = Object.keys(data[0]).filter(k=> k !== 'created_at' && k !== 'id')
        setMembersFields(keys)
      }
    }catch(e){ setStatus('Failed to load members: '+e.message) }
  }
  async function updateMember(id, payload){
    try{
      const res = await fetch(`http://localhost:8000/members/${id}`, { method:'PUT', headers: authHeaders({'Content-Type':'application/json'}), body: JSON.stringify(payload) })
      if(!res.ok) throw new Error('Failed')
      setStatus('Member updated')
      await fetchMembers()
    }catch(e){ setStatus('Update failed: '+e.message) }
  }

  // Members UI local state (was missing and caused runtime errors)
  const [showMemberForm, setShowMemberForm] = useState(false)
  const [editingMember, setEditingMember] = useState(null)
  const [memberForm, setMemberForm] = useState({})
  const [membersFields, setMembersFields] = useState([])
  const [showAllMemberCols, setShowAllMemberCols] = useState(false)
  const [showCollectionsTable, setShowCollectionsTable] = useState(false)
  const [membersCollections, setMembersCollections] = useState([])
  const [membersCollectionsFields, setMembersCollectionsFields] = useState([])
  const [showCollectionsInReports, setShowCollectionsInReports] = useState(false)

  // Load members when navigating to Members page
  useEffect(()=>{
    if(page==='members') fetchMembers('')
  }, [page])

  // ----- Collections / Upload UI (reuse existing flow) -----
  // We'll keep the original upload flow components but scoped under Collections page
  // State for upload flow:
  const [step, setStep] = useState(1)
  const [file, setFile] = useState(null)
  const [headers, setHeaders] = useState([])
  const [preview, setPreview] = useState([])
  const [fullPreview, setFullPreview] = useState([])
  const [mapping, setMapping] = useState({})
  const [s1Column, setS1Column] = useState(null)
  const [collectionCodes, setCollectionCodes] = useState([])
  const [membersLocal, setMembersLocal] = useState([])
  const [selectedChurch, setSelectedChurch] = useState(churches[0]?.id || '')
  const [selectedDate, setSelectedDate] = useState('')
  const [uploaderName, setUploaderName] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [apiKeyStatus, setApiKeyStatus] = useState('')
  const [mappedPreview, setMappedPreview] = useState([])
  const [validationErrors, setValidationErrors] = useState([])

  useEffect(()=>{ fetchCodes(); fetchMembersLocal() }, [])
  useEffect(()=>{ if(step===3||step===2) recomputeMappedPreview() }, [step, mapping, fullPreview, selectedChurch, selectedDate, uploaderName])

  function authFetch(url, opts={}){
    const h = opts.headers? {...opts.headers} : {}
    if(token) h['Authorization'] = `Bearer ${token}`
    return fetch(url, {...opts, headers: h})
  }

  async function fetchCodes(){ try{ const res = await fetch('http://localhost:8000/collection_codes'); const data = await res.json(); setCollectionCodes(data) }catch(e){} }
  async function fetchMembersLocal(){ try{ const res = await fetch('http://localhost:8000/members'); const data = await res.json(); setMembersLocal(data) }catch(e){} }

  async function fetchMembersCollections(){
    try{
      const res = await authFetch('http://localhost:8000/reports/members_collections')
      const data = await res.json()
      if(!res.ok){ setStatus('Failed to load collections: '+(data.detail||JSON.stringify(data))); setMembersCollections([]); return }
      setMembersCollections(data)
      if(data && data.length && (!membersCollectionsFields || membersCollectionsFields.length===0)){
        setMembersCollectionsFields(Object.keys(data[0]).filter(k=> k !== 'added_at'))
      }
    }catch(e){ setStatus('Failed to load collections: '+e.message); setMembersCollections([]) }
  }

  function getOrderedCodes(codes){ if(!codes||!codes.length) return []; const filtered = codes.filter(c=> c.code && String(c.code).toUpperCase() !== 'UNUSED'); const priority = ['Sno','Jina','Zaka','Sadaka']; const out=[]; const used=new Set(); priority.forEach(p=>{ const found = filtered.find(c=> String(c.code).toLowerCase()===p.toLowerCase()); if(found){ out.push(found); used.add(found.column_name) } }); const rest = filtered.filter(c=> !used.has(c.column_name)).sort((a,b)=> (a.code||'').toString().localeCompare((b.code||'').toString())); return out.concat(rest) }

  async function uploadFile(){
    if(!file) return;
    const fd = new FormData(); fd.append('batch', file, file.name);
    setStatus('Uploading file...');
    try{
      const headers = {};
      if(apiKey) headers['X-API-KEY']=apiKey;
      const res = await authFetch('http://localhost:8000/upload/headers',{method:'POST', body: fd, headers});
      const data = await res.json();
      if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
      setHeaders(data.headers);
      setFullPreview(data.full_preview||[]);
      // use full preview as working preview so all rows are editable
      setPreview(data.full_preview||[]);
      setS1Column(data.s1_column||null);
      const m={};
      data.headers.forEach(h=> m[h]= data.suggestions && data.suggestions[h] ? data.suggestions[h] : '');
      setMapping(m);
      recomputeMappedPreview(m);
      setStatus('File uploaded');
      setStep(2);
    }catch(e){ setStatus('Upload failed: '+e.message) }
  }

  function coerceValue(key, val){ if(val===null||val===undefined||val==='') return null; if(typeof val==='number') return val; const sval = String(val).trim(); if(key==='s2'){ const d=new Date(sval); if(!isNaN(d)) return d.toISOString(); return sval } if(key==='s1'){ const n=parseInt(sval.replace(/[^0-9-]/g,''),10); return isNaN(n)? null: n } const sNumeric = new Set(['s3','s5','s6','s7','s8','s9','s13']); if(sNumeric.has(key) || key.startsWith('c') || key.startsWith('l')){ const n=Number(sval.replace(/[^0-9.\-]/g,'')); return isNaN(n)? null: n } return sval }

  function recomputeMappedPreview(mappingToUse = mapping){
    const rows = (fullPreview||[]).map(r=>{
      const out = {collection_code:'import'};
      if(selectedChurch) out.church = selectedChurch;
      if(selectedDate) out.s2 = new Date(selectedDate).toISOString();
      if(uploaderName) out.source = uploaderName;
      Object.keys(r).forEach(h=>{
        const mapped = mappingToUse[h];
        if(mapped){ out[mapped] = coerceValue(mapped, r[h]) }
      });
      try{
        const s2val = out.s2; let s2dt=null;
        if(typeof s2val==='string'){ const d=new Date(s2val); if(!isNaN(d)) s2dt=d }
        else if(s2val instanceof Date) s2dt=s2val;
        const s3val = out.s3; const s3int = s3val!=null? Number(s3val) : null;
        const church_id = out.church || selectedChurch || null;
        if(s2dt && s3int!=null && !out.s1){ const ymd = s2dt.toISOString().slice(0,10).replace(/-/g,''); const cidn = String(church_id||'').padStart(3,'0'); const s3n = String(Number(s3int)).padStart(3,'0'); out.s1 = parseInt(`${ymd}${cidn}${s3n}`,10) }
      }catch(e){}
      return out
    })
    setMappedPreview(rows);
    setValidationErrors([]);
    return rows
  }

  async function validateRows(rows){ try{ const headers = {'Content-Type':'application/json'}; if(apiKey) headers['X-API-KEY']=apiKey; const res = await authFetch('http://localhost:8000/members_collections/validate',{method:'POST', headers, body: JSON.stringify(rows)}); const data = await res.json(); if(!res.ok){ if(res.status===422 && data && data.detail && data.detail.validation_errors){ if(data.detail.rows) setMappedPreview(data.detail.rows); return data.detail.validation_errors || [] } throw new Error(data.detail||JSON.stringify(data)) } if(data.rows) setMappedPreview(data.rows); return data.validation_errors || [] }catch(err){ console.warn('Validation error', err); return [{error: err.message}] } }

  async function submitMapped(){
    // ensure mappedPreview is up-to-date and try extraction
    const rows = mappedPreview.length? mappedPreview : recomputeMappedPreview(mapping);
    const rows2 = recomputeMappedPreview(mapping, rows);
    // client-side pre-validation for missing required fields
    const pre = preValidateRows(rows2);
    if(pre && pre.length){ setValidationErrors(pre); setStep(4); setStatus('Pre-validation failed'); return }
    setStatus('Validating rows before submit...');
    const val = await validateRows(rows2);
    if(val && val.length){ setStatus('Validation failed'); setValidationErrors(val); setPage('collections'); setStep(4); return }
    setStatus('Submitting mapped rows...');
    try{
      const headers = {'Content-Type':'application/json'}; if(apiKey) headers['X-API-KEY']=apiKey;
      const res = await authFetch('http://localhost:8000/members_collections/bulk',{method:'POST', headers, body: JSON.stringify(rows2)});
      const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
      setStatus(`Inserted ${data.inserted} rows`); setStep(5)
    }catch(err){ setStatus('Submit failed: '+err.message) }
  }

  // ----- Reports -----
  const [reportRows, setReportRows] = useState([])
  const [reportFrom, setReportFrom] = useState('')
  const [reportTo, setReportTo] = useState('')
  const [aggRows, setAggRows] = useState([])
  async function fetchMembersCollectionReport(){
    try{
      let url = 'http://localhost:8000/reports/members_collections';
      if(reportFrom && reportTo) url += `?start_date=${encodeURIComponent(reportFrom)}&end_date=${encodeURIComponent(reportTo)}`;
      const res = await authFetch(url);
      const data = await res.json();
      if(!res.ok){ setStatus('Report failed: '+(data.detail||data.error||JSON.stringify(data))); setReportRows([]); return }
      if(!Array.isArray(data)){ setStatus('Report returned unexpected response'); setReportRows([]); return }
      setReportRows(data)
    }catch(e){ setStatus('Report failed: '+e.message); setReportRows([]) }
  }

  function aggregateByCollectionCode(){
    const rows = reportRows || [];
    const map = {};
    rows.forEach(r=>{
      const code = r.collection_code || 'unknown';
      if(!map[code]) map[code] = {collection_code: code, count:0, s5:0, s6:0, s7:0};
      map[code].count += 1;
      map[code].s5 += Number(r.s5||0);
      map[code].s6 += Number(r.s6||0);
      map[code].s7 += Number(r.s7||0);
    })
    setAggRows(Object.values(map));
  }

  // ----- Simple helpers for role checks -----
  function isAdmin(){ return user && user.role === 'admin' }
  function isUploader(){ return user && user.role === 'uploader' }

  // If not authenticated, show a focused login screen (no menu)
  if(!user){
    return (
      <div style={{fontFamily:'Arial',padding:18}}>
        <h2>Church Offerings — Login</h2>
        <div style={{marginTop:12}}>
          <input placeholder='username' value={loginUser} onChange={e=>setLoginUser(e.target.value)} />
          <input placeholder='password' type='password' value={loginPass} onChange={e=>setLoginPass(e.target.value)} />
          <button onClick={doLogin}>Login</button>
          <div style={{marginTop:8,color:'#666'}}>{status}</div>
        </div>
      </div>
    )
  }

  return (
    <div style={{fontFamily:'Arial',padding:18}}>
      <header style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
        <h2>Church Offerings — Admin Console</h2>
        <div>
          <div>
            <strong>{user.username}</strong> ({user.role})
            <button style={{marginLeft:8}} onClick={logout}>Logout</button>
          </div>
        </div>
      </header>

      <nav style={{marginTop:12, marginBottom:12}}>
        <button onClick={()=>setPage('collections')}>Collections</button>
        <button onClick={()=>setPage('members')}>Members</button>
        <button onClick={()=>setPage('reports')}>Reports</button>
        {isAdmin() && <button onClick={()=>{ setPage('admin'); fetchUsers() }}>Admin</button>}
        <span style={{marginLeft:12,color:'#666'}}>{status}</span>
      </nav>

      <main style={{borderTop:'1px solid #eee', paddingTop:12}}>
        {page==='admin' && (
          <div>
            <h3>Admin — Manage Users</h3>
            <div style={{marginBottom:8}}>
              <button onClick={fetchUsers}>Refresh users</button>
            </div>
            <div>
              <h4>Create user</h4>
              <CreateUserForm onCreate={(u,p,c,r)=> createUser(u,p,c,r)} churches={churches} />
            </div>
            <div style={{marginTop:12}}>
              <h4>Users</h4>
              <table border={1} cellPadding={6} style={{borderCollapse:'collapse'}}>
                <thead><tr><th>ID</th><th>Username</th><th>Role</th><th>Church</th></tr></thead>
                <tbody>
                  {usersList.map(u=> <tr key={u.id}><td>{u.id}</td><td>{u.username}</td><td>{u.role}</td><td>{u.church}</td></tr>)}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {page==='members' && (
          <div>
            <h3>Members</h3>
            <div style={{marginBottom:8}}>
              <input placeholder='Search members' value={membersQ} onChange={e=>setMembersQ(e.target.value)} />
              <button onClick={()=>fetchMembers(membersQ)}>Search</button>
              <button style={{marginLeft:8}} onClick={()=>{ setEditingMember(null); setMemberForm({}); setShowMemberForm(true); }}>New Member</button>
              <label style={{marginLeft:12}}><input type='checkbox' checked={showAllMemberCols} onChange={e=>setShowAllMemberCols(e.target.checked)} /> Show all columns</label>
            </div>

            <div style={{maxHeight:400, overflow:'auto'}}>
              <table style={{width:'100%', borderCollapse:'collapse'}}>
                <thead>
                  <tr>
                    {(showAllMemberCols? (membersFields || []) : ['sno','MEMBER_NAME','MEMBER_ID','PHONE','church']).map(h=> <th key={h}>{h}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {members.map(m=> (
                    <tr key={m.id} onClick={()=>{ setEditingMember(m); setMemberForm({...m}); setShowMemberForm(true); }} style={{cursor:'pointer'}}>
                      {(showAllMemberCols? (membersFields || []).map(h=> <td key={h}>{m[h]!==null&&m[h]!==undefined? String(m[h]): ''}</td>) : [m.sno, m.MEMBER_NAME, m.MEMBER_ID, m.PHONE, m.church].map((v,i)=> <td key={i}>{v||''}</td>))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {showMemberForm && (
              <div style={{marginTop:12,border:'1px solid #ddd',padding:8}}>
                <h4>{editingMember? 'Edit Member' : 'New Member'}</h4>
                <div style={{display:'flex',gap:8,flexWrap:'wrap'}}>
                  {(membersFields && membersFields.length? membersFields : Object.keys(memberForm||{})).map(key=>{
                    if(key==='id' || key==='created_at') return null
                    const val = memberForm[key]===undefined? '': memberForm[key]
                    if(key==='church'){
                      return (
                        <select key={key} value={val||''} onChange={e=>setMemberForm(prev=>({...prev,[key]: e.target.value}))}>
                          <option value=''>-- church --</option>
                          {churches.map(c=> <option key={c.id} value={c.id}>{c.name}</option>)}
                        </select>
                      )
                    }
                    const isNumber = typeof val === 'number' || key.toLowerCase().includes('id') || key.toLowerCase().includes('sno') || key.toLowerCase().includes('pledge')
                    return (
                      <input key={key} placeholder={key} value={val||''} type={isNumber? 'number':'text'} onChange={e=>setMemberForm(prev=>({...prev,[key]: isNumber? (e.target.value===''? null: Number(e.target.value)) : e.target.value }))} />
                    )
                  })}
                </div>
                <div style={{marginTop:8}}>
                  <button onClick={async ()=>{
                    try{
                      if(editingMember){
                        const res = await authFetch(`http://localhost:8000/members/${editingMember.id}`,{method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify(memberForm)});
                        const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
                        setStatus('Member updated');
                      }else{
                        const res = await authFetch('http://localhost:8000/members',{method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(memberForm)});
                        const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
                        setStatus('Member created');
                      }
                      setShowMemberForm(false);
                      setEditingMember(null);
                      setMemberForm({});
                      await fetchMembers('');
                    }catch(e){ setStatus('Save failed: '+e.message) }
                  }}>Save</button>
                  <button onClick={()=>{ setShowMemberForm(false); setEditingMember(null); setMemberForm({}) }} style={{marginLeft:8}}>Cancel</button>
                </div>
              </div>
            )}

            {showCollectionsTable && (
              <div style={{marginTop:12}}>
                <h4>Members Collections</h4>
                <div style={{maxHeight:300, overflow:'auto'}}>
                  <table style={{width:'100%', borderCollapse:'collapse'}}>
                    <thead>
                      <tr>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12) : ['id','collection_code','member_id','church','s1','s2','s3','s4','s5']).map(c=> <th key={c}>{c}</th>)}</tr>
                    </thead>
                    <tbody>
                      {membersCollections.map(r=> (
                        <tr key={r.id}><td>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12).map(k=> r[k]) : [r.id,r.collection_code,r.member_id,r.church,r.s1,r.s2,r.s3,r.s4,r.s5]).map((v,i)=><span key={i}>{v!==null&&v!==undefined?String(v):''}{i < 8? ' | ' : ''}</span>)}</td></tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {page==='collections' && (
          <div>
            <h3>Collections — Upload & Manage</h3>
            <div style={{marginBottom:8}}>
              <button onClick={()=>{ setShowCollectionsTable(s=>!s); if(!showCollectionsTable) fetchMembersCollections() }}>{showCollectionsTable? 'Hide Collections':'Show Collections'}</button>
            </div>
            <CollectionsUpload
              token={token}
              authFetch={authFetch}
              collectionCodes={collectionCodes}
              churches={churches}
              fetchCodes={fetchCodes}
              user={user}
            />
            {showCollectionsTable && (
              <div style={{marginTop:12}}>
                <h4>Members Collections</h4>
                <div style={{maxHeight:300, overflow:'auto'}}>
                  <table style={{width:'100%', borderCollapse:'collapse'}}>
                    <thead>
                      <tr>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12) : ['id','collection_code','member_id','church','s1','s2','s3','s4','s5']).map(c=> <th key={c}>{c}</th>)}</tr>
                    </thead>
                    <tbody>
                      {membersCollections.map(r=> (
                        <tr key={r.id}><td>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12).map(k=> r[k]) : [r.id,r.collection_code,r.member_id,r.church,r.s1,r.s2,r.s3,r.s4,r.s5]).map((v,i)=><span key={i}>{v!==null&&v!==undefined?String(v):''}{i < 8? ' | ' : ''}</span>)}</td></tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {page==='reports' && (
          <div>
            <h3>Reports</h3>
            <div>
              <h4>Members Collection Report</h4>
              <div style={{display:'flex', gap:8, alignItems:'center'}}>
                <label>From:</label>
                <input type='date' value={reportFrom} onChange={e=>setReportFrom(e.target.value)} />
                <label>To:</label>
                <input type='date' value={reportTo} onChange={e=>setReportTo(e.target.value)} />
                <button onClick={fetchMembersCollectionReport}>Run Report</button>
                <button style={{marginLeft:8}} onClick={async ()=>{ setShowCollectionsInReports(s=>!s); if(!showCollectionsInReports) await fetchMembersCollections(); }}>{showCollectionsInReports? 'Hide Collections':'View Collections'}</button>
              </div>
              <div style={{maxHeight:400, overflow:'auto', marginTop:8}}>
                <table style={{width:'100%', borderCollapse:'collapse'}}>
                  <thead>
                    <tr>
                      {reportRows[0] ? Object.keys(reportRows[0]).map(k=> <th key={k}>{labelForColumn(k)}</th>) : <th>No rows</th>}
                    </tr>
                  </thead>
                  <tbody>
                    {reportRows.map((r,idx)=> (
                      <tr key={idx}>{Object.keys(r).map(k=> <td key={k} style={{padding:6}}>{String(r[k]||'')}</td>)}</tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div style={{marginTop:8}}>
                <button onClick={aggregateByCollectionCode}>Aggregate by Collection Code</button>
                {aggRows && aggRows.length>0 && (
                  <div style={{marginTop:8}}>
                    <h4>Aggregated by Collection Code</h4>
                    <table style={{width:'100%',borderCollapse:'collapse'}}>
                      <thead><tr><th>Collection</th><th>Count</th><th>Total s5</th><th>Total s6</th><th>Total s7</th></tr></thead>
                      <tbody>{aggRows.map(a=> <tr key={a.collection_code}><td>{a.collection_code}</td><td>{a.count}</td><td>{a.s5}</td><td>{a.s6}</td><td>{a.s7}</td></tr>)}</tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>
              {showCollectionsInReports && (
                <div style={{marginTop:12}}>
                  <h4>Members Collections (Reports)</h4>
                  <div style={{maxHeight:300, overflow:'auto'}}>
                    <table style={{width:'100%', borderCollapse:'collapse'}}>
                      <thead>
                        <tr>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12) : ['id','collection_code','member_id','church','s1','s2','s3','s4','s5']).map(c=> <th key={c}>{c}</th>)}</tr>
                      </thead>
                      <tbody>
                        {membersCollections.map(r=> (
                          <tr key={r.id}><td>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12).map(k=> r[k]) : [r.id,r.collection_code,r.member_id,r.church,r.s1,r.s2,r.s3,r.s4,r.s5]).map((v,i)=><span key={i}>{v!==null&&v!==undefined?String(v):''}{i < 8? ' | ' : ''}</span>)}</td></tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
          </div>
        )}
      </main>
    </div>
  )
}

// ----- Subcomponents -----
function CreateUserForm({onCreate, churches}){
  const [u, setU] = useState('')
  const [p, setP] = useState('')
  const [c, setC] = useState(churches[0]?.id || null)
  const [r, setR] = useState('uploader')
  return (
    <div style={{display:'flex', gap:8, alignItems:'center'}}>
      <input placeholder='username' value={u} onChange={e=>setU(e.target.value)} />
      <input placeholder='password' value={p} onChange={e=>setP(e.target.value)} />
      <select value={c||''} onChange={e=>setC(e.target.value)}>
        <option value=''>-- church --</option>
        {churches.map(ch=> <option key={ch.id} value={ch.id}>{ch.name}</option>)}
      </select>
      <select value={r} onChange={e=>setR(e.target.value)}>
        <option value='uploader'>uploader</option>
        <option value='admin'>admin</option>
      </select>
      <button onClick={()=> onCreate(u,p,c,r)}>Create</button>
    </div>
  )
}

function CollectionsUpload({token, authFetch, collectionCodes, churches, fetchCodes, user}){
  // simplified copy of the prior upload UI kept local to this component scope
  const [step, setStep] = useState(1)
  const [file, setFile] = useState(null)
  const [headers, setHeaders] = useState([])
  const [preview, setPreview] = useState([])
  const [fullPreview, setFullPreview] = useState([])
  const [mapping, setMapping] = useState({})
  const [s1Column, setS1Column] = useState(null)
  const [selectedChurch, setSelectedChurch] = useState(churches[0]?.id || '')
  const [selectedDate, setSelectedDate] = useState('')
  const [uploaderName, setUploaderName] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [apiKeyStatus, setApiKeyStatus] = useState('')
  const [mappedPreview, setMappedPreview] = useState([])
  const [validationErrors, setValidationErrors] = useState([])

  // total steps (1=file/date/church, 2=mapping, 3=preview/edit, 4=fix, 5=done)
  const totalSteps = 5

  useEffect(()=>{ fetchCodes() }, [])
  useEffect(()=>{ if(user && !uploaderName) setUploaderName(user.username) }, [user])

  function coerceValue(key, val){ if(val===null||val===undefined||val==='') return null; if(typeof val==='number') return val; const sval = String(val).trim(); if(key==='s2'){ const d=new Date(sval); if(!isNaN(d)) return d.toISOString(); return sval } if(key==='s1'){ const n=parseInt(sval.replace(/[^0-9-]/g,''),10); return isNaN(n)? null: n } const sNumeric = new Set(['s3','s5','s6','s7','s8','s9','s13']); if(sNumeric.has(key) || key.startsWith('c') || key.startsWith('l')){ const n=Number(sval.replace(/[^0-9.\-]/g,'')); return isNaN(n)? null: n } return sval }

  async function uploadFile(){
    if(!file) return;
    const fd = new FormData(); fd.append('batch', file, file.name);
    try{
      const headers = {};
      const res = await authFetch('http://localhost:8000/upload/headers',{method:'POST', body: fd, headers});
      const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
      setHeaders(data.headers);
      setFullPreview(data.full_preview||[]);
      // prefer server preview when non-empty, otherwise use full_preview
      const initialPreview = (data.preview && data.preview.length) ? data.preview : (data.full_preview||[]);
      setPreview(initialPreview);
      setS1Column(data.s1_column||null);

      // Build initial mapping: prefer server suggestions, else try to match by collectionCodes.code
      const m = {};
      data.headers.forEach(h=>{
        const suggested = data.suggestions && data.suggestions[h] ? data.suggestions[h] : '';
        if(suggested){ m[h]=suggested; return }
        // try to match header text to a collectionCodes.code (case-insensitive)
        const found = (collectionCodes||[]).find(c=> {
          if(!c || !c.code) return false
          const code = String(c.code||'').toLowerCase();
          const hh = String(h||'').toLowerCase();
          return code === hh || hh.includes(code) || code.includes(hh)
        })
        if(found) m[h] = found.column_name
        else m[h] = ''
      })

      setMapping(m);
      // compute mapped preview immediately using the rows returned by the server
      recomputeMappedPreview(m, initialPreview);
      setStep(2)
    }catch(e){ alert('Upload failed: '+e.message) }
  }

  function recomputeMappedPreview(mappingToUse = mapping, rowsSource = null){
    const rowsSrc = rowsSource || preview || fullPreview || [];
    const rows = (rowsSrc||[]).map(r=>{
      const out = {collection_code:'import'};
      if(selectedChurch) out.church = selectedChurch;
      if(selectedDate) out.s2 = new Date(selectedDate).toISOString();
      if(uploaderName) out.source = uploaderName;
      Object.keys(r).forEach(h=>{
        const mapped = mappingToUse[h];
        if(mapped){ out[mapped] = coerceValue(mapped, r[h]) }
      })
      try{
        const s2val = out.s2; let s2dt=null;
        if(typeof s2val==='string'){ const d=new Date(s2val); if(!isNaN(d)) s2dt=d }
        else if(s2val instanceof Date) s2dt=s2val;
        const s3val = out.s3; const s3int = s3val!=null? Number(s3val) : null;
        const church_id = out.church || selectedChurch || null;
        if(s2dt && s3int!=null && !out.s1){ const ymd = s2dt.toISOString().slice(0,10).replace(/-/g,''); const cidn = String(church_id||'').padStart(3,'0'); const s3n = String(Number(s3int)).padStart(3,'0'); out.s1 = parseInt(`${ymd}${cidn}${s3n}`,10) }
        // If s3 missing but s1 is a combined serial, try to extract s3 and church from s1
        if((out.s3===null || out.s3===undefined) && out.s1){
          try{
            const s1s = String(out.s1).replace(/[^0-9]/g,'');
            if(s1s.length >= 14){
              const s3ex = parseInt(s1s.slice(-3),10);
              const churchEx = parseInt(s1s.slice(-6,-3),10);
              if(!isNaN(s3ex)) out.s3 = s3ex;
              if(out.church==null && !isNaN(churchEx)) out.church = churchEx;
            }
          }catch(e){}
        }
      }catch(e){}
      return out
    })
    setMappedPreview(rows); setValidationErrors([]); return rows
  }

  function preValidateRows(rows){
    const msgs = [];
    const rs = rows && rows.length? rows : mappedPreview;
    rs.forEach((r, idx)=>{
      const rowNum = idx+1;
      // s2
      if(!r.s2){ msgs.push(`Row ${rowNum}: s2 (date) is missing. Set Date in Step 1 or map a column to s2.`) }
      // church
      if(!r.church){ msgs.push(`Row ${rowNum}: church is missing. Select Church in Step 1 or map a column to church.`) }
      // s3
      const s3val = r.s3;
      const s1val = r.s1;
      if((s3val===null || s3val===undefined || s3val==='') && !(s1val && String(s1val).replace(/[^0-9]/g,'').length >= 14)){
        msgs.push(`Row ${rowNum}: s3 (serial) is missing — map your serial column to s3 or ensure s1 contains the combined serial.`)
      }
      // s4
      if(!r.s4){ msgs.push(`Row ${rowNum}: s4 (name) is missing. Map the name column to s4.`) }
    })
    return msgs;
  }

  function formatValidationErrors(errs){
    if(!errs || !errs.length) return [];
    const out = [];
    errs.forEach(e=>{
      if(typeof e === 'string'){
        out.push(e);
        return;
      }
      const idx = e.index != null ? e.index : '?';
      const items = e.errors || [];
      items.forEach(it=>{
        let loc = it.loc;
        if(Array.isArray(loc)) loc = loc.join('.');
        const msg = it.msg || (it.message || JSON.stringify(it));
        out.push(`Row ${Number(idx)+1}: ${loc} — ${msg}`);
      })
    })
    return out;
  }

  function labelForColumn(col){
    if(!col) return '';
    const found = (collectionCodes||[]).find(c=> c.column_name === col || c.code === col);
    if(found && found.code) return found.code;
    // fallback: humanize common names
    const human = {
      s1: 'Sno', s2: 'Date', s3: 'Serial', s4: 'Name'
    }[col];
    return human || col;
  }

  async function validateRows(rows){ try{ const headers = {'Content-Type':'application/json'}; const res = await authFetch('http://localhost:8000/members_collections/validate',{method:'POST', headers, body: JSON.stringify(rows)}); const data = await res.json(); if(!res.ok){ if(res.status===422 && data && data.detail && data.detail.validation_errors){ if(data.detail.rows) setMappedPreview(data.detail.rows); return data.detail.validation_errors || [] } throw new Error(data.detail||JSON.stringify(data)) } if(data.rows) setMappedPreview(data.rows); return data.validation_errors || [] }catch(err){ return [{error: err.message}] } }

  async function submitMapped(){ const rows = mappedPreview.length? mappedPreview : recomputeMappedPreview(); const val = await validateRows(rows); if(val && val.length){ setValidationErrors(val); setStep(4); alert('Validation errors present'); return } try{ const headers = {'Content-Type':'application/json'}; const res = await authFetch('http://localhost:8000/members_collections/bulk',{method:'POST', headers, body: JSON.stringify(rows)}); const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data)); alert(`Inserted ${data.inserted} rows`); setStep(5) }catch(e){ alert('Submit failed: '+e.message) } }

  return (
    <div>
      <div style={{marginBottom:8,color:'#333'}}>Step {step} of {totalSteps}</div>
      {step===1 && (
        <div>
          <div>
            <input type='file' onChange={e=>setFile(e.target.files[0])} />
          </div>
          <div style={{marginTop:8}}>
            <label>Date:</label>
            <input type='date' value={selectedDate} onChange={e=>{ setSelectedDate(e.target.value) }} />
            <label style={{marginLeft:8}}>Church:</label>
            <select value={selectedChurch||''} onChange={e=>{ setSelectedChurch(e.target.value) }}>
              <option value=''>-- select --</option>
              {churches.map(c=> <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
            <label style={{marginLeft:8}}>Uploader name:</label>
            <input value={uploaderName} onChange={e=>setUploaderName(e.target.value)} />
          </div>
          <div style={{marginTop:8}}>
            <button onClick={uploadFile}>Upload & Inspect</button>
          </div>
        </div>
      )}
      {step===2 && (
        <div>
          <div>
            <label>Date:</label>
            <input type='date' value={selectedDate} onChange={e=>{ setSelectedDate(e.target.value); recomputeMappedPreview() }} />
            <label style={{marginLeft:8}}>Church:</label>
            <select value={selectedChurch||''} onChange={e=>{ setSelectedChurch(e.target.value); recomputeMappedPreview() }}>
              <option value=''>-- select --</option>
              {churches.map(c=> <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
            <label style={{marginLeft:8}}>Uploader name:</label>
            <input value={uploaderName} onChange={e=>{ setUploaderName(e.target.value); recomputeMappedPreview() }} />
          </div>
          <div style={{marginTop:8}}>
                <table border={1} cellPadding={6} style={{borderCollapse:'collapse'}}>
              <thead><tr><th>Header</th><th>Map to</th></tr></thead>
              <tbody>{headers.map(h=> (
                <tr key={h}><td>{h}</td><td>
                  <select value={mapping[h]||''} onChange={e=>{ setMapping(prev=> ({...prev, [h]: e.target.value})); recomputeMappedPreview({...mapping, [h]: e.target.value}) }}>
                    <option value=''>-- none --</option>
                    {(collectionCodes||[]).map(c=> <option key={c.column_name} value={c.column_name}>{c.column_name} — {c.code}</option>)}
                  </select>
                </td></tr>
              ))}</tbody>
            </table>
              </div>
              <div style={{marginTop:8, display:'flex', gap:8, alignItems:'center'}}>
                <button onClick={()=>setStep(1)}>Back</button>
                <button onClick={()=>setStep(3)} style={{marginLeft:8}}>Next — Preview</button>
                <button onClick={async ()=>{
                    // build header mappings array
                    const arr = Object.keys(mapping).map(h=> ({ header_name: h, mapped_column: mapping[h] }));
                    try{
                      const res = await authFetch('http://localhost:8000/header_mappings',{method:'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(arr)});
                      const data = await res.json(); if(!res.ok) throw new Error(data.detail||JSON.stringify(data));
                      alert('Mappings saved');
                    }catch(e){ alert('Save mapping failed: '+e.message) }
                }} style={{marginLeft:8}}>Save mapping</button>
              </div>
        </div>
      )}

      {step===3 && (
        <div>
          <h4>Mapped preview ({mappedPreview.length} rows)</h4>
          <div style={{maxHeight:400, overflow:'auto'}}>
            <table style={{width:'100%'}}>
              <thead><tr>{mappedPreview[0] ? Object.keys(mappedPreview[0]).map(k=> <th key={k}>{labelForColumn(k)}</th>) : <th>No rows</th>}</tr></thead>
              <tbody>{mappedPreview.map((r,idx)=> (
                <tr key={idx}>{Object.keys(r).map(k=> <td key={k}><input value={r[k]||''} onChange={e=>{ const v=e.target.value; setMappedPreview(prev=>{ const nxt=[...prev]; nxt[idx] = {...nxt[idx], [k]: v}; return nxt }) }} /></td>)}</tr>
              ))}</tbody>
            </table>
          </div>
          <div style={{marginTop:8}}>
            <button onClick={()=>setStep(2)}>Back</button>
            <button onClick={async ()=>{ await submitMapped() }} style={{marginLeft:8}}>Validate & Submit</button>
          </div>
        </div>
      )}

      {step===4 && (
        <div>
          <h4>Fix validation errors</h4>
          {validationErrors && validationErrors.length ? (
            <ul>
              {formatValidationErrors(validationErrors).map((m,i)=> <li key={i}>{m}</li>)}
            </ul>
          ) : (
            <div>No validation errors</div>
          )}
          <button onClick={()=>setStep(3)}>Back to preview</button>
        </div>
      )}

      {step===5 && (
        <div>
          <h4>Done</h4>
          <button onClick={()=>{ setStep(1); setMappedPreview([]); setPreview([]); setFullPreview([]) }}>Start another</button>
        </div>
      )}
    </div>
  )
}
 