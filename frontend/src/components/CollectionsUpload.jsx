import React from 'react'
import { useState, useEffect } from 'react'
import Alert from './Alert'
import Card from './Card'
import Modal from './Modal'
import { encodeS1, decodeS1 } from '../lib/collectionSerial'

export default function CollectionsUpload({ token, authFetch, collectionCodes, churches, fetchCodes, user, labelForColumn, scopedChurchId, onRestrictedChurchAttempt, uploadGridMode, mappedPreviewMode, canManageCollections, membersLocal }) {
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
  const [promptState, setPromptState] = useState({ open: false, level: 'info', message: '' })
  const [copiedPrompt, setCopiedPrompt] = useState(false)
  const [showQuickForm, setShowQuickForm] = useState(false)
  const [quickRows, setQuickRows] = useState([{ id: 1, selected_member_id: '', name: '', phone: '', collection_date: '', total_amount: '', detail_entries: [], selected_detail: '', selected_detail_amount: '' }])
  const [quickSubmitting, setQuickSubmitting] = useState(false)

  const totalSteps = 5

  useEffect(() => { fetchCodes() }, [])
  useEffect(() => {
    if (user && !uploaderName) {
      const first = String(user?.first_name || '').trim()
      const middle = String(user?.middle_name || '').trim()
      const last = String(user?.last_name || '').trim()
      const full = [first, middle, last].filter(Boolean).join(' ')
      setUploaderName(full || String(user?.username || ''))
    }
  }, [user])
  useEffect(() => {
    if (scopedChurchId !== null && scopedChurchId !== undefined && !Number.isNaN(Number(scopedChurchId))) {
      setSelectedChurch(String(scopedChurchId))
    }
  }, [scopedChurchId])

  function savedCollectionDateKey() {
    const userKey = String(user?.id || user?.username || '').trim()
    return userKey ? `saypy.collection.selectedDate.${userKey}` : ''
  }

  useEffect(() => {
    const storageKey = savedCollectionDateKey()
    if (!storageKey) return
    try {
      const savedValue = localStorage.getItem(storageKey)
      if (savedValue) {
        setSelectedDate(savedValue)
      }
    } catch (e) { }
  }, [user?.id, user?.username])

  function saveSelectedCollectionDate() {
    const storageKey = savedCollectionDateKey()
    if (!storageKey) {
      showPrompt('warning', 'Unable to save date selection because no user context was found.')
      return
    }
    if (!selectedDate) {
      showPrompt('warning', 'Choose a date before saving the selection.')
      return
    }
    try {
      localStorage.setItem(storageKey, selectedDate)
      showPrompt('success', `Saved date selection for ${String(user?.username || user?.id || 'current user')}: ${formatPreviewDate(selectedDate)}`)
    } catch (e) {
      showPrompt('error', 'Failed to save date selection: ' + (e?.message || String(e)))
    }
  }

  function canAccessChurch(churchId) {
    if (churchId === null || churchId === undefined || churchId === '') return true
    if (scopedChurchId === null || scopedChurchId === undefined || Number.isNaN(Number(scopedChurchId))) return true
    return Number(churchId) === Number(scopedChurchId)
  }

  function handleRestrictedChurch(context) {
    if (typeof onRestrictedChurchAttempt === 'function') onRestrictedChurchAttempt(context)
    showPrompt('error', `Restricted information: you cannot access ${context} for another church`)
  }

  function localCollectionCodeMeta(col) {
    if (!col) return null
    const matches = (collectionCodes || []).filter(c => c.column_name === col || c.code === col)
    if (!matches.length) return null
    const preferred = matches.find(c => Number(c.church) === Number(scopedChurchId)) || matches.find(c => c.church == null) || matches[0]
    return preferred || null
  }

  function localLabelForColumn(col) {
    if (!col) return ''
    const found = localCollectionCodeMeta(col)
    if (found) return found.custom_collection_name || found.code || found.column_name || col
    return labelForColumn(col)
  }

  function mappedPreviewColumnsLocal() {
    const first = mappedPreview[0]
    if (!first) return []
    return Object.keys(first).filter(k => {
      if (['collection_code', 'church', 's2', 'source'].includes(k)) return false
      return localLabelForColumn(k) !== 'NA.'
    })
  }

  function currentChurchNameLocal() {
    return (churches || []).find(c => Number(c.id) === Number(selectedChurch || scopedChurchId))?.name || String(selectedChurch || scopedChurchId || '-')
  }

  function formatPreviewDate(value) {
    if (!value) return '-'
    const dt = new Date(value)
    if (Number.isNaN(dt.getTime())) return String(value)
    return new Intl.DateTimeFormat('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
    }).format(dt)
  }

  const visibleChurches = (churches || []).filter(c => canAccessChurch(c.id))

  const quickDetailOptions = (collectionCodes || [])
    .filter(c => {
      const churchOk = c && (c.church == null || Number(c.church) === Number(scopedChurchId))
      if (!churchOk) return false
      const col = String(c.column_name || '').toLowerCase()
      const codeLabel = String(c.code || '').trim().toUpperCase()
      if (!col) return false
      if (codeLabel === 'UNUSED') return false
      if (['collection_code', 'church', 'source', 'notes'].includes(col)) return false
      if (col === 's1' || col === 's2' || col === 's3' || col === 's4') return false
      return true
    })
    .sort((a, b) => {
      const al = String(a.custom_collection_name || a.code || a.column_name || '').toLowerCase()
      const bl = String(b.custom_collection_name || b.code || b.column_name || '').toLowerCase()
      return al.localeCompare(bl)
    })

  function makeQuickRow() {
    return {
      id: Date.now() + Math.floor(Math.random() * 1000),
      selected_member_id: '',
      name: '',
      phone: '',
      collection_date: selectedDate || '',
      total_amount: '',
      detail_entries: [],
      selected_detail: '',
      selected_detail_amount: '',
    }
  }

  function updateQuickRow(rowId, patch) {
    setQuickRows(prev => prev.map(r => r.id === rowId ? { ...r, ...patch } : r))
  }

  function addQuickRow() {
    setQuickRows(prev => [...prev, makeQuickRow()])
  }

  function removeQuickRow(rowId) {
    setQuickRows(prev => {
      const next = prev.filter(r => r.id !== rowId)
      return next.length ? next : [makeQuickRow()]
    })
  }

  function addQuickDetailEntry(rowId) {
    let warning = ''
    setQuickRows(prev => prev.map(row => {
      if (row.id !== rowId) return row
      const selectedDetail = String(row.selected_detail || '').trim()
      const selectedAmount = Number(String(row.selected_detail_amount || '').trim())
      if (!selectedDetail) {
        warning = 'Select a collection detail item before clicking Add item.'
        return row
      }
      if (!Number.isFinite(selectedAmount) || selectedAmount <= 0) {
        warning = 'Enter a valid collection detail amount greater than zero.'
        return row
      }
      if ((row.detail_entries || []).some(e => e.code === selectedDetail)) {
        warning = 'This collection detail item is already added. Remove it first to change the amount.'
        return row
      }
      const nextEntries = [...(row.detail_entries || []), { code: selectedDetail, amount: selectedAmount }]
      const autoTotal = nextEntries.reduce((sum, e) => sum + Number(e?.amount || 0), 0)
      return {
        ...row,
        detail_entries: nextEntries,
        total_amount: String(autoTotal),
        selected_detail: '',
        selected_detail_amount: '',
      }
    }))
    if (warning) showPrompt('warning', warning)
  }

  function removeQuickDetailEntry(rowId, code) {
    setQuickRows(prev => prev.map(row => {
      if (row.id !== rowId) return row
      const nextEntries = (row.detail_entries || []).filter(e => e.code !== code)
      const autoTotal = nextEntries.reduce((sum, e) => sum + Number(e?.amount || 0), 0)
      return {
        ...row,
        detail_entries: nextEntries,
        total_amount: String(autoTotal),
      }
    }))
  }

  function quickRowDetailsTotal(row) {
    return (row?.detail_entries || []).reduce((sum, e) => sum + Number(e?.amount || 0), 0)
  }

  async function submitQuickRows() {
    if (!canManageCollections) {
      showPrompt('warning', 'Your role can view the Collections menu but cannot submit collection rows.')
      return
    }
    const effectiveChurch = selectedChurch || scopedChurchId
    if (!effectiveChurch) {
      showPrompt('warning', 'Select a church before submitting collection rows.')
      return
    }
    if (!quickDetailOptions.length) {
      showPrompt('warning', 'No collection detail items are configured for this church. Add collection codes first.')
      return
    }

    const issues = []
    const rowsForBulk = []
    quickRows.forEach((r, idx) => {
      const rowNo = idx + 1
      const selectedMemberIdRaw = String(r.selected_member_id || '').trim()
      const selectedMemberId = selectedMemberIdRaw ? Number(selectedMemberIdRaw) : null
      const selectedMember = selectedMemberId ? (membersLocal || []).find(m => {
        const canonical = Number(m?.MEMBER_ID ?? m?.member_id ?? m?.id ?? 0)
        return canonical === selectedMemberId
      }) : null
      const canonicalMemberId = selectedMember ? Number(selectedMember?.MEMBER_ID ?? selectedMember?.member_id ?? selectedMember?.id ?? 0) : null
      const name = String(r.name || selectedMember?.MEMBER_NAME || '').trim()
      const phone = String(r.phone || selectedMember?.PHONE || '').trim()
      const dateText = String(r.collection_date || '').trim()
      const totalAmount = Number(String(r.total_amount || '').trim())
      const detailEntries = Array.isArray(r.detail_entries) ? r.detail_entries : []
      const detailTotal = detailEntries.reduce((sum, entry) => sum + Number(entry?.amount || 0), 0)
      if (!name) issues.push(`Row ${rowNo}: Name is required.`)
      if (!dateText) issues.push(`Row ${rowNo}: Date of collection is required.`)
      if (!Number.isFinite(totalAmount) || totalAmount <= 0) issues.push(`Row ${rowNo}: Total collection amount must be greater than zero.`)
      if (!detailEntries.length) issues.push(`Row ${rowNo}: Add at least one collection detail item.`)
      if (Math.abs(detailTotal - totalAmount) > 0.009) {
        issues.push(`Row ${rowNo}: Sum of detail amounts (${detailTotal.toFixed(2)}) does not match total collection (${totalAmount.toFixed(2)}).`)
      }
      const parsed = dateText ? new Date(dateText) : null
      if (parsed && !Number.isNaN(parsed.getTime())) {
        detailEntries.forEach(entry => {
          const detailCol = entry.code
          const detailAmount = Number(entry.amount || 0)
          const option = quickDetailOptions.find(o => o.column_name === detailCol)
          if (!option) return
          rowsForBulk.push({
            collection_code: detailCol,
            church: effectiveChurch,
            s2: parsed.toISOString(),
            s4: name,
            phone: phone || null,
            member_id: canonicalMemberId || null,
            source: uploaderName || String(user?.username || ''),
            notes: `${option.custom_collection_name || option.code || option.column_name} | Total: ${totalAmount}`,
            [detailCol]: detailAmount,
          })
        })
      }
    })

    if (issues.length) {
      showPrompt('warning', issues.join('\n'))
      return
    }
    if (!rowsForBulk.length) {
      showPrompt('warning', 'No valid rows were prepared for submission.')
      return
    }

    setQuickSubmitting(true)
    try {
      const validateRes = await authFetch('http://localhost:8000/members_collections/validate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(rowsForBulk),
      })
      const validateData = await validateRes.json().catch(() => ({}))
      if (!validateRes.ok) {
        throw new Error(validateData.detail || JSON.stringify(validateData) || 'Validation failed')
      }
      const validateErrors = Array.isArray(validateData.validation_errors) ? validateData.validation_errors : []
      if (validateErrors.length) {
        setValidationErrors(validateErrors)
        setStep(4)
        showPrompt('warning', 'Quick form validation found issues. Review Step 4 for details.')
        return
      }

      const res = await authFetch('http://localhost:8000/members_collections/bulk', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(rowsForBulk),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data) || 'Submit failed')
      showPrompt('success', `Saved ${data.inserted || 0} collection entries. You can keep adding more rows before closing the form.`)
      setQuickRows([makeQuickRow()])
    } catch (e) {
      showPrompt('error', 'Quick collection submit failed: ' + (e?.message || String(e)))
    } finally {
      setQuickSubmitting(false)
    }
  }

  function showPrompt(level, message) {
    setCopiedPrompt(false)
    setPromptState({ open: true, level: level || 'info', message: String(message || '') })
  }

  async function copyPromptMessage() {
    try {
      if (promptState && promptState.message) {
        await navigator.clipboard.writeText(promptState.message)
        setCopiedPrompt(true)
      }
    } catch (e) {
      setCopiedPrompt(false)
    }
  }

  function coerceValue(key, val) { if (val === null || val === undefined || val === '') return null; if (typeof val === 'number' && key !== 's1') return val; const sval = String(val).trim(); if (key === 's2') { const d = new Date(sval); if (!isNaN(d)) return d.toISOString(); return sval } if (key === 's1') { return sval } const sNumeric = new Set(['s3', 's5', 's6', 's7', 's8', 's9', 's13']); if (sNumeric.has(key) || key.startsWith('c') || key.startsWith('l')) { const n = Number(sval.replace(/[^0-9.\-]/g, '')); return isNaN(n) ? null : n } return sval }

  async function uploadFile() {
    if (!file) return;
    const fd = new FormData(); fd.append('batch', file, file.name);
    try {
      const headers = {};
      const res = await authFetch('http://localhost:8000/upload/headers', { method: 'POST', body: fd, headers });
      const data = await res.json(); if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
      setHeaders(data.headers);
      setFullPreview(data.full_preview || []);
      const initialPreview = (data.preview && data.preview.length) ? data.preview : (data.full_preview || []);
      setPreview(initialPreview);
      setS1Column(data.s1_column || null);

      const m = {};
      data.headers.forEach(h => {
        const suggested = data.suggestions && data.suggestions[h] ? data.suggestions[h] : '';
        if (suggested) { m[h] = suggested; return }
        const found = (collectionCodes || []).find(c => {
          if (!c || !c.code) return false
          const code = String(c.code || '').toLowerCase();
          const hh = String(h || '').toLowerCase();
          return code === hh || hh.includes(code) || code.includes(hh)
        })
        if (found) m[h] = found.column_name
        else m[h] = ''
      })

      setMapping(m);
      recomputeMappedPreview(m, initialPreview);
      setStep(2)
    } catch (e) { showPrompt('error', 'Upload failed: ' + (e && e.message ? e.message : String(e))) }
  }

  function recomputeMappedPreview(mappingToUse = mapping, rowsSource = null) {
    const rowsSrc = rowsSource || preview || fullPreview || [];
    const rows = (rowsSrc || []).map((r, idx) => {
      const out = { collection_entry_method: 'import' };
      if (selectedChurch) out.church = selectedChurch;
      if (selectedDate) out.s2 = new Date(selectedDate).toISOString();
      if (uploaderName) out.source = uploaderName;
      Object.keys(r).forEach(h => {
        const mapped = mappingToUse[h];
        if (mapped) { out[mapped] = coerceValue(mapped, r[h]) }
      })
      try {
        const s2val = out.s2; let s2dt = null;
        if (typeof s2val === 'string') { const d = new Date(s2val); if (!isNaN(d)) s2dt = d }
        else if (s2val instanceof Date) s2dt = s2val;
        const s3val = out.s3; const s3int = s3val != null ? Number(s3val) : null;
        const church_id = out.church || selectedChurch || null;
        if (s2dt && s3int != null && !out.s1) {
          out.s1 = encodeS1({ date: s2dt, church: church_id, s3: s3int })
        }
        if ((out.s3 === null || out.s3 === undefined) && out.s1) {
          const decoded = decodeS1(out.s1)
          if (decoded.s3 != null) out.s3 = decoded.s3
          if (out.church == null && decoded.church != null) out.church = decoded.church
        }
        if (out.s3 === null || out.s3 === undefined || out.s3 === '') {
          out.s3 = idx + 1;
        }
      } catch (e) { }
      return out
    })
    setMappedPreview(rows); setValidationErrors([]); return rows
  }

  function preValidateRows(rows) {
    const msgs = [];
    const rs = rows && rows.length ? rows : mappedPreview;
    rs.forEach((r, idx) => {
      const rowNum = idx + 1;
      if (!r.s2) { msgs.push(`Row ${rowNum}: s2 (date) is missing. Set Date in Step 1 or map a column to s2.`) }
      if (!r.church) { msgs.push(`Row ${rowNum}: church is missing. Select Church in Step 1 or map a column to church.`) }
      const s3val = r.s3;
      const s1val = r.s1;
      if ((s3val === null || s3val === undefined || s3val === '') && !(s1val && String(s1val).replace(/[^0-9]/g, '').length >= 14)) {
        msgs.push(`Row ${rowNum}: s3 (serial) is missing — map your serial column to s3 or ensure s1 contains the combined serial.`)
      }
      if (!r.s4) { msgs.push(`Row ${rowNum}: s4 (name) is missing. Map the name column to s4.`) }
    })
    return msgs;
  }

  function formatValidationErrors(errs) {
    if (!errs || !errs.length) return [];
    const out = [];
    errs.forEach(e => {
      if (typeof e === 'string') {
        out.push(e);
        return;
      }
      const idx = e.index != null ? e.index : '?';
      const items = e.errors || [];
      items.forEach(it => {
        let loc = it.loc;
        if (Array.isArray(loc)) loc = loc.join('.');
        const msg = it.msg || (it.message || JSON.stringify(it));
        out.push(`Row ${Number(idx) + 1}: ${loc} — ${msg}`);
      })
    })
    return out;
  }

  async function validateRows(rows) { try { const headers = { 'Content-Type': 'application/json' }; const res = await authFetch('http://localhost:8000/members_collections/validate', { method: 'POST', headers, body: JSON.stringify(rows) }); const data = await res.json(); if (!res.ok) { if (res.status === 422 && data && data.detail && data.detail.validation_errors) { if (data.detail.rows) setMappedPreview(data.detail.rows); return data.detail.validation_errors || [] } throw new Error(data.detail || JSON.stringify(data)) } if (data.rows) setMappedPreview(data.rows); return data.validation_errors || [] } catch (err) { return [{ error: err.message }] } }

  async function submitMapped() { const rows = mappedPreview.length ? mappedPreview : recomputeMappedPreview(); const val = await validateRows(rows); if (val && val.length) { setValidationErrors(val); setStep(4); showPrompt('warning', 'Validation errors present. Open Step 4 to review details.'); return } try { const headers = { 'Content-Type': 'application/json' }; const res = await authFetch('http://localhost:8000/members_collections/bulk', { method: 'POST', headers, body: JSON.stringify(rows) }); const data = await res.json(); if (!res.ok) throw new Error(data.detail || JSON.stringify(data)); showPrompt('success', `Inserted ${data.inserted} rows`); setStep(5) } catch (e) { showPrompt('error', 'Submit failed: ' + (e && e.message ? e.message : String(e))) } }

  return (
    <div>
      <Card title="Quick Entry" outline variant="primary">
        <div className='flex gap8' style={{ justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap' }}>
          <div>
            <div className='small text-muted'>Enter one or more rows manually.</div>
          </div>
          <button type='button' className='btn btn-primary btn-sm' onClick={() => setShowQuickForm(true)}>Open form</button>
        </div>
      </Card>

        {showQuickForm && (
          <Modal title='Collection Form' onClose={() => setShowQuickForm(false)} maxWidth={1180}>
          <div className='quick-form-panel'>
            {!canManageCollections && (
              <div className='mb8'>
                <Alert level='warning'>Your account can access Collections but cannot submit entries. Ask an admin to grant Collections management rights.</Alert>
              </div>
            )}
            {quickRows.map((row, idx) => (
              <div key={row.id} className='subpanel mb8'>
                <div className='flex gap8 mb8' style={{ justifyContent: 'space-between', alignItems: 'center' }}>
                  <strong>Collection row {idx + 1}</strong>
                  <button type='button' className='btn btn-sm btn-danger' onClick={() => removeQuickRow(row.id)} disabled={quickRows.length === 1 || quickSubmitting}>Remove</button>
                </div>
                <div className='form-grid mb8'>
                  <div>
                    <label style={{ display: 'block', marginBottom: 4 }}>Member (optional)</label>
                    <select
                      value={row.selected_member_id || ''}
                      onChange={e => {
                        const selected = String(e.target.value || '')
                        const found = (membersLocal || []).find(m => String(m?.MEMBER_ID ?? m?.member_id ?? m?.id ?? '') === selected)
                        updateQuickRow(row.id, {
                          selected_member_id: selected,
                          name: found ? String(found?.MEMBER_NAME || '') : row.name,
                          phone: found ? String(found?.PHONE || '') : row.phone,
                        })
                      }}
                      disabled={quickSubmitting}
                    >
                      <option value=''>-- select member --</option>
                      {(membersLocal || []).map(m => (
                        <option key={String(m?.id || m?.MEMBER_ID || m?.member_id || m?.MEMBER_NAME || '')} value={String(m?.MEMBER_ID ?? m?.member_id ?? m?.id ?? '')}>
                          {String(m?.MEMBER_NAME || '')} {m?.PHONE ? `| ${m.PHONE}` : ''} {(m?.MEMBER_ID ?? m?.member_id) ? `| ID:${m?.MEMBER_ID ?? m?.member_id}` : ''}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label style={{ display: 'block', marginBottom: 4 }}>Name</label>
                    <input
                      type='text'
                      list='quick-form-members-list'
                      placeholder='Type or pick from members list'
                      value={row.name}
                      onChange={e => updateQuickRow(row.id, { name: e.target.value })}
                      disabled={quickSubmitting}
                    />
                    <datalist id='quick-form-members-list'>
                      {(membersLocal || []).map(m => (
                        <option key={m.id} value={String(m.MEMBER_NAME || '')} />
                      ))}
                    </datalist>
                  </div>
                  <div>
                    <label style={{ display: 'block', marginBottom: 4 }}>Phone number (optional)</label>
                    <input
                      type='text'
                      placeholder='Phone number'
                      value={row.phone || ''}
                      onChange={e => updateQuickRow(row.id, { phone: e.target.value })}
                      disabled={quickSubmitting}
                    />
                  </div>
                  <div>
                    <label style={{ display: 'block', marginBottom: 4 }}>Date of collection</label>
                    <input type='date' value={row.collection_date} onChange={e => updateQuickRow(row.id, { collection_date: e.target.value })} disabled={quickSubmitting} />
                  </div>
                  <div>
                    <label style={{ display: 'block', marginBottom: 4 }}>Total collection amount</label>
                    <input type='number' min='0' step='0.01' placeholder='0.00' value={row.total_amount} onChange={e => updateQuickRow(row.id, { total_amount: e.target.value })} disabled={quickSubmitting} />
                  </div>
                </div>
                <div className='subpanel'>
                  <label style={{ display: 'block', marginBottom: 6 }}>Collection details</label>
                  <div className='form-grid' style={{ alignItems: 'end' }}>
                    <div>
                      <label className='small' style={{ display: 'block', marginBottom: 4 }}>Collection item</label>
                      <select
                        value={row.selected_detail || ''}
                        onChange={e => updateQuickRow(row.id, { selected_detail: e.target.value })}
                        disabled={quickSubmitting || !quickDetailOptions.length}
                      >
                        <option value=''>-- select collection item --</option>
                        {quickDetailOptions.map(opt => (
                          <option key={opt.column_name} value={opt.column_name}>
                            {opt.custom_collection_name || opt.code || opt.column_name}
                          </option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className='small' style={{ display: 'block', marginBottom: 4 }}>Item amount</label>
                      <input type='number' min='0' step='0.01' placeholder='0.00' value={row.selected_detail_amount || ''} onChange={e => updateQuickRow(row.id, { selected_detail_amount: e.target.value })} disabled={quickSubmitting} />
                    </div>
                    <div>
                      <button type='button' className='btn btn-sm' onClick={() => addQuickDetailEntry(row.id)} disabled={quickSubmitting || !quickDetailOptions.length}>Add item</button>
                    </div>
                  </div>
                  <div className='flex gap8 mt8' style={{ justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap' }}>
                    <strong className='small'>Added items</strong>
                    <span className='small'>Details total: {quickRowDetailsTotal(row).toFixed(2)} | Declared total: {Number(row.total_amount || 0).toFixed(2)}</span>
                  </div>
                  <div className='small' style={{ marginTop: 6, fontWeight: 700, color: Math.abs(quickRowDetailsTotal(row) - Number(row.total_amount || 0)) <= 0.009 ? 'var(--success)' : 'var(--warning)' }}>
                    Audit: Difference = {(quickRowDetailsTotal(row) - Number(row.total_amount || 0)).toFixed(2)}
                  </div>
                  {(!row.detail_entries || !row.detail_entries.length) ? (
                    <div className='hint'>No collection detail items added yet.</div>
                  ) : (
                    <div className='table-wrap mt8'>
                      <table>
                        <thead>
                          <tr>
                            <th>Item</th>
                            <th className='text-right'>Amount</th>
                            <th>Action</th>
                          </tr>
                        </thead>
                        <tbody>
                          {row.detail_entries.map(entry => (
                            <tr key={entry.code}>
                              <td>{localLabelForColumn(entry.code)}</td>
                              <td className='text-right'>{Number(entry.amount || 0).toFixed(2)}</td>
                              <td>
                                <button type='button' className='btn btn-sm btn-danger' onClick={() => removeQuickDetailEntry(row.id, entry.code)} disabled={quickSubmitting}>Remove</button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>
            ))}

            <div className='flex gap8' style={{ flexWrap: 'wrap' }}>
              <button type='button' className='btn' onClick={addQuickRow} disabled={quickSubmitting}>Add another row</button>
              <button type='button' className='btn btn-primary' onClick={submitQuickRows} disabled={quickSubmitting || !canManageCollections}>{quickSubmitting ? 'Submitting...' : 'Submit rows'}</button>
              <button type='button' className='btn btn-ghost' onClick={() => setShowQuickForm(false)} disabled={quickSubmitting}>Close form</button>
            </div>
          </div>
          </Modal>
        )}

      {promptState.open && (
        <div className='mb8'>
          <Alert level={promptState.level === 'error' ? 'error' : promptState.level === 'warning' ? 'warning' : promptState.level === 'success' ? 'success' : 'info'}>
            <div className='flex gap8' style={{ justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
              <strong>{promptState.level === 'error' ? 'Error' : promptState.level === 'warning' ? 'Warning' : promptState.level === 'success' ? 'Success' : 'Info'}</strong>
              <div className='flex gap8'>
                <button type='button' className='btn btn-sm' onClick={copyPromptMessage}>Copy message</button>
                <button type='button' className='btn btn-sm btn-ghost' onClick={() => setPromptState(prev => ({ ...prev, open: false }))}>Close</button>
              </div>
            </div>
            <div style={{ whiteSpace: 'pre-wrap' }}>{promptState.message}</div>
            {copiedPrompt && <div className='mt8' style={{ color: 'var(--success)', fontWeight: 700 }}>Message copied to clipboard.</div>}
          </Alert>
        </div>
      )}

      <Card title={`Step ${step} of ${totalSteps}: Collection Batch Upload`} outline variant="info" className="mt-3">
        {step === 1 && (
          <div>
            <div>
              <input type='file' onChange={e => setFile(e.target.files[0])} />
            </div>
            <div className='toolbar mt-3'>
              <label>Date:</label>
              <input type='date' value={selectedDate} onChange={e => { setSelectedDate(e.target.value) }} />
              <button type='button' className='btn btn-sm btn-ghost' onClick={saveSelectedCollectionDate}>Save Selection</button>
              <label>Church:</label>
              <select value={selectedChurch || ''} onChange={e => {
                const nextChurch = e.target.value
                if (!canAccessChurch(nextChurch)) { handleRestrictedChurch('collection upload'); return }
                setSelectedChurch(nextChurch)
              }}>
                <option value=''>-- select --</option>
                {visibleChurches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
              <label>Uploader name:</label>
              <input value={uploaderName} readOnly disabled />
            </div>
            <div className='mt-3'>
              <button type='button' className='btn btn-primary' onClick={uploadFile} disabled={!file}>Upload &amp; Inspect</button>
            </div>
          </div>
        )}

        {step === 2 && (
          <div>
            <div className='toolbar'>
              <label>Date:</label>
              <input type='date' value={selectedDate} onChange={e => { setSelectedDate(e.target.value); recomputeMappedPreview() }} />
              <button type='button' className='btn btn-sm btn-ghost' onClick={saveSelectedCollectionDate}>Save Selection</button>
              <label>Church:</label>
              <select value={selectedChurch || ''} onChange={e => {
                const nextChurch = e.target.value
                if (!canAccessChurch(nextChurch)) { handleRestrictedChurch('collection upload'); return }
                setSelectedChurch(nextChurch); recomputeMappedPreview()
              }}>
                <option value=''>-- select --</option>
                {visibleChurches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
              <label>Uploader name:</label>
              <input value={uploaderName} readOnly disabled />
            </div>
            <div className='table-wrap mt-3'>
              <table>
                <thead><tr><th>Header</th><th>Map to</th></tr></thead>
                <tbody>{headers.map(h => (
                  <tr key={h}><td>{h}</td><td>
                    <select value={mapping[h] || ''} onChange={e => { setMapping(prev => ({ ...prev, [h]: e.target.value })); recomputeMappedPreview({ ...mapping, [h]: e.target.value }) }}>
                      <option value=''>-- none --</option>
                      {(collectionCodes || []).map(c => <option key={c.column_name} value={c.column_name}>{c.column_name} — {c.code}</option>)}
                    </select>
                  </td></tr>
                ))}</tbody>
              </table>
            </div>
            <div className='flex gap8 mt-3' style={{ alignItems: 'center' }}>
              <button type='button' className='btn btn-ghost btn-sm' onClick={() => setStep(1)}>Back</button>
              <button type='button' className='btn btn-primary btn-sm' onClick={() => setStep(3)}>Next — Preview</button>
              <button type='button' className='btn btn-ghost btn-sm' onClick={async () => {
                const arr = Object.keys(mapping).map(h => ({ header_name: h, mapped_column: mapping[h] }));
                try {
                  const res = await authFetch('http://localhost:8000/header_mappings', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(arr) });
                  const data = await res.json(); if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
                  showPrompt('success', 'Mappings saved');
                } catch (e) { showPrompt('error', 'Save mapping failed: ' + (e && e.message ? e.message : String(e))) }
              }}>Save mapping</button>
            </div>
          </div>
        )}

        {step === 3 && (
          <div>
            <h4>Mapped preview ({mappedPreview.length} rows)</h4>
            <div className='stat-strip subpanel mb8 flex gap8' style={{ flexWrap: 'wrap', alignItems: 'stretch' }}>
              <div className='kv' style={{ minWidth: 180 }}>
                <div className='small' style={{ textTransform: 'uppercase', fontWeight: 800 }}>Church</div>
                <div style={{ fontWeight: 700, fontSize: 16 }}>{currentChurchNameLocal()}</div>
              </div>
              <div className='kv' style={{ minWidth: 180 }}>
                <div className='small' style={{ textTransform: 'uppercase', fontWeight: 800 }}>Date</div>
                <div style={{ fontWeight: 700, fontSize: 16 }}>{formatPreviewDate(selectedDate)}</div>
              </div>
              <div className='kv' style={{ minWidth: 220 }}>
                <div className='small' style={{ textTransform: 'uppercase', fontWeight: 800 }}>Source</div>
                <div style={{ fontWeight: 700, fontSize: 16 }}>{uploaderName || '-'}</div>
              </div>
            </div>
            <div className='table-wrap' style={{ maxHeight: 420 }}>
              <table style={mappedPreviewMode === 'scrollable' ? { width: 'max-content', minWidth: '100%' } : { width: '100%', minWidth: 'max-content' }}>
                <thead><tr>{mappedPreview[0] ? mappedPreviewColumnsLocal().map(k => <th key={k} style={{ whiteSpace: 'nowrap', minWidth: mappedPreviewMode === 'scrollable' ? 180 : undefined }}>{localLabelForColumn(k)}</th>) : <th>No rows</th>}</tr></thead>
                <tbody>{mappedPreview.map((r, idx) => (
                  <tr key={idx}>{mappedPreviewColumnsLocal().map(k => <td key={k}><input value={r[k] || ''} onChange={e => { const v = e.target.value; setMappedPreview(prev => { const nxt = [...prev]; nxt[idx] = { ...nxt[idx], [k]: v }; return nxt }) }} style={{ minWidth: mappedPreviewMode === 'scrollable' ? 180 : 160, width: mappedPreviewMode === 'scrollable' ? 180 : undefined }} /></td>)}</tr>
                ))}</tbody>
              </table>
            </div>
            <div className='flex gap8 mt-3'>
              <button type='button' className='btn btn-ghost btn-sm' onClick={() => setStep(2)}>Back</button>
              <button type='button' className='btn btn-primary btn-sm' onClick={async () => { await submitMapped() }}>Validate &amp; Submit</button>
            </div>
          </div>
        )}

        {step === 4 && (
          <div>
            <h4>Fix validation errors</h4>
            {validationErrors && validationErrors.length ? (
              <ul>
                {formatValidationErrors(validationErrors).map((m, i) => <li key={i}>{m}</li>)}
              </ul>
            ) : (
              <div>No validation errors</div>
            )}
            <button type='button' className='btn btn-ghost btn-sm' onClick={() => setStep(3)}>Back to preview</button>
          </div>
        )}

        {step === 5 && (
          <div>
            <h4>Done</h4>
            <button type='button' className='btn btn-primary btn-sm' onClick={() => { setStep(1); setMappedPreview([]); setPreview([]); setFullPreview([]) }}>Start another</button>
          </div>
        )}
      </Card>
    </div>
  )
}
