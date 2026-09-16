import React from 'react'
import Alert from './Alert'
import Card from './Card'

export default function CollectionCodesPanel({
  hasRoleRight,
  localCollectionCodes,
  currentUserChurchId,
  showCollectionCodesPanel, setShowCollectionCodesPanel,
  collectionCodesMaxRows, setCollectionCodesMaxRows,
  newCodeColumn, setNewCodeColumn,
  newCodeLabel, setNewCodeLabel,
  newCodeCustomName, setNewCodeCustomName,
  newCodeScope, setNewCodeScope,
  newCodeSplitPct, setNewCodeSplitPct,
  createLocalCode,
  editingCodeId, setEditingCodeId,
  editCodeForm, setEditCodeForm,
  updateLocalCode, deleteLocalCode,
  mainGridWrapStyle, mainGridTableStyle,
}){
  const canCollectionCodeEdit = hasRoleRight('can_manage_collections', true) || hasRoleRight('can_view_collection_codes', true)
  const localChurchCollectionCodes = (localCollectionCodes || []).filter(c=> Number(c.church)===Number(currentUserChurchId))

  return (
    <Card title="Collection Codes" outline variant="warning" className="mt-3">
      <p className='small'>
        {canCollectionCodeEdit
          ? 'View existing church collection codes, edit them, or add more for your church.'
          : 'View existing church collection codes.'}
      </p>
      <p className='small'>These codes apply only to your church. Global codes are shared across all churches.</p>
      {!currentUserChurchId && (
        <div className='mb8'>
          <Alert level='warning'>A church assignment is required to manage collection codes.</Alert>
        </div>
      )}

      <div className='mb8'>
        <button type='button' className='link-button' onClick={()=>setShowCollectionCodesPanel(v=>!v)}>
          {showCollectionCodesPanel ? 'Hide collection codes' : 'View collection codes'}
        </button>
      </div>

      {showCollectionCodesPanel && (
        <div className='toolbar'>
          <label>Show</label>
          <select value={collectionCodesMaxRows} onChange={e=>setCollectionCodesMaxRows(Number(e.target.value))}>
            {[10,30,50,100,200].map(n=> <option key={n} value={n}>{n}</option>)}
          </select>
          <span className='small'>Showing {Math.min(localCollectionCodes.length, collectionCodesMaxRows)} of {localCollectionCodes.length}</span>
        </div>
      )}

      {canCollectionCodeEdit && (
        <div className='subpanel mb8'>
          <div className='card-title'>Add New Code</div>
          <div className='form-row'>
            <input type='text' value={newCodeColumn} onChange={e=>setNewCodeColumn(e.target.value)} placeholder='Code key (e.g. c21)' disabled={!currentUserChurchId} />
            <input type='text' value={newCodeLabel} onChange={e=>setNewCodeLabel(e.target.value)} placeholder='Collection label' disabled={!currentUserChurchId} />
            <input type='text' value={newCodeCustomName} onChange={e=>setNewCodeCustomName(e.target.value)} placeholder='Custom collection name (optional)' disabled={!currentUserChurchId} />
            <select value={newCodeScope} onChange={e=>setNewCodeScope(e.target.value)} disabled={!currentUserChurchId}>
              <option value=''>Scope: Local (default)</option>
              <option value='local'>Local</option>
              <option value='conference'>Conference</option>
              <option value='split'>Split</option>
            </select>
            {newCodeScope === 'split' && (
              <input type='number' min='0' max='100' step='0.01' value={newCodeSplitPct} onChange={e=>setNewCodeSplitPct(e.target.value)} placeholder='Conference % (e.g. 58)' style={{width:160}} disabled={!currentUserChurchId} />
            )}
            <button type='button' className='btn btn-primary' onClick={createLocalCode} disabled={!currentUserChurchId}>Add Code</button>
          </div>
        </div>
      )}

      {showCollectionCodesPanel && (
        <div>
          <div className='card-title'>Church Collection Codes Grid (Church ID: {currentUserChurchId || '-'})</div>
          {localChurchCollectionCodes.length === 0 ? (
            <div className='small'>No collection codes found for this church.</div>
          ) : (
            <div className='table-wrap' style={mainGridWrapStyle(420)}>
              <table style={mainGridTableStyle(true)}>
                <thead><tr><th>ID</th><th>Church ID</th><th>Code Key</th><th>Label</th><th>Custom Name</th><th>Scope</th>{canCollectionCodeEdit && <th>Actions</th>}</tr></thead>
                <tbody>
                  {localChurchCollectionCodes.slice(0, collectionCodesMaxRows).map(code=> (
                    <tr key={code.id}>
                      <td>{code.id}</td>
                      <td>{code.church}</td>
                      <td>{code.column_name}</td>
                      <td>{code.code}</td>
                      <td>{code.custom_collection_name || '-'}</td>
                      <td>
                        {String(code.scope || 'local')}
                        {code.scope === 'split' && code.conference_split_pct != null ? ` (${code.conference_split_pct}% conf)` : ''}
                      </td>
                      {canCollectionCodeEdit && (
                        <td className='cell-actions'>
                          {editingCodeId !== code.id ? (
                            <>
                              <button type='button' className='btn btn-sm' style={{marginRight:8}} onClick={()=>{ setEditingCodeId(code.id); setEditCodeForm({column_name:code.column_name, code:code.code, custom_collection_name:(code.custom_collection_name || ''), scope:(code.scope || ''), conference_split_pct:(code.conference_split_pct != null ? String(code.conference_split_pct) : '')}); }}>Edit</button>
                              {hasRoleRight('can_manage_collections', true) && <button type='button' className='btn btn-sm btn-danger' onClick={()=>deleteLocalCode(code.id)}>Delete</button>}
                            </>
                          ) : (
                            <div className='form-row'>
                              <input type='text' value={editCodeForm.column_name} onChange={e=>setEditCodeForm({...editCodeForm,column_name:e.target.value})} />
                              <input type='text' value={editCodeForm.code} onChange={e=>setEditCodeForm({...editCodeForm,code:e.target.value})} />
                              <input type='text' value={editCodeForm.custom_collection_name} onChange={e=>setEditCodeForm({...editCodeForm,custom_collection_name:e.target.value})} />
                              <select value={editCodeForm.scope || ''} onChange={e=>setEditCodeForm({...editCodeForm,scope:e.target.value})}>
                                <option value=''>Local (default)</option>
                                <option value='local'>Local</option>
                                <option value='conference'>Conference</option>
                                <option value='split'>Split</option>
                              </select>
                              {(editCodeForm.scope === 'split') && (
                                <input type='number' min='0' max='100' step='0.01' value={editCodeForm.conference_split_pct || ''} onChange={e=>setEditCodeForm({...editCodeForm,conference_split_pct:e.target.value})} placeholder='Conference %' style={{width:120}} />
                              )}
                              <button type='button' className='btn btn-sm btn-primary' onClick={updateLocalCode}>Save</button>
                              <button type='button' className='btn btn-sm btn-ghost' onClick={()=>{ setEditingCodeId(null); setEditCodeForm({column_name:'',code:'',custom_collection_name:'',scope:'',conference_split_pct:''}); }}>Cancel</button>
                            </div>
                          )}
                        </td>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </Card>
  )
}
