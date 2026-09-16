import React from 'react'
import CollectionsUpload from './CollectionsUpload'
import CollectionCodesPanel from './CollectionCodesPanel'
import PageHeader from './members-core/PageHeader'

export default function CollectionsPage({
  token, authFetch, collectionCodes, churches, fetchCodes, user, labelForColumn,
  currentUserChurchId, denyRestrictedChurchAccess, uploadGridMode, mappedPreviewMode,
  hasRoleRight, membersLocal,
  localCollectionCodes,
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
  const usableCodes = (localCollectionCodes || []).filter(c => String(c?.code || '').trim().toUpperCase() !== 'UNUSED').length
  const scopedCodes = (localCollectionCodes || []).filter(c => c?.church != null).length

  return (
    <div>
      <PageHeader
        title='Collections'
        subtitle='Upload batches, enter quick collections, and manage collection codes'
        stats={[
          { label: 'Codes', value: (localCollectionCodes || []).length },
          { label: 'Usable', value: usableCodes },
          { label: 'Scoped', value: scopedCodes },
          { label: 'Church scope', value: currentUserChurchId || '-' },
        ]}
      />
      <CollectionsUpload
        token={token}
        authFetch={authFetch}
        collectionCodes={collectionCodes}
        churches={churches}
        fetchCodes={fetchCodes}
        user={user}
        labelForColumn={labelForColumn}
        scopedChurchId={currentUserChurchId}
        onRestrictedChurchAttempt={denyRestrictedChurchAccess}
        uploadGridMode={uploadGridMode}
        mappedPreviewMode={mappedPreviewMode}
        canManageCollections={hasRoleRight('can_manage_collections', true)}
        membersLocal={membersLocal}
      />

      <CollectionCodesPanel
        hasRoleRight={hasRoleRight}
        localCollectionCodes={localCollectionCodes}
        currentUserChurchId={currentUserChurchId}
        showCollectionCodesPanel={showCollectionCodesPanel} setShowCollectionCodesPanel={setShowCollectionCodesPanel}
        collectionCodesMaxRows={collectionCodesMaxRows} setCollectionCodesMaxRows={setCollectionCodesMaxRows}
        newCodeColumn={newCodeColumn} setNewCodeColumn={setNewCodeColumn}
        newCodeLabel={newCodeLabel} setNewCodeLabel={setNewCodeLabel}
        newCodeCustomName={newCodeCustomName} setNewCodeCustomName={setNewCodeCustomName}
        newCodeScope={newCodeScope} setNewCodeScope={setNewCodeScope}
        newCodeSplitPct={newCodeSplitPct} setNewCodeSplitPct={setNewCodeSplitPct}
        createLocalCode={createLocalCode}
        editingCodeId={editingCodeId} setEditingCodeId={setEditingCodeId}
        editCodeForm={editCodeForm} setEditCodeForm={setEditCodeForm}
        updateLocalCode={updateLocalCode} deleteLocalCode={deleteLocalCode}
        mainGridWrapStyle={mainGridWrapStyle} mainGridTableStyle={mainGridTableStyle}
      />
    </div>
  )
}
