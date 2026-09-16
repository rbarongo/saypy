import React, { useMemo, useState } from 'react'

/**
 * Generic table with search, sortable columns, and pagination — reused across
 * every Setup sub-page instead of hand-rolling each one.
 *
 * Two modes:
 *  - Client-side (default): pass the full `rows` array; search/sort/paging
 *    all happen in-memory. Right for small reference-data lists.
 *  - Server-side (`serverMode`): the parent owns the data fetch. Pass
 *    `total`, `page`, `onPageChange`, `sortKey`, `sortDir`, `onSortChange`,
 *    `searchValue`, `onSearchChange`; this component just renders controls
 *    and reports interactions back up. Used by Members, which can be large.
 *
 * `columns`: [{ key, label, sortable=true, searchable=true, render(row), sortValue(row) }]
 * `actions(row)`: optional per-row action buttons.
 */
export default function DataTable({
  columns, rows, getRowId = r => r.id, actions, emptyMessage = 'No records found.',
  pageSize = 10, loading = false, toolbarExtra,
  serverMode = false, total, page, onPageChange, sortKey, sortDir, onSortChange, searchValue, onSearchChange,
}){
  const [localSearch, setLocalSearch] = useState('')
  const [localSort, setLocalSort] = useState({ key: null, dir: 'asc' })
  const [localPage, setLocalPage] = useState(1)

  const search = serverMode ? (searchValue || '') : localSearch
  const effSortKey = serverMode ? sortKey : localSort.key
  const effSortDir = serverMode ? sortDir : localSort.dir
  const effPage = serverMode ? page : localPage

  function setSearch(value){
    if(serverMode) onSearchChange(value)
    else { setLocalSearch(value); setLocalPage(1) }
  }

  function handleSort(col){
    if(col.sortable === false) return
    if(serverMode){
      const dir = sortKey === col.key && sortDir === 'asc' ? 'desc' : 'asc'
      onSortChange(col.key, dir)
    }else{
      setLocalSort(prev => ({ key: col.key, dir: prev.key === col.key && prev.dir === 'asc' ? 'desc' : 'asc' }))
    }
  }

  const displayRows = useMemo(() => {
    if(serverMode) return rows
    let out = rows
    if(localSearch.trim()){
      const q = localSearch.trim().toLowerCase()
      out = out.filter(row => columns.some(c => {
        if(c.searchable === false) return false
        const value = c.render ? c.render(row) : row[c.key]
        return String(value ?? '').toLowerCase().includes(q)
      }))
    }
    if(localSort.key){
      const col = columns.find(c => c.key === localSort.key)
      out = [...out].sort((a, b) => {
        const av = col?.sortValue ? col.sortValue(a) : a[localSort.key]
        const bv = col?.sortValue ? col.sortValue(b) : b[localSort.key]
        if(av == null && bv == null) return 0
        if(av == null) return 1
        if(bv == null) return -1
        if(av < bv) return localSort.dir === 'asc' ? -1 : 1
        if(av > bv) return localSort.dir === 'asc' ? 1 : -1
        return 0
      })
    }
    return out
  }, [serverMode, rows, localSearch, localSort, columns])

  const effTotal = serverMode ? (total || 0) : displayRows.length
  const totalPages = Math.max(1, Math.ceil(effTotal / pageSize))

  const pageRows = serverMode
    ? displayRows
    : displayRows.slice((localPage - 1) * pageSize, (localPage - 1) * pageSize + pageSize)

  function changePage(next){
    const clamped = Math.max(1, Math.min(totalPages, next))
    if(serverMode) onPageChange(clamped)
    else setLocalPage(clamped)
  }

  const colSpan = columns.length + (actions ? 1 : 0)

  return (
    <div className='data-table'>
      <div className='data-table-toolbar'>
        <input className='form-control data-table-search' placeholder='Search records...' value={search} onChange={e => setSearch(e.target.value)} />
        <div className='flex-grow-1' />
        {toolbarExtra}
      </div>

      <div className='table-responsive data-table-frame'>
        <table className='table table-hover'>
          <thead>
            <tr>
              {columns.map(col => (
                <th
                  key={col.key}
                  className={col.sortable !== false ? 'th-sortable' : undefined}
                  onClick={col.sortable !== false ? () => handleSort(col) : undefined}
                >
                  {col.label}
                  {col.sortable !== false && effSortKey === col.key && (
                    <span className='th-sort-arrow'>{effSortDir === 'asc' ? ' ▲' : ' ▼'}</span>
                  )}
                </th>
              ))}
              {actions && <th>Actions</th>}
            </tr>
          </thead>
          <tbody>
            {loading && <tr><td colSpan={colSpan} className='data-table-state'>Loading records...</td></tr>}
            {!loading && pageRows.length === 0 && <tr><td colSpan={colSpan} className='data-table-state'>{emptyMessage}</td></tr>}
            {!loading && pageRows.map(row => (
              <tr key={getRowId(row)}>
                {columns.map(col => <td key={col.key}>{col.render ? col.render(row) : row[col.key]}</td>)}
                {actions && <td className='text-nowrap'>{actions(row)}</td>}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className='data-table-footer'>
        <span className='data-table-count'>{effTotal} record{effTotal === 1 ? '' : 's'} - page {effPage} of {totalPages}</span>
        <div className='flex-grow-1' />
        <button type='button' className='btn btn-sm btn-outline-secondary' disabled={effPage <= 1} onClick={() => changePage(effPage - 1)}>Previous</button>
        <button type='button' className='btn btn-sm btn-outline-secondary' disabled={effPage >= totalPages} onClick={() => changePage(effPage + 1)}>Next</button>
      </div>
    </div>
  )
}
