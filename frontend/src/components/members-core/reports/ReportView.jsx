import React, { useEffect, useState } from 'react'
import { Download, FileText } from 'react-feather'
import Alert from '../../Alert'
import PageHeader from '../PageHeader'
import DataTable from '../DataTable'
import useReferenceData from '../useReferenceData'
import { membersApi } from '../api'
import ReportFilters from './ReportFilters'
import { exportReportExcel, exportReportPdf } from './reportExport'

// Filter tokens that need a church-scoped lookup fetch, mapped to the
// membersApi.* lookup + the scopedLookups key it's stored under.
const SCOPED_LOOKUP_TOKENS = {
  sabbathSchoolGroup: 'sabbathSchoolGroups',
  organisation: 'organisations',
  churchRole: 'churchRoles',
}

function humanizeKey(key) {
  return String(key).replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
}

/** Generic viewer used by all 7 reports — see reportsConfig.js. ReportsHub
 * remounts this (via a `key={report.key}` prop) on every report switch, so
 * all state here starts fresh without needing its own reset effect. */
export default function ReportView({ report }) {
  const referenceData = useReferenceData()
  const [filters, setFilters] = useState({})
  const [scopedLookups, setScopedLookups] = useState({ organisations: [], churchRoles: [], sabbathSchoolGroups: [] })
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [exporting, setExporting] = useState('')

  useEffect(() => {
    const needed = report.filters.filter(t => SCOPED_LOOKUP_TOKENS[t])
    if (needed.length === 0) return
    let cancelled = false
    Promise.all(needed.map(t => membersApi[SCOPED_LOOKUP_TOKENS[t]].list({ church_id: filters.church_id || undefined })))
      .then(results => {
        if (cancelled) return
        const patch = {}
        needed.forEach((t, i) => { patch[SCOPED_LOOKUP_TOKENS[t]] = results[i] })
        setScopedLookups(prev => ({ ...prev, ...patch }))
      })
      .catch(() => { /* lookups are supplementary — a report can still run without them */ })
    return () => { cancelled = true }
  }, [filters.church_id]) // eslint-disable-line react-hooks/exhaustive-deps

  async function runReport() {
    setLoading(true); setError('')
    try {
      const data = await membersApi.reports[report.apiFn](filters)
      setResult(data)
    } catch (e) {
      setError(e.message || 'Failed to run report')
    } finally {
      setLoading(false)
    }
  }

  // Run once with the default (empty) filters as soon as a report is opened.
  useEffect(() => { runReport() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  async function doExportExcel() {
    setExporting('excel')
    try { await exportReportExcel(report.label, result.columns, result.rows) }
    finally { setExporting('') }
  }

  const columns = (result?.columns || []).map(c => ({
    key: c.key,
    label: c.label,
    render: row => {
      const v = row[c.key]
      const display = (v === null || v === undefined || v === '') ? '—' : String(v)
      return c.numeric ? <span style={{ display: 'block', textAlign: 'right' }}>{display}</span> : display
    },
  }))
  const tableRows = (result?.rows || []).map((r, i) => ({ ...r, _rowKey: i }))

  const summary = result?.summary || {}
  const summaryTiles = Object.entries(summary).filter(([, v]) => v === null || typeof v !== 'object')
  const nestedSummaries = Object.entries(summary).filter(([, v]) => v && typeof v === 'object' && !Array.isArray(v))
  const listSummaries = Object.entries(summary).filter(([, v]) => Array.isArray(v))

  return (
    <div>
      <PageHeader title={report.label} icon={FileText} />
      <p className='small' style={{ marginTop: -8, marginBottom: 14 }}>{report.description}</p>

      <div className='card mb8'>
        <ReportFilters tokens={report.filters} values={filters} onChange={setFilters} referenceData={referenceData} scopedLookups={scopedLookups} />
        <button type='button' className='btn btn-sm btn-primary mt8' disabled={loading} onClick={runReport}>
          {loading ? 'Running…' : 'Run report'}
        </button>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Could not run report'>{error}</Alert></div>}

      {result && (
        <>
          {summaryTiles.length > 0 && (
            <div className='flex gap8 mb8' style={{ flexWrap: 'wrap' }}>
              {summaryTiles.map(([k, v]) => (
                <div key={k} className='field-chip'><strong>{humanizeKey(k)}:</strong>&nbsp;{String(v)}</div>
              ))}
            </div>
          )}

          {nestedSummaries.map(([k, v]) => (
            <div key={k} className='card mb8'>
              <h5 className='card-title' style={{ fontSize: 13 }}>{humanizeKey(k)}</h5>
              <div className='flex gap8' style={{ flexWrap: 'wrap' }}>
                {Object.entries(v).map(([cat, cnt]) => (
                  <span key={cat} className='field-chip'>{cat}: {typeof cnt === 'object' ? JSON.stringify(cnt) : cnt}</span>
                ))}
              </div>
            </div>
          ))}

          {listSummaries.map(([k, arr]) => (
            <div key={k} className='card mb8'>
              <h5 className='card-title' style={{ fontSize: 13 }}>{humanizeKey(k)}</h5>
              <div className='flex gap8' style={{ flexWrap: 'wrap' }}>
                {arr.map((item, i) => (
                  <span key={i} className='field-chip'>{item.name || item.code || String(item)}{item.count !== undefined ? `: ${item.count}` : ''}</span>
                ))}
              </div>
            </div>
          ))}

          <div className='flex gap8 mb8' style={{ justifyContent: 'flex-end' }}>
            <button type='button' className='btn btn-sm btn-ghost' disabled={!result.rows.length || !!exporting} onClick={doExportExcel}>
              <Download size={14} /> Export Excel
            </button>
            <button type='button' className='btn btn-sm btn-ghost' disabled={!result.rows.length || !!exporting} onClick={() => exportReportPdf(report.label, result.columns, result.rows)}>
              <FileText size={14} /> Export PDF
            </button>
          </div>

          <DataTable columns={columns} rows={tableRows} getRowId={r => r._rowKey} emptyMessage='No records match these filters.' pageSize={20} />
        </>
      )}
    </div>
  )
}
