import React from 'react'
import ColumnPicker from './ColumnPicker'
import Card from './Card'
import PageHeader from './members-core/PageHeader'

export default function ReportsPage({
  periodSummaryFrom, setPeriodSummaryFrom, periodSummaryTo, setPeriodSummaryTo,
  fetchPeriodSummary, periodSummaryLoading,
  exportPeriodSummaryPdf, exportPeriodSummaryExcel,
  periodSummaryRows, periodSummaryTotals,
  currentChurchName,
  formatNumber,
  mainGridWrapStyle, mainGridTableStyle,
  reportFrom, setReportFrom, reportTo, setReportTo,
  fetchMembersCollectionReport,
  showReportColumnPicker, setShowReportColumnPicker,
  reportVisibleColumns, getReportPickerCurrentColumns, getReportAllColumns, reportRows, applyReportColumns,
  showCollectionsInReports, setShowCollectionsInReports, fetchMembersCollections,
  reportMaxRows, setReportMaxRows,
  reportShowOnlyDataColumns, setReportShowOnlyDataColumns,
  labelForColumn,
  reportBuilderOptions, selectedReportBuilder, setSelectedReportBuilder,
  buildSelectedReport, exportBuiltReportPdf, exportBuiltReportExcel,
  builtReportType,
  selectedContributorName, setSelectedContributorName, contributorNameOptions,
  reportCellValue,
  aggregateByCollectionCode, aggRows,
  builtReportRows, builtReportColumns, isBuiltReportRightAlignedColumn, isBuiltReportNumericColumn, labelForBuiltReportColumn,
  itemTotalsByDateRows,
  meetingSummaryRows, meetingSummaryTotals,
  membersCollections, membersCollectionsFields, displayCellValue,
  reportCollectionsMaxRows, setReportCollectionsMaxRows,
}){
  const periodTotal = Number(periodSummaryTotals?.total || 0)
  const builtRowsCount = Array.isArray(builtReportRows) ? builtReportRows.length : 0
  const visibleReportRows = Math.min((reportRows || []).length, reportMaxRows)

  return (
    <div>
      <PageHeader
        title='Reports'
        subtitle='Collection summaries, member contribution views, and export-ready reports'
        stats={[
          { label: 'Summary rows', value: (periodSummaryRows || []).length },
          { label: 'Summary total', value: periodTotal.toLocaleString(undefined, { maximumFractionDigits: 2 }) },
          { label: 'Report rows', value: visibleReportRows },
          { label: 'Built rows', value: builtRowsCount },
        ]}
      />

      <Card title="Period Summary Report" outline variant="primary">
        <div className='small mb8'>Aggregated collection summary by item, split into Conference and Local Church amounts.</div>
        <div className='toolbar'>
          <label>From:</label>
          <input className='period-summary-date' type='date' value={periodSummaryFrom} onChange={e=>setPeriodSummaryFrom(e.target.value)} />
          <label>To:</label>
          <input className='period-summary-date' type='date' value={periodSummaryTo} onChange={e=>setPeriodSummaryTo(e.target.value)} />
          <button type='button' className='btn btn-primary' onClick={fetchPeriodSummary} disabled={periodSummaryLoading}>{periodSummaryLoading ? 'Loading...' : 'Generate Summary'}</button>
          <button type='button' className='btn btn-sm' onClick={exportPeriodSummaryPdf} disabled={periodSummaryLoading || periodSummaryRows.length === 0}>Export PDF</button>
          <button type='button' className='btn btn-sm' onClick={exportPeriodSummaryExcel} disabled={periodSummaryLoading || periodSummaryRows.length === 0}>Export CSV</button>
        </div>

        {periodSummaryRows.length > 0 && (
          <div>
            <div className='small mb8' style={{fontWeight:700}}>Church: <strong>{currentChurchName()}</strong></div>
            <div className='small mb8' style={{fontWeight:700}}>Period: <strong>{periodSummaryFrom || 'All'}</strong> to <strong>{periodSummaryTo || 'All'}</strong></div>
            <div className='table-wrap' style={mainGridWrapStyle(380)}>
              <table style={{...mainGridTableStyle(true), minWidth: 640, width:'100%', tableLayout:'fixed'}}>
                <colgroup>
                  <col style={{width:'40%'}} />
                  <col style={{width:'20%'}} />
                  <col style={{width:'20%'}} />
                  <col style={{width:'20%'}} />
                </colgroup>
                <thead>
                  <tr>
                    <th style={{textAlign:'left'}}>Collection Item</th>
                    <th className='text-right'>Conference</th>
                    <th className='text-right'>{currentChurchName()}</th>
                    <th className='text-right'>Jumla / Total</th>
                  </tr>
                </thead>
                <tbody>
                  {periodSummaryRows.map((row, idx)=> (
                    <tr key={idx}>
                      <td style={{fontWeight:700,whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>{row.item}</td>
                      <td className='text-right' style={{fontWeight:700}}>{row.conference !== 0 ? formatNumber(row.conference) : '-'}</td>
                      <td className='text-right' style={{fontWeight:700}}>{row.local !== 0 ? formatNumber(row.local) : '-'}</td>
                      <td className='text-right' style={{fontWeight:800}}>{formatNumber(row.total)}</td>
                    </tr>
                  ))}
                  <tr className='row-total'>
                    <td>TOTAL</td>
                    <td className='text-right'>{formatNumber(periodSummaryTotals.conference)}</td>
                    <td className='text-right'>{formatNumber(periodSummaryTotals.local)}</td>
                    <td className='text-right'>{formatNumber(periodSummaryTotals.total)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        )}
        {!periodSummaryLoading && periodSummaryRows.length === 0 && (
          <div className='small'>No data yet — pick a date range and click Generate Summary.</div>
        )}
      </Card>

      <Card title="Members Collection Report" outline variant="info" className="mt-3">
        <div className='toolbar'>
          <label>From:</label>
          <input type='date' value={reportFrom} onChange={e=>setReportFrom(e.target.value)} />
          <label>To:</label>
          <input type='date' value={reportTo} onChange={e=>setReportTo(e.target.value)} />
          <button type='button' className='btn btn-primary' onClick={fetchMembersCollectionReport}>Run Report</button>
          <button type='button' className='btn' onClick={()=>setShowReportColumnPicker(true)}>Column Picker ({reportVisibleColumns.length} visible)</button>
          <button type='button' className='btn btn-sm' onClick={async ()=>{ setShowCollectionsInReports(s=>!s); if(!showCollectionsInReports) await fetchMembersCollections(); }}>{showCollectionsInReports? 'Hide Collections':'View Collections'}</button>
          <label>Max rows</label>
          <select value={reportMaxRows} onChange={e=>setReportMaxRows(Number(e.target.value))}>
            {[10,30,50,100].map(n=> <option key={n} value={n}>{n}</option>)}
          </select>
          <label className='flex gap8' style={{alignItems:'center'}}>
            <input type='checkbox' checked={reportShowOnlyDataColumns} onChange={e=>setReportShowOnlyDataColumns(e.target.checked)} />
            Only columns with data
          </label>
          <span className='small'>Showing {Math.min(reportRows.length, reportMaxRows)} of {reportRows.length}</span>
        </div>
        <div className='small mb8'>Showing columns: {reportVisibleColumns.map((c)=> labelForColumn(c)).join(', ')}</div>

        <Card title="Report Builders (5 Initial Reports)" outline variant="secondary" className="mb-3">
          <div className='form-grid mb8'>
            {reportBuilderOptions.map(opt=> (
              <button
                key={opt.id}
                type='button'
                onClick={()=>setSelectedReportBuilder(opt.id)}
                className={`report-builder-tile${selectedReportBuilder === opt.id ? ' report-builder-tile--active' : ''}`}
              >
                <div style={{fontWeight:700}}>{opt.title}</div>
                <div className='small'>{opt.description}</div>
              </button>
            ))}
          </div>
          <div className='toolbar'>
            <button type='button' className='btn btn-primary' onClick={()=>buildSelectedReport(selectedReportBuilder, selectedContributorName)}>Build Selected Report</button>
            <button type='button' className='btn btn-sm' onClick={exportBuiltReportPdf}>Export Built PDF</button>
            <button type='button' className='btn btn-sm' onClick={exportBuiltReportExcel}>Export Built CSV</button>
            <span className='small'>Selected: {reportBuilderOptions.find(r=> r.id===selectedReportBuilder)?.title}</span>
            <span className='small' style={{fontWeight:700, color: builtReportType ? 'var(--text)' : 'var(--text-faint)'}}>
              Last built: {builtReportType ? (reportBuilderOptions.find(r=> r.id===builtReportType)?.title || builtReportType) : 'None'}
            </span>
          </div>
          {selectedReportBuilder === 'contributor_totals' && (
            <div className='toolbar mt8'>
              <label>Contributor Name</label>
              <input
                list='contributor-names-list'
                value={selectedContributorName}
                onChange={e=>setSelectedContributorName(e.target.value)}
                placeholder='Select or type contributor name'
                style={{minWidth:260}}
              />
              <select value={selectedContributorName} onChange={(e)=> setSelectedContributorName(e.target.value)} style={{minWidth:260}}>
                <option value=''>Choose contributor name</option>
                {contributorNameOptions.map((name)=> (
                  <option key={name} value={name}>{name}</option>
                ))}
              </select>
              <datalist id='contributor-names-list'>
                {contributorNameOptions.map((name)=> (
                  <option key={name} value={name} />
                ))}
              </datalist>
            </div>
          )}
        </Card>

        <ColumnPicker
          open={showReportColumnPicker}
          title='Report Column Picker'
          allColumns={getReportAllColumns(reportRows)}
          currentColumns={getReportPickerCurrentColumns()}
          labelForColumn={labelForColumn}
          onApply={applyReportColumns}
          onCancel={()=>setShowReportColumnPicker(false)}
        />

        <div className='table-wrap mt8' style={mainGridWrapStyle(440)}>
          <table style={{...mainGridTableStyle(true), minWidth: Math.max(900, reportVisibleColumns.length * 140)}}>
            <thead>
              <tr>
                {reportRows[0] ? reportVisibleColumns.map(k=> <th key={k}>{labelForColumn(k)}</th>) : <th>No rows</th>}
              </tr>
            </thead>
            <tbody>
              {(() => {
                try{
                  return (Array.isArray(reportRows)? reportRows : []).slice(0, reportMaxRows).map((r,idx)=> (
                    <tr key={idx}>{reportVisibleColumns.map(k=> <td key={k}>{reportCellValue(k, r)}</td>)}</tr>
                  ))
                }catch(e){
                  console.error('Render error in reports table', e)
                  return <tr><td>Error rendering report: {String(e.message||e)}</td></tr>
                }
              })()}
            </tbody>
          </table>
        </div>

        <div className='mt8'>
          <button type='button' className='btn' onClick={aggregateByCollectionCode}>Aggregate by Collection Code</button>
          {aggRows && aggRows.length>0 && (
            <div className='mt8'>
              <h4>Aggregated by Collection Code</h4>
              <div className='table-wrap' style={mainGridWrapStyle(320)}>
                <table style={mainGridTableStyle(true)}>
                  <thead><tr><th>Collection</th><th>Count</th><th>Total s5</th><th>Total s6</th><th>Total s7</th></tr></thead>
                  <tbody>{aggRows.map(a=> <tr key={a.collection_code}><td>{a.collection_code}</td><td>{a.count}</td><td>{a.s5}</td><td>{a.s6}</td><td>{a.s7}</td></tr>)}</tbody>
                </table>
              </div>
            </div>
          )}
        </div>

        {builtReportRows.length > 0 && builtReportColumns.length > 0 && (
          <div className='mt8'>
            <h4>Built Report Output</h4>
            <div className='table-wrap' style={mainGridWrapStyle(320)}>
              <table style={mainGridTableStyle(true)}>
                <thead>
                  <tr>{builtReportColumns.map(c=> <th key={c} className={isBuiltReportRightAlignedColumn(c, builtReportType) ? 'text-right' : undefined}>{labelForBuiltReportColumn(c)}</th>)}</tr>
                </thead>
                <tbody>
                  {builtReportRows.slice(0, reportMaxRows).map((row, idx)=> (
                    <tr
                      key={idx}
                      className={builtReportType === 'contributor_totals' && String(row?.date || '').toUpperCase() === 'TOTAL' ? 'row-total' : undefined}
                    >
                      {builtReportColumns.map(col=> (
                        <td key={col} className={isBuiltReportRightAlignedColumn(col, builtReportType) ? 'text-right' : undefined}>
                          {isBuiltReportNumericColumn(col, builtReportType) ? formatNumber(row[col]) : String(row[col] ?? '')}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {builtReportType === 'item_totals' && itemTotalsByDateRows.length > 0 && (
              <div className='mt8'>
                <h5>Collection Item Totals By Date</h5>
                <div className='table-wrap' style={mainGridWrapStyle(260)}>
                  <table style={mainGridTableStyle(true)}>
                    <thead>
                      <tr>
                        <th>Date</th>
                        <th>Items With Values</th>
                        <th>Local</th>
                        <th>Conference</th>
                        <th>Total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {itemTotalsByDateRows.map((row, idx)=> (
                        <tr key={idx}>
                          <td>{String(row?.date || '')}</td>
                          <td className='text-right'>{Number(row?.entries || 0)}</td>
                          <td className='text-right'>{formatNumber(row?.local || 0)}</td>
                          <td className='text-right'>{formatNumber(row?.conference || 0)}</td>
                          <td className='text-right'>{formatNumber(row?.total || 0)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {meetingSummaryRows.length > 0 && (
          <Card title="Meeting Summary Form" outline variant="warning" className="mt-3">
            <div className='small mb8'>Period: {reportFrom || 'All'} to {reportTo || 'All'}</div>
            <div className='table-wrap' style={mainGridWrapStyle(320)}>
              <table style={mainGridTableStyle(true)}>
                <thead>
                  <tr>
                    <th>Contribution Item</th>
                    <th>Local</th>
                    <th>Conference</th>
                    <th>Total</th>
                  </tr>
                </thead>
                <tbody>
                  {meetingSummaryRows.map((row, idx)=> (
                    <tr key={idx}>
                      <td>{row.item}</td>
                      <td>{formatNumber(row.local)}</td>
                      <td>{formatNumber(row.conference)}</td>
                      <td>{formatNumber(row.total)}</td>
                    </tr>
                  ))}
                  <tr className='row-total'>
                    <td>TOTAL</td>
                    <td>{formatNumber(meetingSummaryTotals.local)}</td>
                    <td>{formatNumber(meetingSummaryTotals.conference)}</td>
                    <td>{formatNumber(meetingSummaryTotals.total)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </Card>
        )}
      </Card>

      {showCollectionsInReports && (
        <Card title="Members Collections (Reports)" outline variant="secondary" className="mt-3">
          <div className='toolbar'>
            <label>Max rows</label>
            <select value={reportCollectionsMaxRows} onChange={e=>setReportCollectionsMaxRows(Number(e.target.value))}>
              {[10,30,50,100].map(n=> <option key={n} value={n}>{n}</option>)}
            </select>
            <span className='small'>Showing {Math.min((membersCollections||[]).length, reportCollectionsMaxRows)} of {(membersCollections||[]).length}</span>
          </div>
          <div className='table-wrap' style={mainGridWrapStyle(300)}>
            <table style={mainGridTableStyle(true)}>
              <thead>
                <tr>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12) : ['id','collection_code','member_id','church','s1','s2','s3','s4','s5']).map(c=> <th key={c}>{labelForColumn(c)}</th>)}</tr>
              </thead>
              <tbody>
                {(() => {
                  try{
                    const rows = Array.isArray(membersCollections)? membersCollections : [];
                    if(rows.length === 0) return <tr><td colSpan={(membersCollectionsFields.length? membersCollectionsFields.slice(0,12).length:9)}>No collection rows</td></tr>
                    return rows.slice(0, reportCollectionsMaxRows).map(r=> (
                      <tr key={r && r.id}><td>{(membersCollectionsFields.length? membersCollectionsFields.slice(0,12).map(k=> displayCellValue(k, r)) : [r.id,r.collection_code,r.member_id,r.church,r.s1,r.s2,r.s3,r.s4,r.s5].map(v=> v)).map((v,i)=><span key={i}>{v!==null&&v!==undefined?String(v):''}{i < 8? ' | ' : ''}</span>)}</td></tr>
                    ))
                  }catch(e){ console.error('Error rendering report collections', e); return <tr><td>Error rendering collections: {String(e.message||e)}</td></tr> }
                })()}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  )
}
