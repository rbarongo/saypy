import { jsPDF } from 'jspdf'
import autoTable from 'jspdf-autotable'

function slug(label) {
  return String(label || 'report').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '')
}

function dateStamp() {
  return new Date().toISOString().slice(0, 10)
}

/** Modeled on CollectionReceiptView.jsx's downloadPdf — plain jsPDF + one
 * autoTable call, just landscape (reports are wider than a receipt). */
export function exportReportPdf(label, columns, rows) {
  const doc = new jsPDF({ orientation: 'landscape' })
  doc.setFontSize(14)
  doc.text(label, 14, 16)
  doc.setFontSize(9)
  doc.text(`Generated ${new Date().toLocaleString()} — ${rows.length} record${rows.length === 1 ? '' : 's'}`, 14, 22)
  autoTable(doc, {
    startY: 28,
    head: [columns.map(c => c.label)],
    body: rows.map(row => columns.map(c => (row[c.key] === null || row[c.key] === undefined ? '' : String(row[c.key])))),
    styles: { fontSize: 8 },
    headStyles: { fillColor: [41, 82, 196] },
  })
  doc.save(`${slug(label)}-${dateStamp()}.pdf`)
}

/** Genuine .xlsx via the `xlsx` (SheetJS) package — lazy-imported here only,
 * never at module top level, so it doesn't inflate the main bundle. */
export async function exportReportExcel(label, columns, rows) {
  const XLSX = await import('xlsx')
  const sheetRows = rows.map(row => {
    const out = {}
    columns.forEach(c => { out[c.label] = row[c.key] ?? '' })
    return out
  })
  const sheet = XLSX.utils.json_to_sheet(sheetRows)
  const book = XLSX.utils.book_new()
  XLSX.utils.book_append_sheet(book, sheet, String(label || 'Report').slice(0, 31))
  XLSX.writeFile(book, `${slug(label)}-${dateStamp()}.xlsx`)
}
