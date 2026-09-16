export function encodeS1({ date, church, s3 }){
  if(!date || s3 == null || Number.isNaN(Number(s3))) return null
  const ymd = date.toISOString().slice(0, 10).replace(/-/g, '')
  const cidn = String(church || '').padStart(3, '0')
  const s3n = String(Number(s3)).padStart(3, '0')
  return `${ymd}${cidn}${s3n}`
}

export function decodeS1(s1){
  if(!s1) return { s3: null, church: null }
  try{
    const digits = String(s1).replace(/[^0-9]/g, '')
    if(digits.length < 14) return { s3: null, church: null }
    const s3 = parseInt(digits.slice(-3), 10)
    const church = parseInt(digits.slice(-6, -3), 10)
    return {
      s3: Number.isNaN(s3) ? null : s3,
      church: Number.isNaN(church) ? null : church,
    }
  }catch(e){
    return { s3: null, church: null }
  }
}
