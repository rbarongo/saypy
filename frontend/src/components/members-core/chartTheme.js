// Validated categorical palette (blue, orange, aqua, yellow, magenta, green,
// violet, red) — fixed hue order, CVD-safe adjacent pairs. Status colors are
// a separate, reserved palette (never reused for "series N").
const CATEGORICAL_LIGHT = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948']
const CATEGORICAL_DARK = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300', '#9085e9', '#e66767']

const STATUS = {
  good: '#0ca30c',
  warning: '#fab219',
  serious: '#ec835a',
  critical: '#d03b3b',
}

// Member status codes mapped onto the reserved status palette rather than an
// arbitrary categorical color — active literally means "good".
const STATUS_CODE_MAP = {
  active: STATUS.good,
  inactive: STATUS.warning,
  transferred: STATUS.serious,
  deceased: STATUS.critical,
}

export function isDarkTheme(){
  if(typeof document === 'undefined') return false
  const stamped = document.documentElement.getAttribute('data-theme')
  if(stamped === 'dark') return true
  if(stamped === 'light') return false
  return typeof window !== 'undefined' && window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches
}

export function categoricalColors(){
  return isDarkTheme() ? CATEGORICAL_DARK : CATEGORICAL_LIGHT
}

/** One hue for every bar/line in a single-series chart — never a different
 * color per bar for a nominal, unordered category (that's the "value ramp on
 * nominal categories" anti-pattern). */
export function seriesColor(){
  return categoricalColors()[0]
}

export function statusColorForCode(code){
  return STATUS_CODE_MAP[code] || categoricalColors()[5]
}

export function cssVar(name, fallback){
  if(typeof document === 'undefined') return fallback
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  return value || fallback
}

/** Shared Chart.js option scaffolding so every chart matches the app's theme
 * (grid/tick colors) without hardcoding light-only values. */
export function baseChartOptions(){
  const textMuted = cssVar('--text-muted', '#6b7280')
  const border = cssVar('--border', '#e5e7eb')
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { labels: { color: textMuted, boxWidth: 12, padding: 12 } },
      tooltip: { backgroundColor: cssVar('--surface-2', '#1f2937'), titleColor: cssVar('--text', '#111'), bodyColor: cssVar('--text', '#111'), borderColor: border, borderWidth: 1 },
    },
    scales: {
      x: { ticks: { color: textMuted }, grid: { color: border, drawTicks: false } },
      y: { ticks: { color: textMuted, precision: 0 }, grid: { color: border, drawTicks: false }, beginAtZero: true },
    },
  }
}
