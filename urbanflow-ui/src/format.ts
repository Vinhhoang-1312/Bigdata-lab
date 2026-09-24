export function formatCount(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? 'Not available' : Math.round(value).toLocaleString('en-US')
}

export function formatRatio(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? 'No baseline' : `${value.toFixed(2)}x`
}

export function sourceLabel(mode: string | undefined): string {
  if (mode === 'historical_replay') return 'Historical replay'
  if (mode === 'live') return 'Live feed'
  return 'Source unknown'
}

export function wapeLabel(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? 'Not evaluated' : `${(value * 100).toFixed(1)}% error`
}

export function newYorkWindow(value: string | null | undefined): string {
  if (!value) return 'No window yet'
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York', month: 'short', day: 'numeric', year: 'numeric',
    hour: '2-digit', minute: '2-digit', hour12: false,
  }).format(new Date(value)) + ' ET'
}
