export function formatCurrency(value: number, decimals = 2): string {
  if (isNaN(value)) return '-'
  if (Math.abs(value) >= 1e9) return `$${(value / 1e9).toFixed(decimals)}B`
  if (Math.abs(value) >= 1e6) return `$${(value / 1e6).toFixed(decimals)}M`
  if (Math.abs(value) >= 1e3) return `$${(value / 1e3).toFixed(decimals)}K`
  return `$${value.toFixed(decimals)}`
}

export function formatPercentage(value: number, decimals = 2): string {
  if (isNaN(value)) return '-'
  return `${(value * 100).toFixed(decimals)}%`
}

export function formatNumber(value: number, decimals = 2): string {
  if (value === null || value === undefined || isNaN(value)) return '-'
  return value.toFixed(decimals)
}
