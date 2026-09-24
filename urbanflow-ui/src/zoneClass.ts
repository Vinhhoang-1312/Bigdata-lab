import type { ZoneRow } from './types'

export type DemandClass = 'missing' | 'no-baseline' | 'high' | 'near' | 'below'

export function demandClass(row: ZoneRow | undefined): DemandClass {
  if (!row) return 'missing'
  if (row.demand_ratio == null) return 'no-baseline'
  if (row.demand_ratio > 1.5) return 'high'
  return row.demand_ratio >= 0.8 ? 'near' : 'below'
}
