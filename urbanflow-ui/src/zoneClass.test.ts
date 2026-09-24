import { describe, expect, it } from 'vitest'
import { demandClass } from './zoneClass'
import type { ZoneRow } from './types'

const row = (ratio: number | null) => ({ demand_ratio: ratio }) as ZoneRow

describe('demandClass', () => {
  it('does not confuse a Gold row without baseline with an absent row', () => {
    expect(demandClass(undefined)).toBe('missing')
    expect(demandClass(row(null))).toBe('no-baseline')
  })

  it('uses the documented demand-ratio threshold', () => {
    expect(demandClass(row(1.51))).toBe('high')
    expect(demandClass(row(1.5))).toBe('near')
    expect(demandClass(row(0.4))).toBe('below')
  })
})
