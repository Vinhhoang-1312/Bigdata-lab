import { describe, expect, it } from 'vitest'
import { formatCount, formatRatio, sourceLabel, wapeLabel } from './format'

describe('honest display labels', () => {
  it('does not turn missing baselines into a numeric ratio', () => {
    expect(formatRatio(null)).toBe('No baseline')
    expect(formatRatio(1.5)).toBe('1.50x')
  })

  it('names old Gold as replay, not live traffic', () => {
    expect(sourceLabel('historical_replay')).toBe('Historical replay')
  })

  it('reports WAPE as error, not accuracy', () => {
    expect(wapeLabel(0.691)).toBe('69.1% error')
  })

  it('formats absent counts visibly', () => {
    expect(formatCount(null)).toBe('Not available')
  })
})
