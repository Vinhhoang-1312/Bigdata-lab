import { expect, test } from '@playwright/test'

test('map renders local taxi-zone polygons without a remote tile service', async ({ page }, testInfo) => {
  const issues: string[] = []
  page.on('console', message => { if (message.type() === 'error') issues.push(`${message.text()} @ ${message.location().url}`) })
  page.on('requestfailed', request => issues.push(`${request.url()}: ${request.failure()?.errorText}`))
  page.on('response', response => { if (response.status() >= 400) issues.push(`${response.status()} ${response.url()}`) })
  await page.goto('/streaming')
  await expect(page.locator('.zone-item')).toHaveCount(8, { timeout: 20_000 })
  await expect(page.locator('.zone-map-shell')).toHaveAttribute('data-ready', 'true', { timeout: 20_000 })
  const canvas = page.locator('.zone-map canvas')
  const info = await canvas.evaluate(element => ({ width: element.width, height: element.height, clientWidth: element.clientWidth, clientHeight: element.clientHeight }))
  console.log('MAP_DIAGNOSTIC', JSON.stringify({ info, issues }))
  await page.screenshot({ path: testInfo.outputPath('map-after-4s.png') })
  expect(info.width).toBeGreaterThan(100)
  expect(issues.some(issue => issue.includes('tile.openstreetmap.org') || issue.includes('cartocdn.com'))).toBe(false)
  expect(issues).toEqual([])
})
