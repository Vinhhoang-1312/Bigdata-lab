import { expect, test } from '@playwright/test'

test('map renders source polygons and background tiles', async ({ page }, testInfo) => {
  const issues: string[] = []
  page.on('console', message => { if (message.type() === 'error') issues.push(message.text()) })
  page.on('requestfailed', request => issues.push(`${request.url()}: ${request.failure()?.errorText}`))
  await page.goto('/streaming')
  await expect(page.locator('.zone-item')).toHaveCount(8, { timeout: 20_000 })
  await page.waitForTimeout(4000)
  const canvas = page.locator('.zone-map canvas')
  const info = await canvas.evaluate(element => ({ width: element.width, height: element.height, clientWidth: element.clientWidth, clientHeight: element.clientHeight }))
  console.log('MAP_DIAGNOSTIC', JSON.stringify({ info, issues }))
  await page.screenshot({ path: testInfo.outputPath('map-after-4s.png') })
  expect(info.width).toBeGreaterThan(100)
})
