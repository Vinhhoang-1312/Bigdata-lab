import { expect, test } from '@playwright/test'

test('streaming views are usable and labelled as replay', async ({ page }, testInfo) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/streaming')
  await expect(page.getByRole('heading', { name: 'Taxi demand by zone' })).toBeVisible()
  await expect(page.getByText('Historical replay', { exact: true })).toBeVisible()
  await expect(page.getByText('Feb 19, 2023', { exact: false }).first()).toBeVisible()
  await expect(page.locator('.zone-item')).toHaveCount(8)
  await expect(page.locator('.zone-map-shell')).toHaveAttribute('data-ready', 'true', { timeout: 20_000 })
  await expect(page.locator('.zone-map canvas')).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('stream-dispatch.png'), fullPage: true })
  await page.getByRole('button', { name: /Reliability/ }).click()
  await expect(page.getByRole('heading', { name: 'Pipeline reliability' })).toBeVisible()
  await expect(page.getByText('Not yet instrumented')).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('stream-reliability.png'), fullPage: true })
  expect(errors).toEqual([])
  expect(await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth)).toBe(0)
})

test('ML forecast and evidence are distinct, with measured uncertainty', async ({ page }, testInfo) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/ml')
  await expect(page.getByRole('heading', { name: 'Pickup demand forecast' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Run forecast' })).toBeEnabled()
  await page.getByRole('button', { name: 'Run forecast' }).click()
  await expect(page.getByText('Central estimate · P50')).toBeVisible()
  await expect(page.getByText('Estimated range · P10–P90')).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('ml-forecast.png'), fullPage: true })
  await page.getByRole('button', { name: /Evidence/ }).click()
  await expect(page.getByRole('heading', { name: 'Model evidence' })).toBeVisible()
  await expect(page.getByText('69.1% error')).toBeVisible()
  await expect(page.getByText('Graph + time, not a Transformer')).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('ml-evidence.png'), fullPage: true })
  expect(errors).toEqual([])
  expect(await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth)).toBe(0)
})

test('viewpoint switching does not reload the document', async ({ page }) => {
  await page.goto('/streaming')
  await expect(page.locator('.zone-item')).toHaveCount(8)
  await page.evaluate(() => { (window as Window & { marker?: number }).marker = 42 })
  await page.getByRole('button', { name: /Reliability/ }).click()
  await page.getByRole('button', { name: /Dispatch/ }).click()
  expect(await page.evaluate(() => (window as Window & { marker?: number }).marker)).toBe(42)
})
