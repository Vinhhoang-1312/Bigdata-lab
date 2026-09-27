import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './qa',
  timeout: 60_000,
  reporter: 'list',
  outputDir: '.tmp/playwright',
  use: { baseURL: process.env.URBANFLOW_UI_BASE_URL ?? 'http://127.0.0.1:5173', channel: 'msedge', screenshot: 'only-on-failure' },
  projects: [
    { name: 'desktop', use: { viewport: { width: 1440, height: 900 } } },
    { name: 'mobile', use: { viewport: { width: 390, height: 844 } } },
  ],
})
