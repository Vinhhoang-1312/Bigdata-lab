import { defineConfig } from 'vitest/config'

export default defineConfig({
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/v1/streaming': 'http://127.0.0.1:8001',
      '/v1/forecast': 'http://127.0.0.1:8002',
      '/v1/anomaly': 'http://127.0.0.1:8002',
      '/v1/ml': 'http://127.0.0.1:8002',
    },
  },
  test: { environment: 'jsdom', include: ['src/**/*.test.ts'] },
})
