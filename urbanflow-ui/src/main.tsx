import React from 'react'
import { createRoot } from 'react-dom/client'
import { ConfigProvider } from 'antd'
import App from './App'
import 'antd/dist/reset.css'
import 'maplibre-gl/dist/maplibre-gl.css'
import './styles.css'

createRoot(document.getElementById('root')!).render(
  <ConfigProvider theme={{ token: {
    colorPrimary: '#0e766e', colorInfo: '#0e766e', colorText: '#182631',
    borderRadius: 6, fontFamily: 'Segoe UI, Arial, sans-serif', fontSize: 14,
  } }}>
    <App />
  </ConfigProvider>,
)
