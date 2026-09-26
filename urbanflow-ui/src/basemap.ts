import type { Map as MapLibreMap, StyleSpecification } from 'maplibre-gl'

export const CARTO_POSITRON_STYLE = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json'

function localStyle(): StyleSpecification {
  return {
    version: 8,
    sources: {},
    layers: [{ id: 'base', type: 'background', paint: { 'background-color': '#e7eff1' } }],
  }
}

export function fallBackToLocalBasemap(map: MapLibreMap, onFallback: () => void): void {
  let cartoActive = true
  map.on('error', () => {
    if (!cartoActive) return
    cartoActive = false
    onFallback()
    map.setStyle(localStyle())
  })
}

export function firstLabelLayer(map: MapLibreMap): string | undefined {
  return map.getStyle().layers.find(layer => layer.type === 'symbol')?.id
}
