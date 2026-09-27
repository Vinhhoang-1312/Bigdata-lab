import { useEffect, useRef, useState } from 'react'
import * as maplibregl from 'maplibre-gl'
import type { GeoJSONSource, Map as MapLibreMap } from 'maplibre-gl'
import type { TripAnomaly } from './types'
import { CARTO_POSITRON_STYLE, fallBackToLocalBasemap, firstLabelLayer } from './basemap'
import MapFocusButton from './MapFocusButton'

type Feature = { type: 'Feature'; properties: Record<string, unknown>; geometry: unknown }
export type ZoneGeometry = { type: 'FeatureCollection'; features: Feature[] }

function coordinates(value: unknown, result: number[][] = []): number[][] {
  if (!Array.isArray(value)) return result
  if (value.length >= 2 && typeof value[0] === 'number' && typeof value[1] === 'number') {
    result.push(value as number[])
  } else {
    value.forEach(item => coordinates(item, result))
  }
  return result
}

function approximateCenter(feature: Feature): number[] | null {
  const points = coordinates((feature.geometry as { coordinates?: unknown })?.coordinates)
  if (!points.length) return null
  const lng = points.map(point => point[0])
  const lat = points.map(point => point[1])
  return [(Math.min(...lng) + Math.max(...lng)) / 2, (Math.min(...lat) + Math.max(...lat)) / 2]
}

function selectedRoute(geometry: ZoneGeometry, trip: TripAnomaly | null) {
  const empty = { type: 'FeatureCollection' as const, features: [] as object[] }
  if (!trip) return empty
  const origin = geometry.features.find(feature => Number(feature.properties.location_id) === trip.pickup_zone_id)
  const destination = geometry.features.find(feature => Number(feature.properties.location_id) === trip.dropoff_zone_id)
  const start = origin && approximateCenter(origin)
  const end = destination && approximateCenter(destination)
  if (!start || !end) return empty
  return { type: 'FeatureCollection' as const, features: [
    { type: 'Feature', properties: { kind: 'route' }, geometry: { type: 'LineString', coordinates: [start, end] } },
    { type: 'Feature', properties: { kind: 'pickup' }, geometry: { type: 'Point', coordinates: start } },
    { type: 'Feature', properties: { kind: 'dropoff' }, geometry: { type: 'Point', coordinates: end } },
  ] }
}

function focusRoute(instance: MapLibreMap, route: ReturnType<typeof selectedRoute>) {
  const line = route.features[0] as { geometry?: { coordinates?: number[][] } } | undefined
  const ends = line?.geometry?.coordinates
  if (ends?.length !== 2) return
  if (ends[0][0] === ends[1][0] && ends[0][1] === ends[1][1]) {
    instance.jumpTo({ center: ends[0] as [number, number], zoom: 11 })
  } else {
    const southwest: [number, number] = [Math.min(ends[0][0], ends[1][0]), Math.min(ends[0][1], ends[1][1])]
    const northeast: [number, number] = [Math.max(ends[0][0], ends[1][0]), Math.max(ends[0][1], ends[1][1])]
    instance.fitBounds([southwest, northeast], { padding: 65, maxZoom: 11, duration: 0 })
  }
}

export default function TripAnomalyMap({ geometry, trips, selected, onSelect }: {
  geometry: ZoneGeometry
  trips: TripAnomaly[]
  selected: TripAnomaly | null
  onSelect: (rank: number) => void
}) {
  const container = useRef<HTMLDivElement>(null)
  const map = useRef<MapLibreMap | null>(null)
  const onSelectRef = useRef(onSelect)
  const tripsRef = useRef(trips)
  const [ready, setReady] = useState(false)
  const [localBasemap, setLocalBasemap] = useState(false)
  const [zoom, setZoom] = useState<number | null>(null)
  onSelectRef.current = onSelect
  tripsRef.current = trips

  useEffect(() => {
    if (!container.current) return
    const instance = new maplibregl.Map({
      container: container.current, center: [-73.96, 40.74], zoom: 10,
      style: CARTO_POSITRON_STYLE,
    })
    map.current = instance
    fallBackToLocalBasemap(instance, () => { setReady(false); setLocalBasemap(true) })
    instance.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right')
    instance.on('moveend', () => setZoom(instance.getZoom()))
    let handlersAttached = false
    instance.on('style.load', () => {
      instance.addSource('anomaly-zones', { type: 'geojson', data: geometry as never })
      instance.addLayer({ id: 'anomaly-zones-fill', type: 'fill', source: 'anomaly-zones',
        paint: { 'fill-color': '#879b9d', 'fill-opacity': 0.13 } }, firstLabelLayer(instance))
      instance.addLayer({ id: 'anomaly-zones-outline', type: 'line', source: 'anomaly-zones',
        paint: { 'line-color': '#789092', 'line-width': 0.65, 'line-opacity': 0.65 } })
      instance.addLayer({ id: 'anomaly-origin', type: 'fill', source: 'anomaly-zones',
        filter: ['==', ['get', 'location_id'], -1],
        paint: { 'fill-color': '#bd4544', 'fill-opacity': 0.53 } })
      instance.addLayer({ id: 'anomaly-destination', type: 'fill', source: 'anomaly-zones',
        filter: ['==', ['get', 'location_id'], -1],
        paint: { 'fill-color': '#d2a14e', 'fill-opacity': 0.47 } })
      instance.addSource('anomaly-route', { type: 'geojson', data: selectedRoute(geometry, null) as never })
      instance.addLayer({ id: 'anomaly-route-line', type: 'line', source: 'anomaly-route',
        paint: { 'line-color': '#243e45', 'line-width': 2.5, 'line-dasharray': [2, 1.3] },
        filter: ['==', ['get', 'kind'], 'route'] })
      instance.addLayer({ id: 'anomaly-route-points', type: 'circle', source: 'anomaly-route',
        filter: ['!=', ['get', 'kind'], 'route'],
        paint: { 'circle-color': ['match', ['get', 'kind'], 'pickup', '#bd4544', '#d2a14e'],
          'circle-radius': ['match', ['get', 'kind'], 'pickup', 8, 5], 'circle-stroke-color': '#fff', 'circle-stroke-width': 1.5 } })
      if (!handlersAttached) {
        handlersAttached = true
        instance.on('click', 'anomaly-zones-fill', event => {
          const id = Number(event.features?.[0]?.properties?.location_id)
          const trip = tripsRef.current.find(item => item.pickup_zone_id === id || item.dropoff_zone_id === id)
          if (trip) onSelectRef.current(trip.rank)
        })
      }
      setReady(true)
    })
    return () => { instance.remove(); map.current = null }
  }, [geometry])

  useEffect(() => {
    const instance = map.current
    if (!instance || !instance.getSource('anomaly-route')) return
    instance.setFilter('anomaly-origin', ['==', ['get', 'location_id'], selected?.pickup_zone_id ?? -1])
    instance.setFilter('anomaly-destination', ['==', ['get', 'location_id'], selected?.dropoff_zone_id ?? -1])
    const route = selectedRoute(geometry, selected)
    ;(instance.getSource('anomaly-route') as GeoJSONSource).setData(route as never)
    focusRoute(instance, route)
    setZoom(instance.getZoom())
  }, [geometry, selected, ready])

  return <div className="zone-map-shell" data-ready={ready ? 'true' : 'false'} data-zoom={zoom?.toFixed(1)}>
    <div className="zone-map" ref={container} aria-label="Anomalous taxi trip pickup and dropoff zones" />
    <MapFocusButton label="Focus selected trip" onClick={() => {
      if (!map.current) return
      focusRoute(map.current, selectedRoute(geometry, selected))
      setZoom(map.current.getZoom())
    }} />
    {localBasemap && <div className="map-fallback" role="status">Base map unavailable. Showing taxi zones.</div>}
    {!ready && <div className="map-loading" role="status">Loading taxi-zone map…</div>}
  </div>
}
