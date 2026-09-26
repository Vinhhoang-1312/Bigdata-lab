import { useEffect, useRef, useState } from 'react'
import maplibregl, { type GeoJSONSource, type Map as MapLibreMap } from 'maplibre-gl'
import type { ZoneRow } from './types'
import { demandClass } from './zoneClass'

type Feature = { type: 'Feature'; properties: Record<string, unknown>; geometry: unknown }
type ZoneGeoJson = { type: 'FeatureCollection'; features: Feature[] }

export default function ZoneMap({ geometry, rows, selectedZone, onSelect }: {
  geometry: ZoneGeoJson; rows: ZoneRow[]; selectedZone: number | null; onSelect: (zone: number) => void
}) {
  const container = useRef<HTMLDivElement>(null)
  const map = useRef<MapLibreMap | null>(null)
  const [ready, setReady] = useState(false)
  const onSelectRef = useRef(onSelect)
  onSelectRef.current = onSelect

  useEffect(() => {
    if (!container.current) return
    const instance = new maplibregl.Map({
      container: container.current,
      center: [-73.96, 40.74], zoom: 10.0,
      style: {
        version: 8,
        sources: {},
        layers: [{ id: 'base', type: 'background', paint: { 'background-color': '#e7eff1' } }],
      },
    })
    map.current = instance
    instance.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right')
    instance.on('load', () => {
      instance.addSource('zones', { type: 'geojson', data: geometry as never })
      instance.addLayer({
        id: 'zones-fill', type: 'fill', source: 'zones',
        paint: {
          'fill-color': ['match', ['get', 'demand_class'], 'high', '#c7353b', 'near', '#dda94b', 'below', '#198d85', 'no-baseline', '#667f89', '#aeb9bd'],
          'fill-opacity': ['match', ['get', 'demand_class'], 'missing', 0.12, 0.68],
        },
      })
      instance.addLayer({ id: 'zones-line', type: 'line', source: 'zones', paint: { 'line-color': '#71818a', 'line-width': 0.7, 'line-opacity': 0.7 } })
      instance.addLayer({ id: 'zone-selected', type: 'line', source: 'zones',
        filter: ['==', ['get', 'location_id'], selectedZone ?? -1],
        paint: { 'line-color': '#14252d', 'line-width': 3 },
      })
      instance.once('idle', () => setReady(true))
      instance.on('click', 'zones-fill', (event) => {
        const feature = event.features?.[0]
        const id = Number(feature?.properties?.location_id)
        if (Number.isInteger(id)) onSelectRef.current(id)
      })
      instance.on('mousemove', 'zones-fill', (event) => {
        instance.getCanvas().style.cursor = 'pointer'
        const feature = event.features?.[0]
        const name = feature?.properties?.zone
        if (name) instance.getCanvas().title = String(name)
      })
      instance.on('mouseleave', 'zones-fill', () => { instance.getCanvas().style.cursor = ''; instance.getCanvas().title = '' })
    })
    return () => { instance.remove(); map.current = null }
  }, [])

  useEffect(() => {
    const instance = map.current
    if (!instance) return
    const byId = new Map(rows.map(row => [row.pickup_zone_id, row]))
    const painted: ZoneGeoJson = {
      type: 'FeatureCollection',
      features: geometry.features.map(feature => {
        const row = byId.get(Number(feature.properties.location_id))
        return { ...feature, properties: { ...feature.properties,
          demand_class: demandClass(row),
          current_trips: row?.current_trip_count ?? null,
        } }
      }),
    }
    const update = () => {
      const source = instance.getSource('zones') as GeoJSONSource | undefined
      if (source) source.setData(painted as never)
      if (instance.getLayer('zone-selected')) instance.setFilter('zone-selected', ['==', ['get', 'location_id'], selectedZone ?? -1])
    }
    if (instance.isStyleLoaded()) update()
    else instance.once('load', update)
  }, [geometry, rows, selectedZone])

  return <div className="zone-map-shell" data-ready={ready ? 'true' : 'false'}>
    <div className="zone-map" ref={container} aria-label="New York taxi-zone demand map" />
    {!ready && <div className="map-loading" role="status">Loading taxi-zone map…</div>}
  </div>
}
