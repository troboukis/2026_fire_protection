import { useEffect, useRef } from 'react'
import * as maplibregl from 'maplibre-gl'
import { LngLatBounds, type ErrorEvent as MapLibreErrorEvent, type Map as MapLibreMap, type StyleSpecification } from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { logError } from '../lib/logger'

export type MunicipalityMapLibreData = {
  works: GeoJSON.FeatureCollection<GeoJSON.Point>
  firePoints: GeoJSON.FeatureCollection<GeoJSON.Point>
  fireShapes: GeoJSON.FeatureCollection<GeoJSON.Polygon | GeoJSON.MultiPolygon>
  activeFires: GeoJSON.FeatureCollection<GeoJSON.Point>
  firms: GeoJSON.FeatureCollection<GeoJSON.Point>
}

export type MunicipalityMapLibreInteraction = {
  id: string
  kind: 'work' | 'fire' | 'copernicus' | 'current-fire' | 'firms'
  point: { x: number; y: number }
  fallback: { x: number; y: number }
  title: string
  items: string[]
  procurementId: number | null
  organizationName: string | null
}

type MunicipalityMapLibreProps = {
  feature: GeoJSON.Feature<GeoJSON.Polygon | GeoJSON.MultiPolygon>
  municipalityName: string
  data: MunicipalityMapLibreData
  fireViewMode: 'points' | 'shapes'
  showBoundary: boolean
  showWorks: boolean
  showFires: boolean
  showActiveFires: boolean
  showFirms: boolean
  onFeatureHover?: (interaction: MunicipalityMapLibreInteraction) => void
  onFeatureLeave?: () => void
  onFeatureClick?: (interaction: MunicipalityMapLibreInteraction) => void
  onBackgroundClick?: () => void
}

type LayerVisibility = Pick<
  MunicipalityMapLibreProps,
  'fireViewMode' | 'showBoundary' | 'showWorks' | 'showFires' | 'showActiveFires' | 'showFirms'
>

function parseInteraction(
  feature: GeoJSON.Feature,
  point: { x: number; y: number },
  fallback: { x: number; y: number },
): MunicipalityMapLibreInteraction | null {
  const properties = feature.properties
  if (!properties) return null
  const id = String(properties.interactionId ?? '')
  const kind = String(properties.kind ?? '') as MunicipalityMapLibreInteraction['kind']
  if (!id || !['work', 'fire', 'copernicus', 'current-fire', 'firms'].includes(kind)) return null

  let items: string[] = []
  try {
    const parsed = JSON.parse(String(properties.tooltipItems ?? '[]'))
    if (Array.isArray(parsed)) items = parsed.filter((item): item is string => typeof item === 'string')
  } catch {
    items = []
  }

  const procurementId = Number(properties.procurementId)
  return {
    id,
    kind,
    point: { x: point.x, y: point.y },
    fallback,
    title: String(properties.tooltipTitle ?? ''),
    items,
    procurementId: Number.isFinite(procurementId) ? procurementId : null,
    organizationName: typeof properties.organizationName === 'string' && properties.organizationName
      ? properties.organizationName
      : null,
  }
}

const OPENFREEMAP_POSITRON_STYLE = 'https://tiles.openfreemap.org/styles/positron'
const BASEMAP_LOAD_TIMEOUT_MS = 8_000

function getFallbackBasemapStyle(): StyleSpecification {
  return {
    version: 8,
    sources: {
      basemap: {
        type: 'raster',
        tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
        tileSize: 256,
        minzoom: 0,
        maxzoom: 17,
        attribution: '© OpenStreetMap contributors',
      },
    },
    layers: [{ id: 'basemap', type: 'raster', source: 'basemap' }],
  }
}

function getGeometryBounds(geometry: GeoJSON.Polygon | GeoJSON.MultiPolygon): LngLatBounds {
  const bounds = new LngLatBounds()
  const polygons = geometry.type === 'Polygon' ? [geometry.coordinates] : geometry.coordinates
  for (const polygon of polygons) {
    for (const ring of polygon) {
      for (const coordinate of ring) bounds.extend([coordinate[0], coordinate[1]])
    }
  }
  return bounds
}

function geometryPath(map: MapLibreMap, geometry: GeoJSON.Polygon | GeoJSON.MultiPolygon): string {
  const polygons = geometry.type === 'Polygon' ? [geometry.coordinates] : geometry.coordinates
  return polygons.map((polygon) => polygon.map((ring) => ring.map((coordinate, index) => {
    const point = map.project([coordinate[0], coordinate[1]])
    return `${index === 0 ? 'M' : 'L'}${point.x.toFixed(2)},${point.y.toFixed(2)}`
  }).join(' ') + ' Z').join(' ')).join(' ')
}

function svgElement<K extends keyof SVGElementTagNameMap>(name: K): SVGElementTagNameMap[K] {
  return document.createElementNS('http://www.w3.org/2000/svg', name)
}

type InteractionCallbacks = Pick<
  MunicipalityMapLibreProps,
  'onFeatureHover' | 'onFeatureLeave' | 'onFeatureClick'
>

function eventPoint(map: MapLibreMap, event: MouseEvent | PointerEvent) {
  const rect = map.getContainer().getBoundingClientRect()
  return { x: event.clientX - rect.left, y: event.clientY - rect.top }
}

function bindFeatureInteraction(
  map: MapLibreMap,
  element: SVGElement,
  feature: GeoJSON.Feature,
  fallback: { x: number; y: number },
  getCallbacks: () => InteractionCallbacks,
) {
  element.setAttribute('pointer-events', 'all')
  element.style.cursor = 'pointer'

  const interactionFor = (event: MouseEvent | PointerEvent) => (
    parseInteraction(feature, eventPoint(map, event), fallback)
  )
  const onMouseMove = (event: MouseEvent) => {
    const interaction = interactionFor(event)
    if (interaction) getCallbacks().onFeatureHover?.(interaction)
  }
  let lastTouchAt = 0
  element.addEventListener('mousemove', onMouseMove)
  element.addEventListener('mouseleave', () => getCallbacks().onFeatureLeave?.())
  element.addEventListener('pointerdown', (event: PointerEvent) => {
    if (event.pointerType === 'mouse') return
    const interaction = interactionFor(event)
    if (!interaction) return
    event.preventDefault()
    event.stopPropagation()
    lastTouchAt = performance.now()
    getCallbacks().onFeatureClick?.(interaction)
  })
  element.addEventListener('click', (event: MouseEvent) => {
    event.preventDefault()
    event.stopPropagation()
    if (performance.now() - lastTouchAt < 700) return
    const interaction = interactionFor(event)
    if (interaction) getCallbacks().onFeatureClick?.(interaction)
  })
}

function appendHitCircle(svg: SVGSVGElement, point: { x: number; y: number }, radius: number) {
  const hit = svgElement('circle')
  hit.setAttribute('cx', point.x.toFixed(2))
  hit.setAttribute('cy', point.y.toFixed(2))
  hit.setAttribute('r', String(radius))
  hit.setAttribute('fill', 'transparent')
  svg.append(hit)
  return hit
}

function appendCircle(
  map: MapLibreMap,
  svg: SVGSVGElement,
  feature: GeoJSON.Feature<GeoJSON.Point>,
  className: string,
  radius: number,
) {
  const [lon, lat] = feature.geometry.coordinates
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null
  const point = map.project([lon, lat])
  const circle = svgElement('circle')
  circle.setAttribute('class', className)
  circle.setAttribute('cx', point.x.toFixed(2))
  circle.setAttribute('cy', point.y.toFixed(2))
  circle.setAttribute('r', String(radius))
  svg.append(circle)
  return { point, circle }
}

function appendFirmsFootprint(
  map: MapLibreMap,
  svg: SVGSVGElement,
  feature: GeoJSON.Feature<GeoJSON.Point>,
) {
  const [lon, lat] = feature.geometry.coordinates
  const scanKm = Math.max(0.1, Number(feature.properties?.scanKm ?? 0.38))
  const trackKm = Math.max(0.1, Number(feature.properties?.trackKm ?? 0.38))
  const latKm = 111.32
  const lonKm = Math.max(1, latKm * Math.cos((lat * Math.PI) / 180))
  const east = map.project([lon + (scanKm / lonKm) / 2, lat])
  const west = map.project([lon - (scanKm / lonKm) / 2, lat])
  const north = map.project([lon, lat + (trackKm / latKm) / 2])
  const south = map.project([lon, lat - (trackKm / latKm) / 2])
  const center = map.project([lon, lat])
  const width = Math.min(24, Math.max(4.5, Math.abs(east.x - west.x)))
  const height = Math.min(24, Math.max(4.5, Math.abs(north.y - south.y)))
  const rect = svgElement('rect')
  rect.setAttribute('class', 'fire-firms__footprint')
  rect.setAttribute('x', (center.x - width / 2).toFixed(2))
  rect.setAttribute('y', (center.y - height / 2).toFixed(2))
  rect.setAttribute('width', width.toFixed(2))
  rect.setAttribute('height', height.toFixed(2))
  svg.append(rect)
  return { point: center, rect }
}

function appendActiveFire(
  map: MapLibreMap,
  svg: SVGSVGElement,
  feature: GeoJSON.Feature<GeoJSON.Point>,
) {
  const [lon, lat] = feature.geometry.coordinates
  const point = map.project([lon, lat])
  const color = String(feature.properties?.statusColor ?? 'rgba(209, 31, 31, 0.547)')
  const group = svgElement('g')
  group.setAttribute('class', 'fire-current__points')

  const pulse = svgElement('circle')
  pulse.setAttribute('class', 'fire-current__point-pulse')
  pulse.setAttribute('cx', point.x.toFixed(2))
  pulse.setAttribute('cy', point.y.toFixed(2))
  pulse.setAttribute('r', '4.2')
  pulse.setAttribute('stroke', color)
  const radiusAnimation = svgElement('animate')
  radiusAnimation.setAttribute('attributeName', 'r')
  radiusAnimation.setAttribute('values', '3.864;7.35;3.864')
  radiusAnimation.setAttribute('keyTimes', '0;0.7;1')
  radiusAnimation.setAttribute('dur', '1.2s')
  radiusAnimation.setAttribute('repeatCount', 'indefinite')
  const opacityAnimation = svgElement('animate')
  opacityAnimation.setAttribute('attributeName', 'stroke-opacity')
  opacityAnimation.setAttribute('values', '1;0.28;0')
  opacityAnimation.setAttribute('keyTimes', '0;0.7;1')
  opacityAnimation.setAttribute('dur', '1.2s')
  opacityAnimation.setAttribute('repeatCount', 'indefinite')
  pulse.append(radiusAnimation, opacityAnimation)

  const marker = svgElement('circle')
  marker.setAttribute('class', 'fire-current__point-marker')
  marker.setAttribute('cx', point.x.toFixed(2))
  marker.setAttribute('cy', point.y.toFixed(2))
  marker.setAttribute('r', '4.2')
  marker.setAttribute('stroke', color)
  group.append(pulse, marker)
  svg.append(group)
  return { point, group }
}

function drawGeographicLayers(
  map: MapLibreMap,
  svg: SVGSVGElement,
  municipality: GeoJSON.Feature<GeoJSON.Polygon | GeoJSON.MultiPolygon>,
  data: MunicipalityMapLibreData,
  visibility: LayerVisibility,
  getCallbacks: () => InteractionCallbacks,
) {
  const container = map.getContainer()
  svg.setAttribute('viewBox', `0 0 ${container.clientWidth} ${container.clientHeight}`)
  svg.replaceChildren()

  if (visibility.showBoundary) {
    const municipalityPath = svgElement('path')
    municipalityPath.setAttribute('class', 'municipality-maplibre__municipality')
    municipalityPath.setAttribute('d', geometryPath(map, municipality.geometry))
    municipalityPath.setAttribute('fill-rule', 'evenodd')
    svg.append(municipalityPath)
  }

  if (visibility.showFires && visibility.fireViewMode === 'shapes') {
    for (const feature of data.fireShapes.features) {
      const path = svgElement('path')
      path.setAttribute('class', 'municipality-maplibre__fire-shape')
      path.setAttribute('d', geometryPath(map, feature.geometry))
      path.setAttribute('fill-rule', 'evenodd')
      svg.append(path)
      const coordinate = feature.geometry.type === 'Polygon'
        ? feature.geometry.coordinates[0][0]
        : feature.geometry.coordinates[0][0][0]
      bindFeatureInteraction(map, path, feature, map.project([coordinate[0], coordinate[1]]), getCallbacks)
    }
  }
  if (visibility.showWorks) {
    for (const feature of data.works.features) {
      const rendered = appendCircle(map, svg, feature, 'municipality-maplibre__work', 5)
      if (!rendered) continue
      bindFeatureInteraction(map, appendHitCircle(svg, rendered.point, 10), feature, rendered.point, getCallbacks)
    }
  }
  if (visibility.showFires && visibility.fireViewMode === 'points') {
    for (const feature of data.firePoints.features) {
      const area = Number(feature.properties?.area ?? 0)
      const radius = Math.max(5, Math.min(12, 5 + Math.sqrt(Math.max(0, area)) * 0.2))
      const rendered = appendCircle(map, svg, feature, 'municipality-maplibre__fire-point', radius)
      if (!rendered) continue
      bindFeatureInteraction(map, appendHitCircle(svg, rendered.point, Math.max(7, radius)), feature, rendered.point, getCallbacks)
    }
  }
  if (visibility.showFirms) {
    for (const feature of data.firms.features) {
      const rendered = appendFirmsFootprint(map, svg, feature)
      bindFeatureInteraction(map, rendered.rect, feature, rendered.point, getCallbacks)
    }
  }
  if (visibility.showActiveFires) {
    for (const feature of data.activeFires.features) {
      const rendered = appendActiveFire(map, svg, feature)
      bindFeatureInteraction(map, appendHitCircle(svg, rendered.point, 8), feature, rendered.point, getCallbacks)
    }
  }
}

export default function MunicipalityMapLibre(props: MunicipalityMapLibreProps) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const overlayRef = useRef<SVGSVGElement | null>(null)
  const mapRef = useRef<MapLibreMap | null>(null)
  const dataRef = useRef(props.data)
  const visibilityRef = useRef<LayerVisibility>(props)
  const callbacksRef = useRef({
    onFeatureHover: props.onFeatureHover,
    onFeatureLeave: props.onFeatureLeave,
    onFeatureClick: props.onFeatureClick,
    onBackgroundClick: props.onBackgroundClick,
  })
  visibilityRef.current = props
  dataRef.current = props.data
  callbacksRef.current = {
    onFeatureHover: props.onFeatureHover,
    onFeatureLeave: props.onFeatureLeave,
    onFeatureClick: props.onFeatureClick,
    onBackgroundClick: props.onBackgroundClick,
  }

  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    const map = new maplibregl.Map({
      container,
      style: OPENFREEMAP_POSITRON_STYLE,
      bounds: getGeometryBounds(props.feature.geometry),
      fitBoundsOptions: { padding: 0, maxZoom: 13 },
      attributionControl: { compact: true },
      canvasContextAttributes: { contextType: 'webgl' },
    })
    mapRef.current = map
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right')

    let attributionCollapsed = false
    const collapseAttributionOnce = () => {
      if (attributionCollapsed) return
      const attribution = container.querySelector<HTMLElement>('.maplibregl-ctrl-attrib:not(.maplibregl-attrib-empty)')
      if (!attribution) return
      attribution.classList.add('maplibregl-compact')
      attribution.classList.remove('maplibregl-compact-show')
      attribution.setAttribute('open', '')
      attributionCollapsed = true
      map.off('styledata', collapseAttributionOnce)
      map.off('sourcedata', collapseAttributionOnce)
    }
    map.on('styledata', collapseAttributionOnce)
    map.on('sourcedata', collapseAttributionOnce)
    collapseAttributionOnce()

    const overlay = svgElement('svg')
    overlay.setAttribute('class', 'municipality-maplibre__geographic-overlay')
    overlay.setAttribute('aria-hidden', 'true')
    container.append(overlay)
    overlayRef.current = overlay

    const draw = () => drawGeographicLayers(
      map,
      overlay,
      props.feature,
      dataRef.current,
      visibilityRef.current,
      () => callbacksRef.current,
    )
    map.on('render', draw)
    map.on('move', draw)
    map.on('resize', draw)
    draw()

    const onBackgroundClick = () => callbacksRef.current.onBackgroundClick?.()
    map.on('click', onBackgroundClick)

    const resizeObserver = new ResizeObserver(() => map.resize())
    resizeObserver.observe(container)

    let usingFallbackStyle = false
    let primaryStyleReady = false
    const switchToFallbackStyle = () => {
      if (usingFallbackStyle || primaryStyleReady) return
      usingFallbackStyle = true
      map.setStyle(getFallbackBasemapStyle())
    }
    const onIdle = () => {
      if (usingFallbackStyle) return
      const basemapLayerIds = (map.getStyle().layers ?? []).map((layer) => layer.id)
      const hasRenderedBasemapFeatures = basemapLayerIds.length > 0
        && map.queryRenderedFeatures(undefined, { layers: basemapLayerIds }).length > 0
      if (hasRenderedBasemapFeatures) primaryStyleReady = true
      else switchToFallbackStyle()
    }
    const basemapTimeout = window.setTimeout(switchToFallbackStyle, BASEMAP_LOAD_TIMEOUT_MS)
    map.once('idle', onIdle)

    map.on('error', (event: MapLibreErrorEvent) => {
      if (import.meta.env.DEV) logError('[MunicipalityMapLibre] map failed', event.error)
      switchToFallbackStyle()
    })

    return () => {
      map.off('render', draw)
      map.off('move', draw)
      map.off('resize', draw)
      map.off('styledata', collapseAttributionOnce)
      map.off('sourcedata', collapseAttributionOnce)
      map.off('click', onBackgroundClick)
      map.off('idle', onIdle)
      window.clearTimeout(basemapTimeout)
      resizeObserver.disconnect()
      overlay.remove()
      overlayRef.current = null
      mapRef.current = null
      map.remove()
    }
  }, [props.feature])

  useEffect(() => {
    const map = mapRef.current
    const overlay = overlayRef.current
    if (!map || !overlay) return
    drawGeographicLayers(map, overlay, props.feature, props.data, visibilityRef.current, () => callbacksRef.current)
  }, [props.data, props.feature, props.fireViewMode, props.showActiveFires, props.showBoundary, props.showFires, props.showFirms, props.showWorks])

  return (
    <div
      ref={containerRef}
      className="municipality-maplibre"
      role="img"
      aria-label={`Χάρτης με το περίγραμμα και τα γεωγραφικά δεδομένα του δήμου ${props.municipalityName}`}
    />
  )
}
