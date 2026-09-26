import { LocateFixed } from 'lucide-react'

export default function MapFocusButton({ label, onClick }: { label: string; onClick: () => void }) {
  return <button className="map-focus-button" type="button" aria-label={label} title={label} onClick={onClick}>
    <LocateFixed size={16} aria-hidden="true" />
  </button>
}
