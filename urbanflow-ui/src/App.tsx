import { useEffect, useState } from 'react'
import { Activity, BarChart3, BrainCircuit, ChevronRight, Database, MapPinned, ShieldCheck } from 'lucide-react'
import Streaming from './Streaming'
import MachineLearning from './MachineLearning'

type Workspace = 'streaming' | 'ml'
type StreamView = 'dispatch' | 'reliability'
type ModelView = 'forecast' | 'evidence'
type Route = { workspace: Workspace; streamView: StreamView; modelView: ModelView }

function currentRoute(): Route {
  const path = window.location.pathname
  return {
    workspace: path.startsWith('/ml') ? 'ml' : 'streaming',
    streamView: path === '/streaming/reliability' ? 'reliability' : 'dispatch',
    modelView: path === '/ml/evidence' ? 'evidence' : 'forecast',
  }
}

function routePath(route: Route): string {
  if (route.workspace === 'streaming') return route.streamView === 'dispatch' ? '/streaming' : '/streaming/reliability'
  return route.modelView === 'forecast' ? '/ml' : '/ml/evidence'
}

export default function App() {
  const [route, setRoute] = useState<Route>(currentRoute)
  const { workspace, streamView, modelView } = route

  useEffect(() => {
    const onPopState = () => setRoute(currentRoute())
    window.addEventListener('popstate', onPopState)
    return () => window.removeEventListener('popstate', onPopState)
  }, [])

  function navigate(next: Route) {
    const path = routePath(next)
    if (window.location.pathname !== path) window.history.pushState({}, '', path)
    setRoute(next)
  }

  return <div className="app-shell">
    <header className="app-header">
      <button className="brand" onClick={() => navigate({ ...route, workspace: 'streaming', streamView: 'dispatch' })} title="UrbanFlow home"><span className="brand-mark"><Activity size={20} strokeWidth={2.3} /></span><span>UrbanFlow</span><small>Mobility intelligence</small></button>
      <nav className="workspace-nav" aria-label="Workspace">
        <button className={workspace === 'streaming' ? 'active' : ''} onClick={() => navigate({ ...route, workspace: 'streaming' })}><Activity size={17} /> Streaming</button>
        <button className={workspace === 'ml' ? 'active' : ''} onClick={() => navigate({ ...route, workspace: 'ml' })}><BrainCircuit size={17} /> Machine learning</button>
      </nav>
      <span className="header-context"><span className="header-dot" /> Local research environment</span>
    </header>
    <div className="app-body">
      <aside className="app-sidebar">
        <div className="sidebar-group-label">{workspace === 'streaming' ? 'STREAMING' : 'MACHINE LEARNING'}</div>
        {workspace === 'streaming' ? <nav className="view-nav" aria-label="Streaming viewpoint">
          <button className={streamView === 'dispatch' ? 'active' : ''} onClick={() => navigate({ ...route, streamView: 'dispatch' })}><MapPinned size={17} /><span><b>Dispatch</b><small>Demand by zone</small></span>{streamView === 'dispatch' && <ChevronRight size={16} />}</button>
          <button className={streamView === 'reliability' ? 'active' : ''} onClick={() => navigate({ ...route, streamView: 'reliability' })}><ShieldCheck size={17} /><span><b>Reliability</b><small>Trust the data</small></span>{streamView === 'reliability' && <ChevronRight size={16} />}</button>
        </nav> : <nav className="view-nav" aria-label="Machine learning viewpoint">
          <button className={modelView === 'forecast' ? 'active' : ''} onClick={() => navigate({ ...route, modelView: 'forecast' })}><BarChart3 size={17} /><span><b>Forecast</b><small>Next 30 minutes</small></span>{modelView === 'forecast' && <ChevronRight size={16} />}</button>
          <button className={modelView === 'evidence' ? 'active' : ''} onClick={() => navigate({ ...route, modelView: 'evidence' })}><Database size={17} /><span><b>Evidence</b><small>Quality and limits</small></span>{modelView === 'evidence' && <ChevronRight size={16} />}</button>
        </nav>}
        <div className="sidebar-footer"><span className="footer-rule" />NYC TLC taxi zones<br />Research and teaching build</div>
      </aside>
      <main id="main-content">{workspace === 'streaming' ? <Streaming viewpoint={streamView} /> : <MachineLearning viewpoint={modelView} />}</main>
    </div>
  </div>
}
