import type { ReactNode } from 'react'

export type PipelineStep = {
  title: string
  detail: string
  icon?: ReactNode
}

export default function TrainingPipeline({
  label,
  title,
  description,
  steps,
}: {
  label: string
  title: string
  description: string
  steps: PipelineStep[]
}) {
  return <section className="training-pipeline" aria-label={title}>
    <div className="section-heading">
      <div><span className="eyebrow">{label}</span><h2>{title}</h2></div>
    </div>
    <p className="training-pipeline-description">{description}</p>
    <ol className="training-pipeline-steps">
      {steps.map((step, index) => <li key={`${step.title}-${index}`}>
        <span className="pipeline-step-index" aria-hidden="true">{index + 1}</span>
        <div><strong>{step.icon}{step.title}</strong><span>{step.detail}</span></div>
      </li>)}
    </ol>
  </section>
}
