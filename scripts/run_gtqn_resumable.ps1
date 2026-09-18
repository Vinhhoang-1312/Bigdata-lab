param(
    [string]$RunId = "gtqn-20260917-001",
    [string]$Variant = "full",
    [int]$MaxEpochs = 60,
    [int]$RetryDelaySeconds = 60
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$logDirectory = Join-Path $projectRoot "urbanflow-ml\output\training-logs"
New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null
$logPath = Join-Path $logDirectory ("gtqn-{0}-{1}.log" -f $RunId, $Variant)

Set-Location $projectRoot
$ErrorActionPreference = "Continue"
while ($true) {
    try {
        "$(Get-Date -Format o) starting or resuming $RunId/$Variant" | Tee-Object -FilePath $logPath -Append
        docker compose up -d spark-master spark-worker-1 spark-worker-2 2>&1 | Tee-Object -FilePath $logPath -Append
        if ($LASTEXITCODE -ne 0) { throw "docker compose up exited with $LASTEXITCODE" }
        docker compose --profile integration run --rm spark-submit `
            --master spark://spark-master:7077 `
            --conf spark.cores.max=2 `
            --conf spark.executor.cores=1 `
            --conf spark.executor.memory=768m `
            --conf spark.eventLog.enabled=true `
            --conf spark.eventLog.compress=false `
            --conf spark.eventLog.dir=file:///workspace/urbanflow-ml/output/spark-events `
            --conf spark.ui.showConsoleProgress=false `
            /workspace/urbanflow-ml/spark/train_gtqn_distributed.py `
            --run-id $RunId --variant $Variant --max-epochs $MaxEpochs --resume 2>&1 | Tee-Object -FilePath $logPath -Append
        $exitCode = $LASTEXITCODE
        if ($exitCode -eq 0) {
            "$(Get-Date -Format o) completed $RunId/$Variant" | Tee-Object -FilePath $logPath -Append
            exit 0
        }
        throw "distributed training exited with $exitCode"
    } catch {
        "$(Get-Date -Format o) attempt failed: $($_.Exception.Message)" | Tee-Object -FilePath $logPath -Append
    }
    "$(Get-Date -Format o) attempt failed; retrying in $RetryDelaySeconds seconds" | Tee-Object -FilePath $logPath -Append
    Start-Sleep -Seconds $RetryDelaySeconds
}
