$ErrorActionPreference = "Stop"

function Assert-CommandSucceeded([string]$Description) {
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
}

try {
    docker compose down --volumes --remove-orphans
    Assert-CommandSucceeded "Initial Docker Compose cleanup"

    Remove-Item -LiteralPath "data/averages.json" -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath "data/averages.json.tmp" -Force -ErrorAction SilentlyContinue

    docker compose config --quiet
    Assert-CommandSucceeded "Docker Compose validation"
    docker compose up -d --build broker topic-init consumer
    Assert-CommandSucceeded "Kafka stack startup"

    $consumerReady = $false
    foreach ($attempt in 1..45) {
        $logs = docker compose logs consumer 2>&1 | Out-String
        if ($logs -match "Consumer ready") {
            $consumerReady = $true
            break
        }
        Start-Sleep -Seconds 1
    }
    if (-not $consumerReady) {
        throw "Consumer did not become ready within 45 seconds"
    }

    docker compose run --rm producer --count 10 --interval 0 --seed 42 --demo
    Assert-CommandSucceeded "Demo order production"

    $aggregateReady = $false
    foreach ($attempt in 1..45) {
        if (Test-Path -LiteralPath "data/averages.json") {
            try {
                $state = Get-Content -Raw -LiteralPath "data/averages.json" |
                    ConvertFrom-Json
                if ($state.total_count -eq 9) {
                    $aggregateReady = $true
                    break
                }
            }
            catch {
                # The atomic state file may be between creation and observation.
            }
        }
        Start-Sleep -Seconds 1
    }
    if (-not $aggregateReady) {
        throw "Expected an aggregate count of 9 within 45 seconds"
    }

    $consumerLogs = docker compose logs consumer 2>&1 | Out-String
    Assert-CommandSucceeded "Consumer log collection"
    if ($consumerLogs -notmatch "RETRY deferred") {
        throw "Consumer logs do not show non-blocking retry deferral"
    }
    if ($consumerLogs -notmatch "product=TEMPORARY_FAILURE") {
        throw "Consumer logs do not show temporary-failure recovery"
    }
    if ($consumerLogs -notmatch "DLQ published.*type=permanent") {
        throw "Consumer logs do not show permanent-failure DLQ routing"
    }

    $dlqOutput = docker compose run --rm dlq-viewer --max-messages 1 --timeout 15 |
        Out-String
    Assert-CommandSucceeded "DLQ inspection"
    if ($dlqOutput -notmatch '"failure-type": "permanent"') {
        throw "DLQ output does not contain the permanent failure record"
    }

    Write-Host (
        "Kafka integration test passed: 9 aggregated orders, " +
        "retry success, and 1 permanent DLQ record."
    )
}
finally {
    docker compose down --volumes --remove-orphans
}
