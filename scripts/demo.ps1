$ErrorActionPreference = "Stop"

Write-Host "Starting Kafka, creating topics, and building the consumer..."
docker compose up -d --build broker topic-init consumer

Write-Host "Publishing a 10-order demonstration batch..."
docker compose run --rm producer --count 10 --interval 0.35 --seed 42 --demo

Write-Host "Waiting for retry processing to finish..."
Start-Sleep -Seconds 6

Write-Host "`nConsumer output:"
docker compose logs --tail 100 consumer

Write-Host "`nDead Letter Queue contents:"
docker compose run --rm dlq-viewer --max-messages 1 --timeout 15

Write-Host "`nDemo complete. Run .\scripts\stop.ps1 when finished."
