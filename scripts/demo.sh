#!/usr/bin/env sh
set -eu

docker compose up -d --build broker topic-init consumer
docker compose run --rm producer --count 10 --interval 0.35 --seed 42 --demo
sleep 6
docker compose logs --tail 100 consumer
docker compose run --rm dlq-viewer --max-messages 1 --timeout 15

printf '%s\n' 'Demo complete. Run docker compose down when finished.'
