#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  docker compose down --volumes --remove-orphans
}
trap cleanup EXIT

docker compose down --volumes --remove-orphans
rm -f data/averages.json data/averages.json.tmp
docker compose config --quiet
docker compose up -d --build broker topic-init consumer

for _ in $(seq 1 45); do
  if docker compose logs consumer 2>&1 | grep -q "Consumer ready"; then
    break
  fi
  sleep 1
done
docker compose logs consumer 2>&1 | grep -q "Consumer ready"

docker compose run --rm producer --count 10 --interval 0 --seed 42 --demo

for _ in $(seq 1 45); do
  if python -c 'import json; assert json.load(open("data/averages.json", encoding="utf-8"))["total_count"] == 9' 2>/dev/null; then
    break
  fi
  sleep 1
done
python -c 'import json; state=json.load(open("data/averages.json", encoding="utf-8")); assert state["total_count"] == 9, state'

consumer_logs="$(docker compose logs consumer 2>&1)"
grep -q "RETRY deferred" <<<"$consumer_logs"
grep -q "product=TEMPORARY_FAILURE" <<<"$consumer_logs"
grep -q "DLQ published.*type=permanent" <<<"$consumer_logs"

dlq_output="$(docker compose run --rm dlq-viewer --max-messages 1 --timeout 15)"
grep -q '"failure-type": "permanent"' <<<"$dlq_output"

printf '%s\n' "Kafka integration test passed: 9 aggregated orders, retry success, and 1 permanent DLQ record."
