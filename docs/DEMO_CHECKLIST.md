# Live Demonstration Checklist

## Before presenting

- Confirm the report shows `Threemavithana T.M.` and `EG/2021/4835`.
- Start Docker Desktop.
- Open PowerShell in the repository directory.
- For a clean run, use `docker compose down` and delete
  `data/averages.json` if it exists.
- Run `python -m pytest` if Python dependencies are installed.

## Demonstration sequence

1. Briefly show `order.avsc` and point out `orderId`, `product`, and `price`.
2. Show the three topics and data flow in the README architecture diagram.
3. Run `docker compose up -d --build broker topic-init consumer`.
4. Run `docker compose logs -f consumer` in the first terminal.
5. In a second terminal, run
   `docker compose run --rm producer --count 10 --interval 0.35 --seed 42 --demo`.
6. Point out each live `PROCESSED` line and the changing overall average.
7. Point out the two `RETRY` cycles and eventual successful processing.
8. Point out the direct `DLQ published` line for the permanent failure.
9. Stop following logs with Ctrl+C; this does not stop the consumer container.
10. Run `docker compose run --rm dlq-viewer --max-messages 1 --timeout 15`.
11. Explain the decoded Avro order and failure headers shown by the viewer.
12. Show the tests, report, README, and Git log.

## Short explanation to give

“The producer validates each order against `order.avsc` and sends binary Avro to
Kafka. The consumer updates both overall and per-product running averages. It
commits an offset only after processing or safely rerouting the message.
Temporary errors use exponential-backoff retries without blocking unrelated
orders, while permanent errors go straight to the dead letter topic. The DLQ
keeps the original Avro order and stores diagnostic details in Kafka headers.”
