# Real Time Analytics MicroService Challenge - Daniel Tateishi

- **Producer** 
    - exposes a REST endpoint and publishes messages to the incoming.user_activity topic.
- **Consumer** 
    - Implement a Kafka consumer that reads messages from the `incoming.user_activity` topic.
    - Perform data transformations, such as counting the number of users and the number of `page_view` activities.
    - Write aggregated results to a PostgreSQL database. Only one table is needed/recommended.
- **API**
    - `GET /stats` (port 8080) returns the aggregated results stored in PostgreSQL:
      the number of distinct users, the total count per activity, and the count per user per activity.
      The list of users is paginated.

      ```json
      {
        "total_users": 2,
        "activity_totals": { "page_view": 5 },
        "user_activity_counts": {
          "u1": { "page_view": 3 },
          "u2": { "page_view": 2 }
        },
        "pagination": { "page": 1, "page_size": 50, "total_pages": 1 }
      }
      ```
      - `total_users` and `activity_totals` always cover every user. Only `user_activity_counts` is paginated, in `user_id` order, and a user is never split across pages.
      - Query parameters: `page` (default `1`) and `page_size` (default `50`, maximum `100`), for example `GET /stats?page=2&page_size=20`.
      - A `page` after the last one returns an empty `user_activity_counts` with the same totals.
      - A `page` or `page_size` that is not a whole number in range returns `400`; database errors return `500`, both as `{"error": "..."}`; other methods return `405`.

## Running the local setup

### Prerequisites

- Docker with Compose v2 (`docker compose`).
- Python 3.10+ for the load test (optional).
- Go 1.27.1 to run the unit tests outside Docker (optional).

### 1. Start everything

From the repository root:

```bash
docker compose -f deployments/docker-compose.yml up --build
```

- 3 Kafka controllers and 3 Kafka brokers;
- PostgreSQL, with a health check the consumer and the API wait for;
- `kafka-init`, a one-shot job that creates the `incoming.user_activity` topic with 6 partitions and exits;
- the `producer`, `consumer` and `api` services, which start once the topic and the database are ready.

| Service | Host port | What it is |
|---|---|---|
| producer | 8081 | `POST /user_activities` |
| api | 8080 | `GET /stats` |
| postgres | 5432 | user `analytics_chall`, database `analytics_db`, no password |
| broker-1, broker-2, broker-3 | 29092, 39092, 49092 | Kafka, reachable from the host |

The consumer runs one worker goroutine per partition it owns. To use a different partition count, stop the stack and start it again with `TOPIC_PARTITIONS=3 docker compose -f deployments/docker-compose.yml up --build`.

### 2. Send an activity and read the stats

```bash
curl -i -X POST localhost:8081/user_activities \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"user-1","activity_type":"page_view","timestamp":"2026-10-05T10:00:00Z","metadata":{"page_url":"/pricing","referrer":"google"}}'
```

The producer answers `202 Accepted` with `{"status":"ok"}`. A payload without a `user_id` is rejected with `400`. Fields outside the activity schema are ignored.

```bash
curl localhost:8080/stats
```

```json
{"total_users":1,"activity_totals":{"page_view":1},"user_activity_counts":{"user-1":{"page_view":1}},"pagination":{"page":1,"page_size":50,"total_pages":1}}
```

The consumer flushes after every batch, so the numbers show up within a few seconds.

### 3. Stress test with Locust

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r loadtest/requirements.txt
locust -f loadtest/locustfile.py --headless
```

Each simulated user has its own `user_id` and posts a `page_view` activity to the producer at random intervals (between `MIN_WAIT_SECONDS` and `MAX_WAIT_SECONDS`), with a random `page_url` and `referrer`.

The number of users grows gradually. All the settings are constants at the top of `loadtest/locustfile.py`:

| Constant | Default | Meaning |
|---|---|---|
| `HOST` | `http://localhost:8081` | producer to hit (`--host` overrides it) |
| `INITIAL_USERS` | 10 | users at the start |
| `USERS_PER_STEP` | 10 | users added at every step |
| `STEP_INTERVAL_SECONDS` | 15 | seconds between steps |
| `MAX_USERS` | 100 | where the growth stops |
| `HOLD_SECONDS` | 30 | how long to keep `MAX_USERS` before stopping |
| `MIN_WAIT_SECONDS`, `MAX_WAIT_SECONDS` | 0.5, 2.0 | pause between the requests of one user |

To watch it live in the browser instead, leave out `--headless` and open <http://localhost:8089>.

While it runs, `curl localhost:8080/stats` shows `total_users` growing with the user count. When it finishes and the consumer has caught up, `activity_totals.page_view` equals Locust's request count minus its failures. The consumer has caught up when the `LAG` column of this command is 0 for every partition:

```bash
docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker-1:19092 --describe --group analytics-agg
```

### 4. Stop

```bash
docker compose -f deployments/docker-compose.yml down -v
```

`-v` also deletes the PostgreSQL data, so the stats start from zero next time. Leave it out to keep them.

### Running the tests

```bash
go test ./cmd/... ./internal/...
```

They are unit tests and need neither Docker nor a database. Add `-race` to run them with the race detector

>Note: -race needs a C compiler.

### Configuration

Every service reads its settings from environment variables, which `deployments/docker-compose.yml` sets for the containers. `.env.example` lists all of them with sample values. The producer and the consumer exit at startup with `KAFKA_BROKERS must list at least one broker` when no broker is configured.
