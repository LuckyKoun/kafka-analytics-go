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

      ```json
      {
        "total_users": 2,
        "activity_totals": { "page_view": 5 },
        "user_activity_counts": {
          "u1": { "page_view": 3 },
          "u2": { "page_view": 2 }
        }
      }
      ```
      Errors return `500` with `{"error": "..."}`; other methods return `405`.
