# Real Time Analytics MicroService Challenge - Daniel Tateishi

- **Producer** 
    - exposes a REST endpoint and publishes messages to the incoming.user_activity topic.
- **Consumer** 
    - Implement a Kafka consumer that reads messages from the `incoming.user_activity` topic.
    - Perform data transformations, such as counting the number of users and the number of `page_view` activities.
    - Write aggregated results to a PostgreSQL database. Only one table is needed/recommended.
