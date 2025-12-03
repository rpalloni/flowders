### Flow of orders - flowders
A simple project where a producer generates random orders data and send them to a kafka cluster.
There are 3 brokers and one 'orders' topic with 10 partitions, one for each customer (partition key).
Spark cluster with 2 workers reads the kafka streaming from topic and calculates aggregations per customer every minute.

Input example:

{"order_id":4444,"customer_id":CUS_60,"amount":608.21,"timestamp":"2025-09-22T19:27:05.425770"}
{"order_id":3560,"customer_id":CUS_60,"amount":204.67,"timestamp":"2025-09-22T19:27:11.433860"}
{"order_id":7517,"customer_id":CUS_58,"amount":322.45,"timestamp":"2025-09-22T19:27:20.438745"}
{"order_id":6987,"customer_id":CUS_55,"amount":866.6,"timestamp":"2025-09-22T19:27:26.443795"}
{"order_id":3520,"customer_id":CUS_52,"amount":215.33,"timestamp":"2025-09-22T19:27:36.450808"}
{"order_id":1097,"customer_id":CUS_52,"amount":668.06,"timestamp":"2025-09-22T19:27:41.456560"}
{"order_id":2549,"customer_id":CUS_50,"amount":616.46,"timestamp":"2025-09-22T19:27:48.461685"}
{"order_id":2662,"customer_id":CUS_57,"amount":757.36,"timestamp":"2025-09-22T19:27:51.465676"}
{"order_id":8961,"customer_id":CUS_54,"amount":327.33,"timestamp":"2025-09-22T19:27:57.471678"}

Output example:

+------------------------------------------+-----------+-------------+-------------+
|window                                    |customer_id|client_orders|client_amount|
+------------------------------------------+-----------+-------------+-------------+
|{2025-09-22 19:27:00, 2025-09-22 19:28:00}|CUS_58     |1            |322.45       |
|{2025-09-22 19:27:00, 2025-09-22 19:28:00}|CUS_57     |1            |757.36       |
|{2025-09-22 19:27:00, 2025-09-22 19:28:00}|CUS_52     |2            |883.39       |
|{2025-09-22 19:27:00, 2025-09-22 19:28:00}|CUS_55     |1            |866.60       |
|{2025-09-22 19:27:00, 2025-09-22 19:28:00}|CUS_54     |1            |327.33       |
|{2025-09-22 19:27:00, 2025-09-22 19:28:00}|CUS_60     |2            |812.88       |
|{2025-09-22 19:27:00, 2025-09-22 19:28:00}|CUS_50     |1            |616.46       |
+------------------------------------------+-----------+-------------+-------------+

### UI
Kafka UI: localhost:8888
Spark App UI: localhost:4040
Spark Master UI: localhost:8090

### Run project
`docker compose up --build`

# kafka CLI
You can use any of the broker as bootstrap broker to access the cluster:
`docker compose exec kafka1 bash`

`/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka1:9092`
`/opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server kafka1:9092`
`/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --partition 5  --from-beginning`