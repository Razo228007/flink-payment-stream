Real-Time Payment Processing Pipeline (Kafka + PyFlink)

This project demonstrates a real-time data streaming pipeline using Apache Kafka and Apache Flink (PyFlink). It simulates payment transactions, processes them in real time, filters high-value payments, and sends the results to another Kafka topic. The system consists of a Python producer that generates random payment events, a Flink streaming application that processes and filters the data, and a Docker-based Kafka environment for local setup.

The producer script generates random payment events with fields such as payment_id, user_id, merchant_id, amount, currency, and payment_time. These events are sent to a Kafka topic called "payments" every second. The data is serialized in JSON format and continuously streamed into Kafka.

The Flink application reads data from the "payments" topic using a Kafka source connector. It parses incoming JSON strings into a Payment dataclass, filters out transactions where the amount is less than or equal to 500, and converts the remaining high-value transactions back into JSON format containing only payment_id and amount. The processed data is printed to the console and also written to another Kafka topic called "filtered-payments" using a Kafka sink connector.

The system uses Docker Compose to run Kafka and Zookeeper locally. Kafka is exposed on port 9092 and Zookeeper on port 2181. This allows the entire pipeline to run locally without any external dependencies.

To run the project, first start Kafka and Zookeeper using docker-compose up -d. Then create the required Kafka topics: "payments" and "filtered-payments". After that, run producer.py to start generating payment events. Finally, run flink_app.py to start the Flink streaming job that processes the data in real time.

The input message format is a JSON object containing payment_id, user_id, merchant_id, amount, currency, and payment_time. The output format contains only payment_id and amount for transactions where amount is greater than 500.

The filtering logic is simple: only payments with amount > 500 are passed through the pipeline. All other events are ignored.

Kafka must be running before starting both the producer and Flink application. The producer generates one event per second. The Flink job runs in streaming mode and processes data continuously.

This project demonstrates real-time stream processing, Kafka integration, and Flink data pipelines in a simple but practical way.

Author: Razmik Bazeyan
