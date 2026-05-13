"""
------------------------------------------------------------
 Script Name:        flink_app.py

 Description:        This script implements a real-time
                     stream processing application using
                     Apache Flink and Kafka. It reads payment
                     events from a Kafka topic, filters
                     high-value payments (amount > 500), and
                     writes the results to another Kafka topic.
                     The filtered payments are also printed
                     to the console for debugging purposes.

                     The pipeline demonstrates:
                     - Integration of PyFlink with Kafka
                     - Data parsing, filtering, and transformation
                     - Real-time event processing

 Author:             Your Name
 Created On:         08.05.2026
 Last Modified:      13.05.2026

 Version:            1.0
 Python Version:     3.10+
 Dependencies:
     - pyflink
     - kafka-python (for testing producer)
     - json
     - dataclasses

 Notes:
     - Kafka broker must be running locally on port 9092.
     - Kafka topics used:
         - payments (input)
         - filtered-payments (output)
     - Ensure Flink Kafka connector JAR version matches Kafka broker version.
     - Internet connection not required; runs locally.
------------------------------------------------------------
"""
import json
from dataclasses import dataclass

from pyflink.common import Types, Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema
)

@dataclass
class Payment:
    payment_id: str
    user_id: str
    merchant_id: str
    amount: float
    currency: str
    payment_time: str

def parse_payment(json_str):
    data = json.loads(json_str)
    return Payment(
        payment_id=data.get("payment_id", "unknown"),
        user_id=data.get("user_id", "unknown"),
        merchant_id=data.get("merchant_id", "unknown"),
        amount=float(data.get("amount", 0.0)),
        currency=data.get("currency", "unknown"),
        payment_time=data.get("payment_time", "unknown")
    )

def filter_high_amount(payment):
    return payment.amount > 500

def convert_payment(payment):
    return json.dumps({
        "payment_id": payment.payment_id,
        "amount": payment.amount
    })

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("payments") \
        .set_group_id("flink-consumer-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "kafka-source"
    )

    processed = (
        stream
        .map(parse_payment)
        .filter(filter_high_amount)
        .map(convert_payment, output_type=Types.STRING())
    )

    processed.print()


    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("filtered-payments")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    processed.sink_to(kafka_sink)
    env.execute("Payments Flink App")

if __name__ == "__main__":
    main()