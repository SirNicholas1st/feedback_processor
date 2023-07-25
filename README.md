# feedback_processor
WIP This repository contains a simple Flask application that serves as a customer feedback form. The submitted data is sent to a Kafka topic. The Kafka consumer, implemented as a Spark application, processes the data and writes it to a Cassandra table for further analysis.
