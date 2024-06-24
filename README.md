This repository contains two main components.

**1. KafkaStream** 🌡️:
- Aggregates temperature values from the Kafka broker over a 5-minute window.
- Computes the average temperature within each window and publishes it to a topic.

**2. KafkaConsumer** 🌿:
- Retrieves CO2 concentration values from the Kafka broker.
- Publishes messages based on specified threshold values.
