#!/bin/bash

/opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic retail_events \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic retail_events \
  --bootstrap-server localhost:9092