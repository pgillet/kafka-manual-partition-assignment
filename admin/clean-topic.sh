#!/bin/bash

KAFKA_HOME=${KAFKA_HOME:-/home/pascal/Workbench/Projects/PDGS/kafka_2.12-2.2.0}

${KAFKA_HOME}/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic my-topic
${KAFKA_HOME}/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic __consumer_offsets
${KAFKA_HOME}/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic __transaction_state

# Delete log files
rm /tmp/pdgs-logs/log-*.txt