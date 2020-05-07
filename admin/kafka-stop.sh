#!/bin/bash

KAFKA_HOME=${KAFKA_HOME:-/home/pascal/Workbench/Projects/PDGS/kafka_2.12-2.2.0}

DIR=$(dirname "$0")

# Stop Kakfa brokers
${KAFKA_HOME}/bin/kafka-server-stop.sh ${DIR}/config/server-2.properties
${KAFKA_HOME}/bin/kafka-server-stop.sh ${DIR}/config/server-1.properties
${KAFKA_HOME}/bin/kafka-server-stop.sh ${DIR}/config/server-0.properties

# Stop Zookeeper
${KAFKA_HOME}/bin/zookeeper-server-stop.sh