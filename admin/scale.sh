#!/bin/bash

kubectl scale deployment/poc-kafka-consumer-deployment --replicas=$1


