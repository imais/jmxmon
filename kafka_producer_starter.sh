#!/bin/bash

i=1
while read line
do
  echo "# Starting Kafka producer ($i) with throughput = $line "
  java -cp .:./lib/* setup.core -r -t $line &
  i=$((i+1))
done < "${1:-/dev/stdin}"

