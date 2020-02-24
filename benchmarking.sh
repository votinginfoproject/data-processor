#!/usr/bin/env bash

for f in ../benchmarking/files/*; do
  for run in {1..5}; do
    echo "$f" >> benchmarking.txt
    { time lein run "$f" ; } 2>> benchmarking.txt
    echo "" >> benchmarking.txt
    echo "" >> benchmarking.txt
  done
done
