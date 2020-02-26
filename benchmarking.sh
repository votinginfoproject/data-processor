#!/usr/bin/env bash

# files=( "../benchmarking/files/3_xml_medium.zip" "../benchmarking/files/3_xml_big.zip" )
# for f in "${files[@]}"; do
for f in ../benchmarking/files/*; do
  for run in {1..5}; do
    echo "$f" >> benchmarking-4.txt
    { time lein run "$f" ; } 2>> benchmarking-4.txt
    echo "" >> benchmarking-4.txt
    echo "" >> benchmarking-4.txt

    psql -U lizstarin -d dataprocessor \
    -c "truncate table public.xml_tree_values;"
  done
done
