#!/usr/bin/env bash

# files=( "../benchmarking/files/3_xml_medium.zip" "../benchmarking/files/3_xml_big.zip" )
# for f in "${files[@]}"; do
for f in ../benchmarking/5.1-files/4-5.1-large-vipFeed-53-2020-02-11.xml; do
  for run in {1..5}; do
    echo "$f" >> benchmarking-5.1-large.txt
    { time lein run "$f" ; } 2>> benchmarking-5.1-large.txt
    echo "" >> benchmarking-5.1-large.txt
    echo "" >> benchmarking-5.1-large.txt

    psql -U lizstarin -d dataprocessor \
    -c "truncate table public.xml_tree_values;"
  done
done
