## Shell script to run these feeds:

```
#!/usr/bin/env bash

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
```

## 3_csv_small.zip

real	0m18.825s
user	0m14.490s
sys	0m1.214s

real	0m19.196s
user	0m14.937s
sys	0m1.249s

real	0m19.299s
user	0m14.917s
sys	0m1.344s

real	0m19.058s
user	0m15.074s
sys	0m1.302s

real	0m19.655s
user	0m15.705s
sys	0m1.275s


## 3_csv_medium.zip

real	0m24.838s
user	0m20.144s
sys	0m2.108s

real	0m25.382s
user	0m20.714s
sys	0m2.068s

real	0m25.165s
user	0m20.357s
sys	0m2.137s

real	0m25.612s
user	0m20.572s
sys	0m2.155s

real	0m24.780s
user	0m20.025s
sys	0m2.041s


## 3_csv_big.zip

real	1m12.989s
user	1m0.919s
sys	0m8.805s

real	1m12.849s
user	1m0.670s
sys	0m8.686s

real	1m14.076s
user	1m1.963s
sys	0m8.882s

real	1m13.364s
user	1m1.189s
sys	0m8.797s

real	1m13.951s
user	1m1.706s
sys	0m8.941s


## 3_xml_small.zip

real	0m44.998s
user	0m39.918s
sys	0m2.653s

real	0m45.172s
user	0m39.970s
sys	0m2.644s

real	0m44.789s
user	0m40.141s
sys	0m2.542s

real	0m45.927s
user	0m40.872s
sys	0m2.739s

real	0m44.323s
user	0m40.034s
sys	0m2.548s


## 3_xml_medium.zip

real	3m50.721s
user	3m38.473s
sys	0m13.870s

real	4m0.631s
user	3m48.045s
sys	0m14.540s

real	3m57.937s
user	3m45.715s
sys	0m14.742s

real	4m0.332s
user	3m50.957s
sys	0m14.380s

real	4m0.173s
user	3m49.905s
sys	0m14.522s


## 3_xml_big.zip

real	21m39.901s
user	20m31.760s
sys	1m27.791s

real	22m13.528s
user	20m54.850s
sys	1m30.280s

real	22m4.420s
user	20m27.985s
sys	1m27.254s

real	22m58.915s
user	21m25.387s
sys	1m35.363s

real	22m9.673s
user	20m37.476s
sys	1m27.612s
