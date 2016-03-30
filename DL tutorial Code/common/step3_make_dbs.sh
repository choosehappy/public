#!/bin/bash

for kfoldi in {1..5}
do
echo "doing fold $kfoldi"
~/caffe/build/tools/convert_imageset -shuffle -backend leveldb   ./ ../train_w32_${kfoldi}.txt ../DB_train_${kfoldi} &
~/caffe/build/tools/convert_imageset -shuffle -backend leveldb   ./ ../test_w32_${kfoldi}.txt ../DB_test_${kfoldi} &
done




FAIL=0
for job in `jobs -p`
do
    echo $job
    wait $job || let "FAIL+=1"
done




echo "number failed: $FAIL"

cd ../

for kfoldi in {1..5}
do
echo "doing fold $kfoldi"
~/caffe/build/tools/compute_image_mean DB_train_${kfoldi} DB_train_w32_${kfoldi}.binaryproto -backend leveldb  &
done



