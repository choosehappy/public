#!/bin/bash

for li in {1..4}
do
echo "doing level $li"
cd subs_${li}
for kfoldi in {1..5}
do
echo "doing fold $kfoldi"
~/caffe/build/tools/convert_imageset -shuffle -backend leveldb   ./ train_w32_${kfoldi}.txt ../DB_train_${kfoldi}_${li} &
~/caffe/build/tools/convert_imageset -shuffle -backend leveldb   ./ test_w32_${kfoldi}.txt ../DB_test_${kfoldi}_${li} &
done
cd ..
done


FAIL=0

for job in `jobs -p`
do
    echo $job
    wait $job || let "FAIL+=1"
done




echo "number failed: $FAIL"



for li in {1..4}
do
echo "doing level $li"
for kfoldi in {1..5}
do
echo "doing fold $kfoldi"
~/caffe/build/tools/compute_image_mean DB_train_${kfoldi}_${li} DB_train_w32_${kfoldi}_${li}.binaryproto -backend leveldb  &
done
done


