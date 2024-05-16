#! /bin/bash

echo "" > o.txt
for i in {0..50}; 
do 
  echo $i >> o.txt
  go test -run 3D >> o.txt; 
  echo "" >> o.txt
done
