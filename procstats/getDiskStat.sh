#!/bin/bash -e

pid=$1
r=$(cat /proc/$pid/io | grep rchar|awk '{print $2}')
w=$(cat /proc/$pid/io | grep wchar|awk '{print $2}')
time=$(date +%s%N)
echo $time $r $w
