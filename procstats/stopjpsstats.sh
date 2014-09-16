#!/bin/bash -e

for pid in $(ps -ef|grep getJpsStats|awk '{print $2}')
do
    kill $pid
done