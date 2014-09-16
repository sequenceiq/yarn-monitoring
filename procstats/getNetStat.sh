#!/bin/bash
pid=$1

line=$(cat /proc/$pid/net/netstat | grep 'IpExt: ' | tail -n 1)
time=$(date +%s%N)
in=$(echo $line|awk '{ print $8  }')
out=$(echo $line|awk '{ print $9  }')
echo $time $in $out
