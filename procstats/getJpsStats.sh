while [[ 1 ]];
do
    i=0
    for data in $(jps|grep -v Jps);
    do
	if [[ $i  == 0 ]];
	then
	    let i=$i+1
	    pid=$data
	else
	    i=0
	    procname=$data
#	    echo $pid $procname
	    ./getNetStat.sh $pid >> $HOST"_"$procname"_"$pid"_net"
	    ./getDiskStat.sh $pid >> $HOST"_"$procname"_"$pid"_disk"
	fi
    done
    sleep 1
done

