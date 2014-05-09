source("RProjects/JobHistory.R")
library("rjson")
library("Hmisc")

mrrun<-function(name, jobURL=NULL, dir=".", save=FALSE)
{
	if ( is.null(jobURL) )
	{
		run<-fromJSON(readLines(paste(dir,"/",name,".RData",sep="")))
	}
	else
	{
		run<-list(name=name)
		class(run)<-"mrrun"
    urlVector<-strsplit(jobURL,"/")[[1]]
    historyServerUrl<-paste(urlVector[1:(length(urlVector)-1)],collapse="/")    
		jobId<-urlVector[length(urlVector)]
		job<-getJob(jobId, historyServerUrl)
		counters<-getTaskCounters(jobId, historyServerUrl)
		run$job<-job
		run$counters<-counters
    if ( save )
		  writeLines(toJSON(run), paste(dir,"/",name,".RData",sep=""))
	}
	run
}

createRunsAndTest<-function(runNumbers, jobIds, historyServer, historyServerId, dir="runs", save=TRUE)
{
  for( i in 1:length(runNumbers))
  {
    #print(sprintf("run%d",runNumbers[i]))
    #print(sprintf("%s/%s_%04d",historyServer,historyServerId,jobIds[i]))
    run<-mrrun(sprintf("run%d",runNumbers[i]),sprintf("%s/%s_%04d",historyServer,historyServerId,jobIds[i]),dir=dir,save=save)
  }
  test<-mrtest(paste("run",runNumbers,sep=""),dir=dir)
  test
}

mrtest<-function(runNames=NULL, dir=".")
{
	result<-list()
	class(result)<-"mrtest"
	for(i in 1:length(runNames))
	{
		run<-mrrun(runNames[i], dir=dir)
		result[[run$name]]<-run
	}
	result
}

addRun<-function(mrtest, mrrun)
{
  mrtest[[mrrun$name]]<-mrrun
  mrtest
}

plotInputBytesRead<-function(mrtest)
{
	res<-vector()
	for(i in 1:length(mrtest))
	{
		res<-c(res, mean(mrtest[[i]]$counters$FileInputFormatCounter.BYTES_READ)/(1024*1024))
	}
	plot(res,type="l", xlab="run",ylab="mbytes")
	res
}



plotElapsedMapTimesStat<-function(mrtest, add=FALSE)
{
	means<-vector()
	mins<-vector()
	maxs<-vector()
	sds<-vector()
	for( i in 1:length(mrtest))
	{
		means<-c(means,mean(mrtest[[i]]$job$tasks$elapsedTime))
		mins<-c(mins,min(mrtest[[i]]$job$tasks$elapsedTime))
		maxs<-c(maxs,max(mrtest[[i]]$job$tasks$elapsedTime))
		sds<-c(sds,sd(mrtest[[i]]$job$tasks$elapsedTime))
	}	
#	par(fg="black")
	errbar(1:length(mrtest),means, maxs, mins, errbar.col="red", ylab="ms", xlab="runs", main="Mean, min, max elapsed times per run", add=add)		
 # par(fg="red")
	errbar(1:length(mrtest),means, means+sds, means-sds , lwd=2, add=TRUE, ylab="ms", xlab="runs", main="Mean, min, max elapsed times per run")
	#par(fg="black")
}

plotElapsedMapTimes<-function(mrtest)
{
	for( i in 1:length(mrtest))
	{
		if (i==1)
			plot(mrtest[[i]]$job$tasks$elapsedTime, type="l", xlab="tasks", ylab="elapsedTime")
		else
			lines(mrtest[[i]]$job$tasks$elapsedTime, col=i)
	}
}

plotInputRecordsProcessedPerSec<-function(mrtest)
{
  for( i in 1:length(mrtest))
  {
    attemptindices<-match(mrtest[[i]]$job$tasks$successfulAttempt,mrtest[[i]]$job$attempts$id)
    mapindices<-which(mrtest[[i]]$job$attempts$type[attemptindices]=="MAP")
    bytespersec<-mrtest[[i]]$counters$FileInputFormatCounter.BYTES_READ/mrtest[[i]]$job$attempts[mapindices]$elapsedTime
    if (i==1)
      plot(bytespersec, type="l")
    else
      lines(bytespersec, col=i)
  }
}

plotElapsedTimesByRun<-function(mrtest)
{
  res<-vector()
  for( i in 1:length(mrtest))
  {
    res<-c(res,mrtest[[i]]$job$job$finishTime-mrtest[[i]]$job$job$startTime)
  }
  barplot(res, names.arg=1:length(mrtest),xlab="run",ylab="ms")  
}

plotMeanInputRecordsProcessedPerSecondByRun<-function(mrtest)
{
  res<-vector()
  for( i in 1:length(mrtest))
  {
    attemptindices<-match(mrtest[[i]]$job$tasks$successfulAttempt,mrtest[[i]]$job$attempts$id)
    mapindices<-which(mrtest[[i]]$job$attempts$type[attemptindices]=="MAP")
    res<-c(res, mean(1000*mrtest[[i]]$counters$TaskCounter.MAP_INPUT_RECORDS/mrtest[[i]]$job$attempts[mapindices]$elapsedTime))
  }
  barplot(res, names.arg=1:length(mrtest), xlab="run",ylab="records")
  
}

plotMeanBytesProcessedPerSecondByRun<-function(mrtest)
{
  res<-vector()
  for( i in 1:length(mrtest))
  {
    attemptindices<-match(mrtest[[i]]$job$tasks$successfulAttempt,mrtest[[i]]$job$attempts$id)
    mapindices<-which(mrtest[[i]]$job$attempts$type[attemptindices]=="MAP")
    res<-c(res, mean((1000/(1024*1024))*mrtest[[i]]$counters$FileInputFormatCounter.BYTES_READ/mrtest[[i]]$job$attempts[mapindices]$elapsedTime))
  }
  barplot(res, names.arg=1:length(mrtest), xlab="run",ylab="mbytes")
  
}

plotMeanInputBytesPerSecByNode<-function(mrtest, minmax=TRUE)
{
  result<-list()
  for( i in 1:length(mrtest))
  {
    attemptindices<-match(mrtest[[i]]$job$tasks$successfulAttempt,mrtest[[i]]$job$attempts$id)
    mapindices<-which(mrtest[[i]]$job$attempts$type[attemptindices]=="MAP")
    for( m in 1:length(mapindices))
    {
      node<-mrtest[[i]]$job$attempts$nodeHttpAddress[mapindices[m]]
      bytespersec<-1000*mrtest[[i]]$counters$FileInputFormatCounter.BYTES_READ[mapindices[m]]/(mrtest[[i]]$job$attempts$elapsedTime[mapindices[m]]*1024*1024)
      if ( is.null(result[[node]]))
        result[[node]]<-bytespersec
      else
        result[[node]]<-c(result[[node]],bytespersec)
    }
  }
  means<-vector()
  mins<-vector()
  maxs<-vector()
  sds<-vector()
  res<-result[sort(names(result))]

  for( n in 1:length(res))
  {
    means<-c(means, mean(res[[n]]))
    mins<-c(mins, min(res[[n]]))
    maxs<-c(maxs, max(res[[n]]))
    sds<-c(sds,sd(res[[n]]))
  }
  if ( minmax)
    errbar(names(res),means, maxs, mins, ylab="mbytes", xlab="nodes", main="Mean, min, max elapsed times per node")
  else
    errbar(names(res),means, means+sds, means-sds, ylab="mbytes", xlab="nodes", main="Mean +- stdev elapsed times per node")
  res
}

plotMeanInputRecordsPerSecByNode<-function(mrtest, minmax=TRUE)
{
  result<-list()
  for( i in 1:length(mrtest))
  {
    attemptindices<-match(mrtest[[i]]$job$tasks$successfulAttempt,mrtest[[i]]$job$attempts$id)
    mapindices<-which(mrtest[[i]]$job$attempts$type[attemptindices]=="MAP")
    for( m in 1:length(mapindices))
    {
      node<-mrtest[[i]]$job$attempts$nodeHttpAddress[mapindices[m]]
      recordspersec<-1000*mrtest[[i]]$counters$TaskCounter.MAP_INPUT_RECORDS[mapindices[m]]/(mrtest[[i]]$job$attempts$elapsedTime[mapindices[m]])
      if ( is.null(result[[node]]))
        result[[node]]<-recordspersec
      else
        result[[node]]<-c(result[[node]],recordspersec)
    }
  }
  means<-vector()
  mins<-vector()
  maxs<-vector()
  sds<-vector()
  res<-result[sort(names(result))]
  
  for( n in 1:length(res))
  {
    means<-c(means, mean(res[[n]]))
    mins<-c(mins, min(res[[n]]))
    maxs<-c(maxs, max(res[[n]]))
    sds<-c(sds,sd(res[[n]]))
  }
  if ( minmax)
    errbar(names(res),means, maxs, mins, ylab="records", xlab="nodes", main="Mean, min, max elapsed times per node")
  else
    errbar(names(res),means, means+sds, means-sds, ylab="records", xlab="nodes", main="Mean +- stdev elapsed times per node")
  res
}

plotMeanElapsedMapTimesByNode<-function(mrtest, taskType="MAP", minmax=TRUE)
{
	result<-list()
	for( i in 1:length(mrtest))
	{
		attemptindices<-match(mrtest[[i]]$job$tasks$successfulAttempt,mrtest[[i]]$job$attempts$id)
		mapindices<-which(mrtest[[i]]$job$attempts$type[attemptindices]==taskType)
		for( m in 1:length(mapindices))
		{
			node<-mrtest[[i]]$job$attempts$nodeHttpAddress[mapindices[m]]
			if ( is.null(result[[node]]))
				result[[node]]<-mrtest[[i]]$job$attempts$elapsedTime[mapindices[m]]
			else
				result[[node]]<-c(result[[node]],mrtest[[i]]$job$attempts$elapsedTime[mapindices[m]])
		}
	}
	means<-vector()
	mins<-vector()
	maxs<-vector()
  sds<-vector()
  res<-result[sort(names(result))]
  for( n in 1:length(res))
  {
    means<-c(means, mean(res[[n]]))
    mins<-c(mins, min(res[[n]]))
    maxs<-c(maxs, max(res[[n]]))
    sds<-c(sds,sd(res[[n]]))
  }
  if ( minmax)
    errbar(names(res),means, maxs, mins, ylab="ms", xlab="nodes", main="Mean, min, max elapsed times per node")
  else
    errbar(names(res),means, means+sds, means-sds, ylab="ms", xlab="nodes", main="Mean +- stdev elapsed times per node")
  res
}

plotJobElapsedTimes<-function(mrtest)
{
  res<-vector()
  for(i in 1:length(mrtest))
  {
    res<-c(res,mrtest[[i]]$job$job$finishTime-mrtest[[i]]$job$job$startTime)
  }
  barplot(res, names.arg=1:length(mrtest), width=rep(1,length(mrtest)), space=0)
}

plotJobStartTimes<-function(mrtest)
{
  res<-vector()
  for(i in 1:length(mrtest))
  {
    res<-c(res,mrtest[[i]]$job$job$startTime-mrtest[[1]]$job$job$startTime)
  }
  barplot(res, names.arg=1:length(mrtest), width=rep(1,length(mrtest)), space=0)
}

getJobElapsedTimesMean<-function(mrtest)
{
  res<-vector()
  for(i in 1:length(mrtest))
  {
    res<-c(res,mrtest[[i]]$job$job$finishTime-mrtest[[i]]$job$job$startTime)
  }
  mean(res)
}

getTaskElapsedTimesMean<-function(mrtest)
{
  res<-vector()
  for( i in 1:length(mrtest))
  {
    attemptindices<-match(mrtest[[i]]$job$tasks$successfulAttempt,mrtest[[i]]$job$attempts$id)
    mapindices<-which(mrtest[[i]]$job$attempts$type[attemptindices]=="MAP")
    res<-c(res,mrtest[[i]]$job$tasks$elapsedTime)
  }
  mean(res)
}

getTasksBytesProcessedMean<-function(mrtest)
{
  res<-vector()
  for( i in 1:length(mrtest))
  {
    attemptindices<-match(mrtest[[i]]$job$tasks$successfulAttempt,mrtest[[i]]$job$attempts$id)
    mapindices<-which(mrtest[[i]]$job$attempts$type[attemptindices]=="MAP")
    res<-c(res, (1000/(1024*1024))*mrtest[[i]]$counters$FileInputFormatCounter.BYTES_READ/mrtest[[i]]$job$attempts[mapindices]$elapsedTime)
  }
  mean(res)
}
getTasksInputRecordsProcessedMean<-function(mrtest)
{
  res<-vector()
  for( i in 1:length(mrtest))
  {
    attemptindices<-match(mrtest[[i]]$job$tasks$successfulAttempt,mrtest[[i]]$job$attempts$id)
    mapindices<-which(mrtest[[i]]$job$attempts$type[attemptindices]=="MAP")
    res<-c(res, 1000*mrtest[[i]]$counters$TaskCounter.MAP_INPUT_RECORDS/mrtest[[i]]$job$attempts[mapindices]$elapsedTime)
  }
  mean(res)
}

plotJobElapsedTimesMeansForTests<-function(names, ...)
{
  resTasks<-vector()
  resJobs<-vector()
  tests<-list(...)
  print(names(tests[[1]]))
  for( t in 1:length(tests))
  {
    resTasks<-c(resTasks,getTaskElapsedTimesMean(tests[[t]]))
    resJobs<-c(resJobs,getJobElapsedTimesMean(tests[[t]]))
  }
  plot(resJobs,type="b", ylab="ms",xlab="test", ylim=c(min(resJobs,resTasks),max(resJobs,resTasks)), axes=FALSE)
  lines(resTasks,col="red")
  points(resTasks,col="red")
  axis(side=1,at=c(1,2,3,4),labels=names)
  axis(side=2)
}