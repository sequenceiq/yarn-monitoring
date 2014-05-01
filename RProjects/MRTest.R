source("RProjects/JobHistory.R")
library("rjson")
library("Hmisc")
mrrun<-function(name, jobId, fromFile=FALSE)
{
	if ( fromFile)
	{
		run<-fromJSON(readLines(paste(jobId,".RData",sep="")))
	}
	else
	{
		run<-list(name=name)
		class(run)<-"mrrun"
		job<-getJob(jobId, "node02.gusgus.linux:19888")
		counters<-getTaskCounters(jobId, "node02.gusgus.linux:19888")
		run$job<-job
		run$counters<-counters
	}
	run
}

saveRun<-function(run)
{
	writeLines(toJSON(run), paste(run$name,".RData",sep=""))
}

mrruns<-function(runNames=NULL)
{
	result<-list()
	class(result)<-"mrruns"
	if (runVector != NULL )
	{
		for(i in 1:length(runNames))
		{
			run<-mrrun(runNames[i],fromFile=TRUE)
			result[[run$name]]<-run
		}
	}
	result
}

addRun<-function(mrruns, run)
{
	mrruns[[run$name]]<-run
	saveRun(run)
	mrruns
}
plotInputBytesRead<-function(mrruns)
{
	res<-vector()
	for(i in 1:length(mrruns))
	{
		res<-c(res, mean(mrruns[[i]]$counters$FileInputFormatCounter.BYTES_READ))
	}
	plot(res,type="l")
	res
}

plotElapsedMapTimesStat<-function(mrruns)
{
	means<-vector()
	mins<-vector()
	maxs<-vector()
	sds<-vector()
	for( i in 1:length(mrruns))
	{
		means<-c(means,mean(mrruns[[i]]$job$tasks$elapsedTime))
		mins<-c(mins,min(mrruns[[i]]$job$tasks$elapsedTime))
		maxs<-c(maxs,max(mrruns[[i]]$job$tasks$elapsedTime))
		sds<-c(sds,sd(mrruns[[i]]$job$tasks$elapsedTime))
	}	
#	par(fg="black")
	errbar(1:length(mrruns),means, maxs, mins, errbar.col="red", ylab="ms", xlab="runs", main="Mean, min, max elapsed times per run")		
 # par(fg="red")
	errbar(1:length(mrruns),means, means+sds, means-sds , lwd=2, add=TRUE, ylab="ms", xlab="runs", main="Mean, min, max elapsed times per run")
	#par(fg="black")
}
plotElapsedMapTimes<-function(mrruns)
{
	for( i in 1:length(mrruns))
	{
		if (i==1)
			plot(mrruns[[i]]$job$tasks$elapsedTime, type="l", xlab="tasks", ylab="elapsedTime")
		else
			lines(mrruns[[i]]$job$tasks$elapsedTime, col=i)
	}
}

plotInputRecordsProcessedPerSec<-function(mrruns)
{
  for( i in 1:length(mrruns))
  {
    attemptindices<-match(mrruns[[i]]$job$tasks$successfulAttempt,mrruns[[i]]$job$attempts$id)
    mapindices<-which(mrruns[[i]]$job$attempts$type[attemptindices]=="MAP")
    bytespersec<-mrruns[[i]]$counters$FileInputFormatCounter.BYTES_READ/mrruns[[i]]$job$attempts[mapindices]$elapsedTime
    if (i==1)
      plot(bytespersec, type="l")
    else
      lines(bytespersec, col=i)
  }
}

plotMeanInputBytesPerSecByNode<-function(mrruns, minmax=TRUE)
{
  result<-list()
  for( i in 1:length(mrruns))
  {
    attemptindices<-match(mrruns[[i]]$job$tasks$successfulAttempt,mrruns[[i]]$job$attempts$id)
    mapindices<-which(mrruns[[i]]$job$attempts$type[attemptindices]=="MAP")
    for( m in 1:length(mapindices))
    {
      node<-mrruns[[i]]$job$attempts$nodeHttpAddress[mapindices[m]]
      bytespersec<-1000*mrruns[[i]]$counters$FileInputFormatCounter.BYTES_READ[mapindices[m]]/(mrruns[[i]]$job$attempts$elapsedTime[mapindices[m]]*1024*1024)
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

plotMeanInputRecordsPerSecByNode<-function(mrruns, minmax=TRUE)
{
  result<-list()
  for( i in 1:length(mrruns))
  {
    attemptindices<-match(mrruns[[i]]$job$tasks$successfulAttempt,mrruns[[i]]$job$attempts$id)
    mapindices<-which(mrruns[[i]]$job$attempts$type[attemptindices]=="MAP")
    for( m in 1:length(mapindices))
    {
      node<-mrruns[[i]]$job$attempts$nodeHttpAddress[mapindices[m]]
      recordspersec<-1000*mrruns[[i]]$counters$TaskCounter.MAP_INPUT_RECORDS[mapindices[m]]/(mrruns[[i]]$job$attempts$elapsedTime[mapindices[m]])
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

plotMeanElapsedMapTimesByNode<-function(mrruns, taskType="MAP", minmax=TRUE)
{
	result<-list()
	for( i in 1:length(mrruns))
	{
		attemptindices<-match(mrruns[[i]]$job$tasks$successfulAttempt,mrruns[[i]]$job$attempts$id)
		mapindices<-which(mrruns[[i]]$job$attempts$type[attemptindices]==taskType)
		for( m in 1:length(mapindices))
		{
			node<-mrruns[[i]]$job$attempts$nodeHttpAddress[mapindices[m]]
			if ( is.null(result[[node]]))
				result[[node]]<-mrruns[[i]]$job$attempts$elapsedTime[mapindices[m]]
			else
				result[[node]]<-c(result[[node]],mrruns[[i]]$job$attempts$elapsedTime[mapindices[m]])
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
