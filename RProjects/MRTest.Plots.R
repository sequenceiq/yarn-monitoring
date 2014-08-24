source("RProjects/MRTest.R")
source("RProjects/MRRun.R")

plotInputBytesRead.mrtest<-function(mrtest)
{
  res<-vector()
  for(i in 1:length(mrtest))
  {
    res<-c(res, mean(getInputBytesOfMapTasks.mrrun(mrtest[[i]])/(1024*1024)))
  }
  plot(res,type="l", xlab="run",ylab="mbytes")
}

plotElapsedMapTimesStat.mrtest<-function(mrtest, add=FALSE)
{
  means<-vector()
  mins<-vector()
  maxs<-vector()
  sds<-vector()
  for( i in 1:length(mrtest))
  {
    means<-c(means,mean(getElapsedTimesOfTasks.mrrun(mrtest[[i]])))
    mins<-c(mins,min(getElapsedTimesOfTasks.mrrun(mrtest[[i]])))
    maxs<-c(maxs,max(getElapsedTimesOfTasks.mrrun(mrtest[[i]])))
    sds<-c(sds,sd(getElapsedTimesOfTasks.mrrun(mrtest[[i]])))
  }  
  #	par(fg="black")
  errbar(1:length(mrtest),means, maxs, mins, errbar.col="red", ylab="ms", xlab="runs", main="Mean, min, max elapsed times per run", add=add)		
  # par(fg="red")
  errbar(1:length(mrtest),means, means+sds, means-sds , lwd=2, add=TRUE, ylab="ms", xlab="runs", main="Mean, min, max elapsed times per run")
  #par(fg="black")
}


plotInputRecordsProcessedPerSec.mrtest<-function(mrtest)
{
  for( i in 1:length(mrtest))
  {
    bytespersec<-getInputBytesOfMapTasks.mrrun(mrtest[[i]])/getElapsedTimesOfMapTasks.mrrun(mrtest[[i]])
    if (i==1)
      plot(bytespersec, type="l")
    else
      lines(bytespersec, col=i)
  }
}

plotElapsedTimesByRun.mrtest<-function(mrtest)
{
  res<-vector()
  for( i in 1:length(mrtest))
  {
    res<-c(res,getElapsedTime.mrrun(mrtest[[i]]))
  }
  barplot(res, names.arg=1:length(mrtest),xlab="run",ylab="ms")  
}

plotMeanInputRecordsProcessedPerSecondByRun.mrtest<-function(mrtest)
{
  res<-vector()
  for( i in 1:length(mrtest))
  {
    res<-c(res, mean(1000*getInputRecordsOfMapTasks.mrrun(mrtest[[i]])/getElapsedTimesOfMapTasks.mrrun(mrtest[[i]])))
  }
  barplot(res, names.arg=1:length(mrtest), xlab="run",ylab="records")
  
}

plotMeanBytesProcessedPerSecondByRun.mrtest<-function(mrtest)
{
  res<-vector()
  for( i in 1:length(mrtest))
  {
    res<-c(res, mean((1000/(1024*1024))*getInputBytesOfMapTasks.mrrun(mrtest[[i]])/getElapsedTimesOfMapTasks.mrrun(mrtest[[i]])))
  }
  barplot(res, names.arg=1:length(mrtest), xlab="run",ylab="mbytes")
  
}

plotMeanInputBytesPerSecByNode.mrtest<-function(mrtest, minmax=TRUE)
{
  result<-list()
  for( i in 1:length(mrtest))
  {
    res<-getInputBytesReadByNodesAndTasks.mrrun(mrtest[[i]])
    result<-merge_items(res, result)
  }
  print(result)
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
}

plotMeanInputRecordsPerSecByNode.mrtest<-function(mrtest, minmax=TRUE)
{
  result<-list()
  for( i in 1:length(mrtest))
  {
    res<-getInputRecordsReadByNodesAndTasks.mrrun(mrtest[[i]])
    result<-merge_items(res, result)
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

merge_items<-function(items1, items2)
{
  names<-names(items1)
  for(i in 1:length(items1))
  {
    if ( is.null(items2[[names[i]]]))
      items2[[names[i]]]<-items1[[i]]
    else
      items2[[names[i]]]<-c(items2[[names[i]]],items1[[i]]) 
  }
  items2
}

plotMeanElapsedMapTimesByNode.mrtest<-function(mrtest, taskType="MAP", minmax=TRUE)
{
  result<-list()
  for( i in 1:length(mrtest))
  {
    res<-getMapElapsedTimesByNodesAndTasks.mrrun(mrtest[[i]])
    result<-merge_items(res, result)
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
}

plotJobElapsedTimes.mrtest<-function(mrtest)
{
  res<-vector()
  for(i in 1:length(mrtest))
  {
    res<-c(res,getElapsedTime.mrrun(mrtest[[i]]))
  }
  barplot(res, names.arg=1:length(mrtest), width=rep(1,length(mrtest)), space=0)
}

plotJobStartTimes.mrtest<-function(mrtest)
{
  res<-vector()
  for(i in 1:length(mrtest))
  {
    res<-c(res,mrtest[[i]]$job$job$startTime-mrtest[[1]]$job$job$startTime)
  }
  barplot(res, names.arg=1:length(mrtest), width=rep(1,length(mrtest)), space=0)
}
