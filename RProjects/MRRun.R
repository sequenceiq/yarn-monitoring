source("RProjects/JobHistory.R")
library("rjson")

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

getElapsedTime.mrrun<-function(run)
{
  run$job$job$finishTime-run$job$job$startTime
}

getElapsedTimesOfTasks.mrrun<-function(run)
{
  run$job$tasks$elapsedTime
}


getElapsedTimesOfMapTasks.mrrun<-function(run)
{
  attemptindices<-match(run$job$tasks$successfulAttempt,run$job$attempts$id)
  mapindices<-which(run$job$attempts$type[attemptindices]=="MAP")
  run$job$attempts[mapindices]$elapsedTime
}

getElapsedTimesOfReduceTasks.mrrun<-function(run)
{
  attemptindices<-match(run$job$tasks$successfulAttempt,run$job$attempts$id)
  mapindices<-which(run$job$attempts$type[attemptindices]=="REDUCE")
  run$job$attempts[mapindices]$elapsedTime
}

getInputRecordsOfMapTasks.mrrun<-function(run)
{
  run$counters$TaskCounter.MAP_INPUT_RECORDS
}


getOutputRecordsOfMapTasks.mrrun<-function(run)
{
  run$counters$TaskCounter.MAP_OUTPUT_RECORDS
}

getInputBytesOfMapTasks.mrrun<-function(run)
{
  run$counters$FileInputFormatCounter.BYTES_READ
}

getInputBytesReadByNodesAndTasks.mrrun<-function(run)
{
  result<-list()
  attemptindices<-match(run$job$tasks$successfulAttempt,run$job$attempts$id)
  mapindices<-which(run$job$attempts$type[attemptindices]=="MAP")
  for( m in 1:length(mapindices))
  {
    node<-run$job$attempts$nodeHttpAddress[mapindices[m]]
    bytespersec<-run$counters$FileInputFormatCounter.BYTES_READ[mapindices[m]]
    if ( is.null(result[[node]]))
      result[[node]]<-bytespersec
    else
      result[[node]]<-c(result[[node]],bytespersec)
  }
  result
}

getInputRecordsReadByNodesAndTasks.mrrun<-function(run)
{
  result<-list()
  attemptindices<-match(run$job$tasks$successfulAttempt,run$job$attempts$id)
  mapindices<-which(run$job$attempts$type[attemptindices]=="MAP")
  for( m in 1:length(mapindices))
  {
    node<-run$job$attempts$nodeHttpAddress[mapindices[m]]
    bytespersec<-run$counters$TaskCounter.MAP_INPUT_RECORDS[mapindices[m]]
    if ( is.null(result[[node]]))
      result[[node]]<-bytespersec
    else
      result[[node]]<-c(result[[node]],bytespersec)
  }
  result
}

getMapElapsedTimesByNodesAndTasks.mrrun<-function(run)
{
  result<-list()
  attemptindices<-match(run$job$tasks$successfulAttempt,run$job$attempts$id)
  mapindices<-which(run$job$attempts$type[attemptindices]=="MAP")
  #print(mapindices)
  for( m in 1:length(mapindices))
  {
    node<-run$job$attempts$nodeHttpAddress[mapindices[m]]
    elapsed<-run$job$attempts$elapsedTime[mapindices[m]]
    #print(node)
    #print(elapsed)
    if ( is.null(result[[node]]))
      result[[node]]<-elapsed
    else
      result[[node]]<-c(result[[node]],elapsed)
  }
  result
}
