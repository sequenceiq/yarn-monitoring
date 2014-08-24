source("RProjects/JobHistory.R")
source("RProjects/TimeBoxes.R")
library("rjson")

# It constructs an mrrun for the given name and jobURl, the result can be saved in a directory
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

# it returns the elapsed time of the mrrun
getElapsedTime.mrrun<-function(run)
{
  run$job$job$finishTime-run$job$job$startTime
}

#it returns the elapsed times of the mrrun
getElapsedTimesOfTasks.mrrun<-function(run)
{
  run$job$tasks$elapsedTime
}

# it returns the elapsed times of map tasks for the run
getElapsedTimesOfMapTasks.mrrun<-function(run)
{
  attemptindices<-match(run$job$tasks$successfulAttempt,run$job$attempts$id)
  mapindices<-which(run$job$attempts$type[attemptindices]=="MAP")
  run$job$attempts[mapindices]$elapsedTime
}

# it returns the elapsed times of reduce tasks for the given run
getElapsedTimesOfReduceTasks.mrrun<-function(run)
{
  attemptindices<-match(run$job$tasks$successfulAttempt,run$job$attempts$id)
  mapindices<-which(run$job$attempts$type[attemptindices]=="REDUCE")
  run$job$attempts[mapindices]$elapsedTime
}

# it return the number of input records of the map tasks for the given run
getInputRecordsOfMapTasks.mrrun<-function(run)
{
  run$counters$TaskCounter.MAP_INPUT_RECORDS
}

# it return the number of output records of the map tasks for the given run
getOutputRecordsOfMapTasks.mrrun<-function(run)
{
  run$counters$TaskCounter.MAP_OUTPUT_RECORDS
}

# it return the number of input bytes of the map tasks for the given run
getInputBytesOfMapTasks.mrrun<-function(run)
{
  run$counters$FileInputFormatCounter.BYTES_READ
}

# it return the number of input bytes of the map tasks by nodes for the given run
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

# it return the number of input bytes of the map tasks by nodes for the given run
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

# it returns the elapsed times of tasks by nodes for the given run
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
#it creates time box data that can be shown using the TimeBoxes.R functions
createTimeBoxData.mrrun <- function(mrrun, relative=FALSE)
{
  result<-timeboxes()
  attemptindices<-match(mrrun$job$tasks$successfulAttempt,mrrun$job$attempts$id)
  mapindices<-which(mrrun$job$attempts$type[attemptindices]=="MAP")
  reduceindices<-which(mrrun$job$attempts$type[attemptindices]=="REDUCE")
  if (relative)
    minstart<-min(mrrun$job$attempts$startTime)
  else
    minstart<-0
  for(i in 1:length(mapindices))
  {
    node<-mrrun$job$attempts$nodeHttpAddress[mapindices[i]]
    result<-addBox.timeboxes(result, node, c(mrrun$job$attempts$startTime[mapindices[i]]-minstart,mrrun$job$attempts$finishTime[mapindices[i]]-minstart,-1,-1,-1))
  }
  if ( length(reduceindices)>0 )
  {
    for(i in 1:length(reduceindices))
    {
      node<-mrrun$job$attempts$nodeHttpAddress[reduceindices[i]]  
      result<-addBox.timeboxes(result, node, c(-1, mrrun$job$attempts$startTime[reduceindices[i]]-minstart,mrrun$job$attempts$shuffleFinishTime[i]-minstart,mrrun$job$attempts$mergeFinishTime[i]-minstart,mrrun$job$attempts$finishTime[reduceindices[i]]-minstart))
    }
  }
  result
}