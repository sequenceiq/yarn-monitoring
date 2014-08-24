source("RProjects/MRRun.R")
library("rjson")
library("Hmisc")

# it load the run defined by jobIds and historyServer. It can also save the loaded data
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

# it create and mrtest instance
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

# it adds a run to mrtest and returns the mrtest
addRun<-function(mrtest, mrrun)
{
  mrtest[[mrrun$name]]<-mrrun
  mrtest
}

# it returns the elapsed times of mrruns in the test
getElapsedTimesOfRuns.mrtest<-function(test)
{
  res<-vector()
  for( t in 1:length(test))
  {
    res<-c(res,getElapsedTime.mrrun(test[[t]]))
  }
  res
}

# it returns the ealpsed times of all the tasks in the mrruns in the test
getElapsedTimesOfTasks.mrtest<-function(test)
{
  res<-vector()
  for( t in 1:length(test))
  {
    res<-c(res,getElapsedTimesOfTasks.mrrun(test[[t]]))
  }
  res
}

# it returns the mean elapsed times of runs in test
getMeanElapsedTimesOfRuns.mrtest<-function(test)
{
  res<-vector()
  for( t in 1:length(test))
  {
    res<-c(res,mean(getElapsedTimesOfTasks.mrrun(test[[t]])))
  }
  res
}

# it returns number of records read by map tasks for all the runs in the test
getRecordsReadByRuns.mrtest<-function(test)
{
  res<-vector()
  for( t in 1:length(test))
  {
    res<-c(res,getInputRecordsOfMapTasks.mrrun(test[[t]]))
  }
  res
}
