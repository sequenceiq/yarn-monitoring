source("RProjects/MRRun.R")
library("rjson")
library("Hmisc")

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

getElapsedTimesOfRuns.mrtest<-function(test)
{
  res<-vector()
  for( t in 1:length(test))
  {
    res<-c(res,getElapsedTime.mrrun(test[[t]]))
  }
  res
}

getElapsedTimesOfTasks.mrtest<-function(test)
{
  res<-vector()
  for( t in 1:length(test))
  {
    res<-c(res,getElapsedTimesOfTasks.mrrun(test[[t]]))
  }
  res
}

getRecordsReadByRuns.mrtest<-function(test)
{
  res<-vector()
  for( t in 1:length(test))
  {
    res<-c(res,getInputRecordsOfMapTasks.mrrun(test[[t]]))
  }
  res
}

getInputRecordsOfMapTasks.mrtest<-function(test)
{
  res<-vector()
  for( t in 1:length(test))
  {
    res<-c(res,getInputRecordsOfMapTasks.mrrun(test[[t]]))
  }
  res
}