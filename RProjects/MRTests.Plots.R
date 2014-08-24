source("RProjects/MRTest.R")

plotJobElapsedTimesMeansForTests.mrtests<-function(names, ...)
{
  resTasks<-vector()
  resJobs<-vector()
  tests<-list(...)
  print(names(tests[[1]]))
  for( t in 1:length(tests))
  {
    resJobs<-c(resJobs,mean(getElapsedTimesOfRuns.mrtest(tests[[t]])))
    resTasks<-c(resTasks,mean(getElapsedTimesOfTasks.mrtest(tests[[t]])))
  }
  plot(resJobs,type="b", ylab="ms",xlab="test", ylim=c(min(resJobs,resTasks),max(resJobs,resTasks)), axes=FALSE)
  lines(resTasks,col="red")
  points(resTasks,col="red")
  axis(side=1,at=c(1,2,3,4),labels=names)
  axis(side=2)
}

plotJobElapsedTimesMaxsForTests.mrtests<-function(names, ...)
{
  resTasks<-vector()
  resJobs<-vector()
  tests<-list(...)
  for( t in 1:length(tests))
  {
    resTasks<-c(resTasks,max(getElapsedTimesOfTasks.mrtest(tests[[t]])))
    resJobs<-c(resJobs,max(getElapsedTimesOfRuns.mrtest(tests[[t]])))
  }
  plot(resJobs,type="b", ylab="ms",xlab="test", ylim=c(min(resJobs,resTasks),max(resJobs,resTasks)), axes=FALSE)
  lines(resTasks,col="red")
  points(resTasks,col="red")
  axis(side=1,at=c(1,2,3,4),labels=names)
  axis(side=2)
}
