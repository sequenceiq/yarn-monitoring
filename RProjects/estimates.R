source("RProjects/WebHDFS.R")

inputSizes<-function(webhdfs, path)
{
  result<-vector()
  statuses<-lsFiles.webhdfs(webhdfs, path, TRUE)
  for(s in 1:length(statuses))
  {
    result<-c(result, statuses[[s]]$length)
  }
  result
}
estimateElapsedTime<-function(webhdfs, path, run, freeSlots, blockSize)
{
  sizes<-inputSizes(webhdfs, path)
  avgTime<-mean(getElapsedTimesOfTasks.mrrun(run))
  mapperNum=0
  for(s in 1:length(sizes))
  {
    mapperNum<-mapperNum+ceiling(sizes[[s]]/blockSize)
  }
  avgTime*(ceiling(mapperNum/freeSlots))
}
