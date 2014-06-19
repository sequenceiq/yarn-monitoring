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
  blocks<-vector()
  time<-max(getElapsedTimesOfMapTasks.mrrun(run))
  mapperNum=0
  for(s in 1:length(sizes))
  {
    newblocks<-c(rep(blockSize,floor(sizes[[s]]/blockSize)),sizes[[s]]-floor(sizes[[s]]/blockSize)*blockSize)
    blocks<-c(blocks,newblocks)
  }
  calcRunningTimes(sort(time*blocks/blockSize,decreasing=TRUE), freeSlots)
}

calcRunningTimes<-function(times, slots)
{
  nodes<-rep(0,slots)
  for(t in 1:length(times))
  {
    i<-which.min(nodes)
    nodes[i]<-nodes[i]+times[t]
  }
  nodes
}
