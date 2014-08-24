source("RProjects/MRRun.R")

# This function plots lines for each mapper horizontally. The horizontal axis is the time in ms
plotMapTasksTimes.mrrun <- function(mrrun)
{
  indices<-which(mrrun$job$tasks$type=="MAP")
  plotTasksTimesData.mrrun(mrrun$job, indices)
}

# This function plots lines for each red+ucer horizontally. The horizontal axis is the time in ms
plotReduceTasksTimes.mrrun <- function(mrrun)
{
  indices<-which(mrrun$job$tasks$type=="REDUCE")
  plotTasksTimesData.mrrun(mrrun$job, indices)
}

# This function plots lines for each mapper and reducer horizontally. The horizontal axis is the time in ms
plotTasksTimesData.mrrun <- function(mrrun, indices=1:length(mrrun$job$tasks$startTime))
{
  times<-cbind(mrrun$job$tasks$startTime[indices],mrrun$job$tasks$finishTime[indices])
  sortedtimes<-times[order(times[,1]),]
  toplot<-sortedtimes-sortedtimes[1,1]
  for(i in 1:nrow(toplot))
  {
    if ( i==1)
      plot(rbind(c(toplot[i,1],i),c(toplot[i,2],i)),type="l", xlim=c(0,max(toplot)),ylim=c(0,nrow(toplot)), xlab="time (ms)", ylab="task num")
    else
      lines(rbind(c(toplot[i,1],i),c(toplot[i,2],i)))
  }
}

# This function plots the number of active mappers at every time point when this number changes
plotActiveMappersNumData.mrrun <- function(mrrun, replot=FALSE, minTime=NULL)
{
  nums <- getActiveTasksNumData(mrrun$job, which(mrrun$job$tasks$type=="MAP"), minTime)
  plotActiveTasksNum(nums, replot=replot)
}

# This function plots the number of active reducers at every time point when this number changes
plotActiveReducersNumData.mrrun <- function(mrrun, replot=FALSE, minTime=NULL)
{
  nums <- getActiveTasksNumData(mrrun$job, which(mrrun$job$tasks$type=="REDUCE"), minTime)
  plotActiveTasksNum(nums, replot=replot)
  
}

# This function plot the number of active tasks (mappers or reducers as two graphs, reducers with phases) 
# at every time point when this number changes
plotActiveMRTasksNumData.mrrun <- function(mrrun, relative=TRUE)
{
  if (relative)
  {
    indices<-which(mrrun$job$tasks$type=="MAP")
    offset <- min(mrrun$job$tasks$startTime[indices])
  }
  else
    offset<-0
  
  numsM <- getActiveTasksNumData(mrrun$job, which(job$tasks$type=="MAP"),offset)
  numsSP <- getActiveShufflePhaseReducerNumData(mrrun$job, offset)
  numsMP <- getActiveMergePhaseReducerNumData(mrrun$job, offset)
  numsRP <- getActiveReducePhaseReducerNumData(mrrun$job, offset)
  
  yrange<-range(c(numsM[,2], numsSP[,2], numsMP[,2], numsRP[,2]))
  xrange<-range(c(numsM[,1], numsSP[,1], numsMP[,1], numsRP[,1]))
  plotActiveTasksNum(numsM, replot=FALSE, col="green", xlim=xrange, ylim=yrange)
  plotActiveTasksNum(numsSP, replot=TRUE, col="darkorange")
  plotActiveTasksNum(numsMP, replot=TRUE, col="magenta")
  plotActiveTasksNum(numsRP, replot=TRUE, col="blue")
}

# This function plot the number of active reducers in different phases (shuffle, merge, reduce)at every time point when this number changes
plotActiveReduceTasksNumDetailedData.mrrun <- function(mrrun, relative=TRUE)
{
  if (relative)
  {
    indices<-which(mrrun$job$tasks$type=="REDUCE")
    offset <- min(mrrun$job$tasks$startTime[indices])
  }
  else
    offset<-0
  numsSP <- getActiveShufflePhaseReducerNumData(mrrun$job, offset )
  numsMP <- getActiveMergePhaseReducerNumData(mrrun$job, offset )
  numsRP <- getActiveReducePhaseReducerNumData(mrrun$job, offset )
  yrange<-range(c(numsSP[,2], numsMP[,2], numsRP[,2]))
  xrange<-range(c(numsSP[,1], numsMP[,1], numsRP[,1]))
  plotActiveTasksNum(numsSP, replot=FALSE, col="darkorange", xlim=xrange, ylim=yrange)
  plotActiveTasksNum(numsMP, replot=TRUE, col="magenta")
  plotActiveTasksNum(numsRP, replot=TRUE, col="blue")
}

# This helper function does the actual plotting or replotting
plotActiveTasksNum <- function(nums, replot=FALSE, col="black", xlim=NULL, ylim=NULL)
{
  if (replot)
    points(nums, type="s", xlab="time (ms)",ylab="number of tasks", col=col)
  else
    plot(nums, type="s", xlab="time (ms)",ylab="number of tasks", col=col, xlim=xlim, ylim=ylim)
}

########################### HELPER FUNCTIONS ####################
# This function return the number of active tasks (mappers or reducers depend on indices) at every time point when this number changes
getActiveTasksNumData <- function(job, indices=1:length(job$tasks$startTime), minTime=NULL)
{
  times<-rbind(cbind(job$tasks$startTime[indices],rep(1,length(indices))),
               cbind(job$tasks$finishTime[indices],rep(-1,length(indices))))
  nums <- calcNums(times, minTime)
  nums
}

# This function return the number of active reducers in shuffle at every time point when this number changes
getActiveShufflePhaseReducerNumData <- function(job, minTime=NULL)
{
  indices<-which(job$tasks$type=="REDUCE")
  times<-rbind(cbind(job$tasks$startTime[indices],rep(1,length(indices))),
               cbind(job$attempts$shuffleFinishTime,rep(-1,length(job$attempts$shuffleFinishTime))))
  nums <- calcNums(times, minTime)
  nums
}
# This function return the number of active reducers in merge at every time point when this number changes
getActiveMergePhaseReducerNumData <- function(job, minTime=NULL)
{
  times<-rbind(cbind(job$attempts$shuffleFinishTime,rep(1,length(job$attempts$shuffleFinishTime))),
               cbind(job$attempts$mergeFinishTime,rep(-1,length(job$attempts$mergeFinishTime))))
  nums <- calcNums(times, minTime)
  nums
}
# This function return the number of active reducers in merge at every time point when this number changes
getActiveReducePhaseReducerNumData <- function(job, minTime=NULL)
{
  indices<-which(job$tasks$type=="REDUCE")
  times<-rbind(cbind(job$attempts$mergeFinishTime,rep(1,length(job$attempts$mergeFinishTime))),
               cbind(job$tasks$finishTime[indices],rep(-1,length(indices))))
  nums <- calcNums(times, minTime)
  nums
}
# This function calculates the number of tasks if times are given as (time, +-1) pairs
calcNums <- function(times, minTime=NULL)
{
  sortedtimes<-times[order(times[,1]),]
  toplot<-sortedtimes
  if ( is.null(minTime) )
    toplot[,1]<-sortedtimes[,1]-sortedtimes[1,1]
  else
    toplot[,1]<-sortedtimes[,1]-minTime
  nums<-matrix(nrow=0,ncol=2)
  num<-0
  for(i in 1:nrow(toplot))
  {
    num<-num+toplot[i,2]
    nums<-rbind(nums,c(toplot[i,1],num))
  }
  nums
}
