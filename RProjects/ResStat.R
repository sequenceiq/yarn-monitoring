library("rjson")
readResStat<-function(root, type="DataNode", subtype="net")
{
  resstat<-list()
  class(resstat)<-"resstat"
  resstat$name<-paste(type,subtype,sep="_")
  resstat$files<-sort(list.files(root,pattern=paste("node.*",type,".*",subtype,sep="")))
  if ( is.null(resstat$files))
    null
  resstat$data<-list()
  for(i in 1:length(resstat$files))
  {
    resstat$data[[i]]<-as.matrix(read.table(paste(root,resstat$files[[i]],sep="/"),header=FALSE,sep=" "))
  }
  resstat
}
readResStatsOfRun<-function( runname, root)
{
    resstats<-list()
    class(resstats)<-"resstats"
    resstats$runname<-paste("resstats_",runname,sep="")
    resstats$data$datanode_net<-readResStat(root, "DataNode","net")
    resstats$data$datanode_disk<-readResStat(root, "DataNode","disk")
    resstats$data$nodemanager_net<-readResStat(root, "NodeManager","net")
    resstats$data$nodemanager_disk<-readResStat(root, "NodeManager","disk")
    resstats$data$yarnchild_net<-readResStat(root, "YarnChild","net")
    resstats$data$yarnchild_disk<-readResStat(root, "YarnChild","disk")
  resstats
}

slice<-function(l, indices)
{
  j<-1
  result<-list()
  for(i in 1:length(indices))
  {
    result[[j]]<-l[[indices[i]]]
    j<-j+1
  }
  result
}
plot.resstat<-function(resstat, netin=TRUE, nodes=1:length(resstat$data))
{
  if (netin)
  {
    index<-2
    ylab<-"net in (bytes)"
  }
  else
  {
    index<-3
    ylab<-"net out (bytes)"
  }
  ymin<-0
  ymax<-0
  xmin<-resstat$data[[1]][1,1]
  xmax<-resstat$data[[1]][1,1]
  slicedData<-slice(resstat$data, nodes)
  files<-slice(resstat$files, nodes)
  transformedData<-list()
  for(i in 1:length(slicedData))
  {
    transformedData[[i]]<-transform.resstat(slicedData[[i]])
  }
  for(i in 1:length(transformedData))
  {
    ymin<-min(ymin,min(transformedData[[i]][,index]-transformedData[[i]][1,index]))
    ymax<-max(ymax,max(transformedData[[i]][,index]-transformedData[[i]][1,index]))
    xmin<-min(xmin,min(transformedData[[i]][,1]))
    xmax<-max(xmax,max(transformedData[[i]][,1]))
  }
  
  for(i in 1:length(transformedData))
  {
    if ( i==1)
      plot(transformedData[[i]][,1], transformedData[[i]][,index]-transformedData[[i]][1,index], type="l", xlim=c(xmin,xmax),ylim=c(ymin,ymax), xlab=files[i], ylab=ylab)
    else
      lines(transformedData[[i]][,1], transformedData[[i]][,index]-transformedData[[i]][1,index], col=i)
  }
}
plot.resstats<-function(resstats, nodename, input=TRUE)
{
  if (input)
    pos<-2
  else
    pos<-3
  ry<-vector()
  rx<-vector()
  leg<-vector()
  for(i in 1:length(resstats$data))
  {
    nodenames<-lapply(resstats$data[[i]]$files,FUN=function(x){strsplit(x,"_")[[1]][1]})
    if ( is.na(match(nodename,nodenames)))
      next
    leg<-c(leg,resstats$data[[i]]$name)
    #print(resstats$data[[i]]$name)
    #print(match(nodename,nodenames))
    #print(resstats$data[[i]]$data[match(nodename,nodenames)][[1]])
    transformed<-transform.resstat(resstats$data[[i]]$data[match(nodename,nodenames)][[1]])
    ry<-cbind(ry,range(transformed[,pos]))
    rx<-cbind(rx,range(transformed[,1]))
  }
  plot.new()
  plot.window(xlim=range(rx),ylim=range(ry))
  axis(1)
  axis(2)
  if (input)
    title(main="resources statistics in",xlab=nodename, ylab="MB")
  else
    title(main="resources statistics out",xlab=nodename, ylab="MB")
  legend(x="left", y="top", legend=leg, col=c(1,2,3,4,5,6),lty=1)
  for(i in 1:length(resstats$data))
  {
    nodenames<-lapply(resstats$data[[i]]$files,FUN=function(x){strsplit(x,"_")[[1]][1]})
    if ( is.na(match(nodename,nodenames)))
      next
    transformed<-transform.resstat(resstats$data[[i]]$data[match(nodename,nodenames)][[1]])
    lines(transformed[,1], transformed[,pos], col=i)
  }
}
transform.resstat<-function(data)
{
  #result<-matrix(0,nrow=nrow(data)-1, ncol=ncol(data))
  result<-matrix(0,nrow=nrow(data), ncol=ncol(data))
  #result[,1]<-data[1:nrow(data)-1,1]
  result[,1]<-data[1:nrow(data),1]
  #result[,2]<-(100000000(data[2:nrow(data),2]-data[1:nrow(data)-1,2]))/(data[2:nrow(data),1]-data[1:nrow(data)-1,1])
  #result[,2]<-(data[2:nrow(data),2]-data[1:nrow(data)-1,2])
  result[,2]<-(data[1:nrow(data),2]-data[1,2])/(1024*1024)
  #result[,3]<-(1000000000*(data[2:nrow(data),3]-data[1:nrow(data)-1,3]))/(data[2:nrow(data),1]-data[1:nrow(data)-1,1])
  #result[,3]<-(data[2:nrow(data),3]-data[1:nrow(data)-1,3])
  result[,3]<-(data[1:nrow(data),3]-data[1,3])/(1024*1024)
  result
}
