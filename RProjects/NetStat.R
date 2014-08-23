readNetStats<-function(root, type="DataNode", subtype="net")
{
  netstat<-list()
  class(netstat)<-"netstat"
  netstat$files<-sort(list.files(root,pattern=paste("node.*",type,".*",subtype,sep="")))
  netstat$data<-list()
  for(i in 1:length(netstat$files))
  {
    netstat$data[[i]]<-as.matrix(read.table(paste(root,netstat$files[[i]],sep="/"),header=FALSE,sep=" "))
  }
  netstat
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
plotNetStats<-function(netstat, netin=TRUE, nodes=1:length(netstat$data))
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
  xmin<-netstat$data[[1]][1,1]
  xmax<-netstat$data[[1]][1,1]
  slicedData<-slice(netstat$data, nodes)
  files<-slice(netstat$files, nodes)
  transformedData<-list()
  for(i in 1:length(slicedData))
  {
    transformedData[[i]]<-transformNetStat(slicedData[[i]])
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
transformNetStat<-function(data)
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
