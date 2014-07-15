readNetStats<-function(root, type="DataNode")
{
  files<-sort(list.files(root,pattern=paste("node.*",type,".*",sep="")))
  data<-list()
  for(i in 1:length(files))
  {
    data[[i]]<-as.matrix(read.table(paste(root,files[[i]],sep="/"),header=FALSE,sep=" "))
  }
  data
}
plotNetStats<-function(data, inorout=TRUE)
{
  if (inorout)
    index<-2
  else
    index<-3
  ymin<-0
  ymax<-0
  xmin<-data[[1]][1,1]
  xmax<-data[[1]][1,1]
  for(i in 1:length(data))
  {
    ymin<-min(ymin,min(data[[i]][,index]-data[[i]][1,index]))
    ymax<-max(ymax,max(data[[i]][,index]-data[[i]][1,index]))
    xmin<-min(xmin,min(data[[i]][,1]))
    xmax<-max(xmax,max(data[[i]][,1]))
  }
  
  for(i in 1:length(data))
  {
    if ( i==1)
      plot(data[[i]][,1], data[[i]][,index]-data[[i]][1,index], type="l", xlim=c(xmin,xmax),ylim=c(ymin,ymax))
    else
      lines(data[[i]][,1], data[[i]][,index]-data[[i]][1,index], col=i)
  }
}
