library("rjson")
library("RCurl")
webhdfs<-function(url)
{
  url
}
ls.webhdfs<-function(webhdfs, path)
{
  url<-paste(webhdfs,"/webhdfs/v1",path, "/?op=LISTSTATUS",sep="")
  fromJSON(getURL(url,httpheader = c(Accept="application/json")))$FileStatuses$FileStatus
}
lsFiles.webhdfs<-function(webhdfs, path, recursive=FALSE)
{
  result<-list()
  statuses<-ls.webhdfs(webhdfs, path)
  for( s in 1:length(statuses))
  {
    if ( recursive && statuses[[s]]$type=="DIRECTORY")
      result<-c(result, lsFiles.webhdfs(webhdfs, paste(path,"/",statuses[[s]]$pathSuffix,sep=""), TRUE))
    if ( statuses[[s]]$type=="FILE" )
    {
      statuses[[s]]$pathSuffix<-paste(path,statuses[[s]]$pathSuffix,sep="/")
      result<-c(result, statuses[s])
    }
  }
  result
}