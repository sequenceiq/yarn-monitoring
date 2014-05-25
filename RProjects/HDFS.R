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
  ls.webhdfs(webhdfs, )
}