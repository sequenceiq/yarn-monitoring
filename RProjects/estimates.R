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