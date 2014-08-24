# This is a helper function that is used while loading the job from historyServer
transposeListOfLists <- function(listoflist)
{
  result<-list()
  for(i in 1:length(listoflist))
  {
    for(j in 1:length(listoflist[[i]]))
    {
      result[[names(listoflist[[i]][j])]]<-c(result[[names(listoflist[[i]][j])]],listoflist[[i]][[j]])
    }  
  }  
  result
}

cutNamesNode02<-function(name)
{
  substr(name,5,6)
}
cutNamesUnicum<-function(name)
{
  substr(name,4,6)
}