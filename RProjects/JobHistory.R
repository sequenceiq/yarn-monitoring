library("rjson")
library("RCurl")
source("RProjects/Utils.R")

# It returns all the data of a job with specified jobId and specified history server
# The historyServer is in format "hostname:port"
getJob <- function(jobId, historyServer)
{
	job <- list()
	url<-paste(historyServer,"/ws/v1/history/mapreduce/jobs/",jobId, sep="")
	job$job <- fromJSON(getURL(url,httpheader = c(Accept="application/json")))$job
	url<-paste(historyServer,"/ws/v1/history/mapreduce/jobs/",jobId,"/tasks", sep="")
	job$tasks <- transposeListOfLists(fromJSON(getURL(url,httpheader = c(Accept="application/json")))$tasks$task)
	attempts<-list()
	for(i in 1:length(job$tasks$successfulAttempt))
	{
		attempt<-job$tasks$successfulAttempt[i]
		url<-paste(historyServer,"/ws/v1/history/mapreduce/jobs/",jobId,"/tasks/",job$tasks$id[i], "/attempts/",attempt,sep="")
		attempts[[i]]<-fromJSON(getURL(url,httpheader = c(Accept="application/json")))$taskAttempt
	}
	job$attempts<-transposeListOfLists(attempts)
	class(job)<-"mrjob"
	job
}

#It returns the task counters of a job for a given job and historyserver
getTaskCounters <- function(jobId, historyServer)
{
	result<-list()
	url<-paste(historyServer,"/ws/v1/history/mapreduce/jobs/",jobId,"/tasks", sep="")
	tasks <- transposeListOfLists(fromJSON(getURL(url,httpheader = c(Accept="application/json")))$tasks$task)
	for(i in 1:length(tasks$successfulAttempt))
	{
		attempt<-tasks$successfulAttempt[i]
		url<-paste(historyServer,"/ws/v1/history/mapreduce/jobs/",jobId,"/tasks/",tasks$id[i], "/attempts/",attempt,"/counters",sep="")
		counter<-fromJSON(getURL(url,httpheader = c(Accept="application/json")))$jobTaskAttemptCounters$taskAttemptCounterGroup
		for( j in 1:length(counter))
		{
			groups<-counter[[j]]
			for(g in 1:length(groups$counter))
			{
				key<-paste(tail(strsplit(groups$counterGroupName,"\\.")[[1]],n=1),groups$counter[[g]]$name,sep=".");
				value<-groups$counter[[g]]$value
				if (is.null(result[[key]]))
					result[[key]]<-c(value)
				else
					result[[key]]<-c(result[[key]],value)
			}
		}
	}
	result
}



