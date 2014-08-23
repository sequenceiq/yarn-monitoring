# constructor of timeboxes
timeboxes<-function()
{
	timebox<-list()
	class(timebox)<-"timeboxes"
	timebox
}

# this method adds boxes to a line in timeboxes
addBox.timeboxes<-function(data, line, box)
{
	if (is.null(data[[line]]))
		data[[line]]<-matrix(box, nrow=1, ncol=length(box))
      else
		data[[line]]<-rbind(data[[line]], box)
	data
}

#it shows the time boxes given the timebox data. The lineGroupNum is the number of groups,
# the linesPerGroup is the number of timeboxes in a group. The lineNameConv parameter can be used to 
# transform the names of groups (example had431->431)
plot.timeboxes<-function(data, lineGroupNum=length(data), linesPerGroup=1, sortGroups=TRUE, colors=c("green", "darkorange", "magenta", "blue"), ylim=c(0,lineGroupNum+0.5), lineNameConv=identity)
{
    cols=ncol(data[[1]])
  	for(i in 1:length(data))
  	{
		  data[[i]]<-matrix(data[[i]][sort.list(data[[i]][,1]), ], ncol=cols)
  	}
  	if (sortGroups)
 		data<-data[order(names(data))]
	minx=.Machine$double.xmax
	maxx=0
	for(n in 1:length(data))
	{
		mi<-min(data[[n]][data[[n]]>=0])
		if (minx > mi )
			minx <- mi
		ma<-max(data[[n]][data[[n]]>=0])
		if (maxx < ma )
			maxx <- ma		
	}
	plot(1, type="n", xlim=c(minx-0.5,maxx+0.5), ylim=ylim,  xlab="time (ms)",ylab="nodes")
	axis(4,at=(1:length(data))-0.5,labels=lineNameConv(names(data)))
	for(i in 1:lineGroupNum)
	{
		lines(c(minx-0.5,maxx+0.5),c(i,i))	
	}

	for(n in 1:length(data))
	{ 
		for(r in 1:nrow(data[[n]]))
		{
			for(c in 1:(ncol(data[[n]])-1))
			{
				col<-colors[c]				
				if ( data[[n]][r,c]!=-1 && data[[n]][r,c+1]!=-1)
				#if ( data[[n]][r,c+1]!=-1)
					rect(data[[n]][r,c],((n-1)*linesPerGroup+((r-1)%%linesPerGroup))/linesPerGroup,data[[n]][r,c+1],((n-1)*linesPerGroup+((r-1)%%linesPerGroup+1))/linesPerGroup, col=col)
			}
		}
	}
}
