plotTimeBoxes<-function(data, nodeNum=21, slotsPerNode=4)
{
	minx=.Machine$integer.max
	maxx=0
	for(n in 1:length(data))
	{
		mi<-min(data[[n]])
		if (minx > mi )
			minx <- mi
		ma<-max(data[[n]])
		if (maxx < ma )
			maxx <- ma		
	}
	plot(1, type="n", xlim=c(minx-0.5,maxx+0.5),ylim=c(0,nodeNum*slotsPerNode+0.5))
	for(n in 1:length(data))
	{
		for(r in 1:nrow(data[[n]]))
		{
			for(c in 1:3)
			{
				if ( data[[n]][r,3]==0)
					col="green"
				else	if ( c==1)
					col="darkorange"
				else if ( c==2)
					col="magenta"
				else if (c==3)
					col="blue"
				if ( data[[n]][r,c+1]!=0)
					rect(data[[n]][r,c],(n-1)*slotsPerNode+((r-1)%%slotsPerNode),data[[n]][r,c+1],(n-1)*slotsPerNode+((r-1)%%slotsPerNode+1), col=col)
			}
		}
	}
}

data<-list()
#data[[1]]<-rbind(c(10,20,0,0),c(10,34,0,0),c(30,44,57,65),c(30,55,75,82))

data[[1]]<-rbind(c(10,20,0,0),c(10,34,0,0),c(30,44,57,65),c(30,55,75,82),c(40,70,0,0),c(42,72,0,0))
data[[2]]<-rbind(c(12,22,0,0),c(15,23,0,0),c(40,55,65,74),c(51,64,77,85))
data[[3]]<-rbind(c(18,32,0,0))
plotTimeBoxes(data)