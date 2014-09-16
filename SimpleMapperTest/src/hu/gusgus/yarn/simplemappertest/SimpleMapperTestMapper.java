package hu.gusgus.yarn.simplemappertest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class SimpleMapperTestMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text>
{
	private int numberOfOutputs;
	private boolean collectKey;
  @Override
	public void configure(JobConf job) {
		
	  numberOfOutputs=job.getInt("numberOfOutputs", 0);
	  	collectKey = job.getBoolean("collectKey", false);
		super.configure(job);
	}

@Override
  public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter)
      throws IOException
  {
	for(int i=0;i<numberOfOutputs;++i)
	{
	   if ( collectKey )
		  output.collect(key, value);
	  else
		  output.collect(null, value);
	}
  }
}
