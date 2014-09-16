package hu.gusgus.yarn.simplemappertest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class LinePartitioner implements Partitioner<LongWritable, Text> {
	@Override
	public int getPartition(LongWritable key, Text value, int numReducer) {
		
		return (int)((key.get()/16)%numReducer);
	}

	@Override
	public void configure(JobConf arg0) {
	}

}
