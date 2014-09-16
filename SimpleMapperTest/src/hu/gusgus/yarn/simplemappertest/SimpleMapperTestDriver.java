package hu.gusgus.yarn.simplemappertest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class SimpleMapperTestDriver extends Configured implements Tool
{
  public int run(String[] origArgs) throws Exception
  {
    GenericOptionsParser parser = new GenericOptionsParser(origArgs);
    String args[] = parser.getRemainingArgs();
    if ( args.length < 3)
    {
    	System.err.println("Bad number of parameters.");
    	System.err.println("Usage: output numreduce numberOfOutputs collectKey inputs");
    	return -1;
    }
    JobConf conf = new JobConf(getConf(), SimpleMapperTestDriver.class);

    conf.setJobName("SimpleMapperTest");

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(SimpleMapperTestMapper.class);
    conf.setPartitionerClass(LinePartitioner.class);

    FileOutputFormat.setOutputPath(conf, new Path(args[0]));
	conf.setNumReduceTasks(Integer.valueOf(args[1]));
	conf.setInt("numberOfOutputs", Integer.valueOf(args[2]));
	conf.setBoolean("collectKey", Boolean.valueOf(args[3]));
	System.out.println("numberOfOutputs="+args[2]);
	System.out.println("collectKey="+args[3]);
	for( int i=4;i<args.length;++i)
	{
		addInputFiles(conf, FileSystem.get(URI.create(args[i]),conf),args[i]);
	}
    
    JobClient.runJob(conf);
    return 0;
  }

  public static void addInputFiles(JobConf conf, FileSystem fs, String dir) throws FileNotFoundException, IllegalArgumentException, IOException
  {
	  FileStatus[] statuses = fs.listStatus(new Path(dir));
	  if ( statuses != null)
	  {
		  for(int i=0;i<statuses.length;++i)
		  {
			  if (statuses[i].isDir())
				  addInputFiles(conf, statuses[i].getPath().getFileSystem(conf),statuses[i].getPath().toString());
			  else
				  FileInputFormat.addInputPath(conf, new Path(statuses[i].getPath().toString()));
		  }
	  	}
  }
  public static void main(String[] args) throws Exception
  {
    int ret = ToolRunner.run(new SimpleMapperTestDriver(), args);
    System.exit(ret);
  }
}
