package arbitaryutils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/*
 * 01. We have to use a custom mapper.
 * 02. The default reducer is used
 * 03. We have to set the no of reducers to 1 as we want overall sorting
 * 
 * Sort the file based on a particular column
 * 
 * 
hadoop jar arbitaryutils-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
arbitaryutils.Sorting \
-Dmapred.reduce.tasks=1 -Ddfs.replication=1 \
/user/amishra/synthetic.csv /user/amishra/synthetic_output
 */
public class Sorting extends Configured implements Tool{

	
	public static void main(String[] args) throws Exception {
		int rc=ToolRunner.run(new Configuration(),new Sorting(), args);
		System.exit(rc);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		
		Configuration conf= getConf();
		
		
		
		Job job = new Job(conf);
		job.setJarByClass(Sorting.class);
		job.setJobName("Sorting");
		
		job.setMapperClass(SrtMapper.class);
		
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		job.setReducerClass(Reducer.class);
		//job.setNumReduceTasks(5);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		
		return job.waitForCompletion(true) ? 0 : 1;
		
		
		
	}
	
	
	
	
	public static class SrtMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			
			String [] k = value.toString().split("\t");
			int nk = Integer.parseInt(k[0]);
			
			context.write(new IntWritable(nk) , new Text(k[1]));
		}
		
	}

}
