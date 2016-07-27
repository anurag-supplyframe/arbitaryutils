package arbitaryutils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * 01. Check what are the default compression params 
 * 02. No reducers are used. Not even the default reducer.
 * 
 *Compress a file into bzip2
hadoop jar arbitaryutils-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
arbitaryutils.CompressToBz2 \
-Ddfs.replication=1 \
/user/amishra/L2reln /user/amishra/L2reln_bz2
 */

public class CompressToBz2 extends Configured implements Tool{
	
	
	public static void main(String[] args) throws Exception {
		int rc= ToolRunner.run(new Configuration(), new CompressToBz2(), args);
		System.exit(rc);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf= getConf();
		
		
		System.out.println("mapred.output.compress"+conf.get("mapred.output.compress"));
		System.out.println("mapred.output.compression.type"+conf.get("mapred.output.compression.type"));
		System.out.println("mapred.output.compression.codec"+conf.get("mapred.output.compression.codec"));
		System.out.println("mapred.compress.map.output"+conf.get("mapred.compress.map.output"));
		System.out.println("mapred.map.output.compression.codec"+conf.get("mapred.map.output.compression.codec"));
		
		//compress to bzip2
	    conf.set("mapred.output.compress", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
	    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
	    
	    conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec",	"org.apache.hadoop.io.compress.BZip2Codec");
		
		
		
		Job job = new Job(conf);
		job.setJarByClass(CompressToBz2.class);
		job.setJobName("CompressToBz2");
		
		job.setMapperClass(CmprsBz2Mapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		

		
		job.setNumReduceTasks(0);
		
		//FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
		//FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	public static class CmprsBz2Mapper extends Mapper<LongWritable, Text , Text , NullWritable>{
		
		private Text out = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			out.set(value);
			context.write(out, NullWritable.get());
			
		}
	}

}
