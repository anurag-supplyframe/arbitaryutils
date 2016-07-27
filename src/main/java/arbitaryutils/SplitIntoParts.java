package arbitaryutils;



import java.io.IOException;
import java.util.Iterator;

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
 *01. we are using the system provided default mapper and then use a custom reducer
 *02. how to set the log dir of your choice
 * 
 * 
 * use of this class.
 *Gzip file is not splittable. So when you upload a gigantic file into hdfs.
 *A single mapper will process the input file which is bad.
 *So we would like to split the file into mutiple gzip of bzip2 file based on the parameters passed
 *output.codec.type either pass GZIP or BZ2. What ever output you want
 *mapred.reduce.tasks no of reducer tasks which is same as no of output files
 *
 
 
yarn jar arbitaryutils-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
arbitaryutils.SplitIntoParts \
-Dmapred.reduce.tasks=10 -Ddfs.replication=3 \
-Doutput.codec.type=BZ2 \
/user/amishra/fff_pairwise_man2.bz2 /user/amishra/fff_pairwise_man2
 */

public class SplitIntoParts extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		String logDir = System.getProperty("hadoop.log.dir");
		System.out.println(logDir);
//		System.setProperty("hadoop.log.dir", "/home/amishra/hadooplogs");
//		logDir = System.getProperty("hadoop.log.dir");
//		System.out.println(logDir);
		
		int rc  = ToolRunner.run(new Configuration(), new SplitIntoParts(), args);
		System.exit(rc);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		
		Configuration conf= getConf();
		
		
		conf.set("mapred.output.compress", "true");
	    conf.set("mapred.output.compression.type", "BLOCK");
	    
	    String outputCodecType = conf.get("output.codec.type");
	    if("BZ2".equals(outputCodecType)){
	    	conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
	    }else if("GZ".equals(outputCodecType)){ 
	    	conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
	    }else{
	    	conf.set("mapred.output.compress", "false");
	    }
	    
		
	    conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.map.output.compression.codec",	"org.apache.hadoop.io.compress.SnappyCodec");
		
		Job job = new Job(conf);
		job.setJarByClass(SplitIntoParts.class);
		job.setJobName("SplitIntoParts");
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setReducerClass(SplitIntoPartsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
		
		
	}
	
	
	
	public static class SplitIntoPartsReducer extends Reducer<LongWritable, Text, Text, NullWritable>{
		
		
		@Override
		protected void reduce(LongWritable key, Iterable<Text> vals,
				Reducer<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			Iterator<Text> itr = vals.iterator();
			while(itr.hasNext()){
				Text text = itr.next();
				context.write(text, NullWritable.get());
			}
			
			
		}
	}

}


