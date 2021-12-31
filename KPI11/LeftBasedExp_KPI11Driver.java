package com.ramesh.hranalysis;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Sales breakdown by store
 * Handle bad records (use multipleOutputs APIs)
 */

public final class LeftBasedExp_KPI11Driver
{
	 public final static void main(String[] args) throws Exception
		{
			args = new String[] { 
					"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/9.HR_Analysis/input",
					"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/9.HR_Analysis/Output_Data_KPI11/"};
					 
					/* delete the output directory before running the job */
					FileUtils.deleteDirectory(new File(args[1])); 
					 
					if (args.length != 2) {
					System.err.println("Please specify the input and output path");
					System.exit(-1);
					}
					
					System.setProperty("hadoop.home.dir","/home/hadoop/work/hadoop-3.1.2");
		
		final Configuration conf = new Configuration();

		final Job job = new Job(conf, "HR data Analysis");
		job.setJarByClass(LeftBasedExp_KPI11Driver.class);
		job.setMapperClass(LeftBasedExp_KPI11Mapper.class);
		job.setReducerClass(LeftBasedExp_KPI11Reducer.class); 
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path input=new Path(args[0]);
	    Path output=new Path(args[1]);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output); 
		
		System.exit(job.waitForCompletion(true)? 0:1); 
	}
}
