package com.ramesh.hranalysis;

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

public final class DeptSalaryBaseLeftDriver
{
	public final static void main(final String[] args) throws Exception
	{
		final Configuration conf = new Configuration();

		final Job job = new Job(conf, "HR data Analysis");
		job.setJarByClass(DeptSalaryBaseLeftDriver.class);
		job.setMapperClass(DeptSalaryBaseLeftMapper.class);
		job.setReducerClass(DeptSalaryBaseLeftReducer.class); 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
		FileInputFormat.addInputPath(job, new Path("/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/HR_Analysis/input"));
		FileOutputFormat.setOutputPath(job, new Path("/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/HR_Analysis/output_lowsalLeft"));

		job.waitForCompletion(true);
	}
}
