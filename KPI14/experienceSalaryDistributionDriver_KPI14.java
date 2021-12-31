package com.ramesh.hranalysis;
 
	import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

	import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;



	public class experienceSalaryDistributionDriver_KPI14 {

		 
			public final static void main(String[] args) throws Exception
			{
				args = new String[] { 
						"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/9.HR_Analysis/input",
						"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/9.HR_Analysis/Output_Data_KPI14/"};
						 
						/* delete the output directory before running the job */
						FileUtils.deleteDirectory(new File(args[1])); 
						 
						if (args.length != 2) {
						System.err.println("Please specify the input and output path");
						System.exit(-1);
						}
						
						System.setProperty("hadoop.home.dir","/home/hadoop/work/hadoop-3.1.2");

						Configuration conf = new Configuration();			
		 
			Job job = new Job(conf, "kpi 14");
			job.setJobName("Experienced employee salary distributions");
			job.setJarByClass(experienceSalaryDistributionDriver_KPI14.class);
			job.setMapperClass(experienceSalaryDistributionMapper_KPI14.class);
			job.setReducerClass(experienceSalaryDistributionReducer_KPI14.class);
			
			
			job.setInputFormatClass(TextInputFormat.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			
			Path input=new Path(args[0]);
		    Path output=new Path(args[1]);
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output); 
			job.waitForCompletion(true);
		}
	
		
		
	}
