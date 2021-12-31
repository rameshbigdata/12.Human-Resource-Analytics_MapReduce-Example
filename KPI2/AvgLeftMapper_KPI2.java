package com.ramesh.hranalysis; 

	import java.io.IOException;

	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Mapper;

	public class AvgLeftMapper_KPI2 extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public final void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
		{
			final String line = value.toString(); 
//		satisfaction_level,last_evaluation,number_project,average_montly_hours,time_spend_company,Work_accident,left,promotion_last_5years,Department,salary
//		0.38,0.53,2,157,3,0,1,0,sales,low

			final String[] data = line.trim().split(",");
			
			if ((data.length == 10) && (!data[0].contains("satisfaction_level")) && (Integer.parseInt(data[6])==1))				// check length of the data
			{					
				final int leftsttus = Integer.parseInt(data[6]);	//214.05
				final String department =data[8];
				context.write(new Text (department), new IntWritable(Integer.parseInt(data[6])));
//				(ConstantKey,214.05) output to reducer
			}
		}
	}