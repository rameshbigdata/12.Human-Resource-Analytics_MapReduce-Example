package com.ramesh.hranalysis; 

	import java.io.IOException;

	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Mapper;

	public class DeptSalaryDistMapper_KPI5 extends Mapper<LongWritable, Text, Text, Text>
	{
		public final void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
		{
			final String line = value.toString(); 
//		satisfaction_level,last_evaluation,number_project,average_montly_hours,time_spend_company,Work_accident,left,promotion_last_5years,Department,salary
//		0.38,0.53,2,157,3,0,1,0,sales,low

			final String[] data = line.trim().split(",");
			
			if ((data.length == 10) && (!data[0].contains("satisfaction_level")))				// check length of the data
			{					
				String dept =  data[8];
				String salary = data[9];
				context.write(new Text (dept), new Text (salary));
//				(ConstantKey,214.05) output to reducer
			}
		}
	}