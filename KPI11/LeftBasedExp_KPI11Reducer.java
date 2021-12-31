package com.ramesh.hranalysis; 

	import java.io.IOException;
import java.text.DecimalFormat;

	import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	public class LeftBasedExp_KPI11Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
	{
		public final void reduce(final IntWritable key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
		{
//Department wise average satisfaction level, average working hours and no of employee who left company.
			 
			int sum = 0;
			for (final IntWritable val : values)
			{
				sum=sum+val.get();
			} 
		context.write(key, new IntWritable(sum)); 
		}
	}