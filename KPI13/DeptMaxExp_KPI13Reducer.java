package com.ramesh.hranalysis; 

	import java.io.IOException;
import java.text.DecimalFormat;

	import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	public class DeptMaxExp_KPI13Reducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public final void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
		{
//Department wise average satisfaction level, average working hours and no of employee who left company.
int maxValue=0;

			for(IntWritable value : values) 
			{
			    if(value.get() > maxValue) 
			    {
			        maxValue = value.get();
			    }
			}
		context.write(key, new IntWritable(maxValue));

		}
	}