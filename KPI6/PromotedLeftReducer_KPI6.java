package com.ramesh.hranalysis; 

	import java.io.IOException;
	import java.text.DecimalFormat;
	import java.io.*;
	import org.apache.hadoop.io.DoubleWritable;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;

	public class PromotedLeftReducer_KPI6 extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public final void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException
		{
 
			int sum = 0;
//			Key => department  Value => 214.05,100.00,200.00,300.00.....
			
			for (final IntWritable val : values)
			{
				sum=sum+val.get(); 
			} 			 
			context.write(key, new IntWritable(sum)); 
		}
	}