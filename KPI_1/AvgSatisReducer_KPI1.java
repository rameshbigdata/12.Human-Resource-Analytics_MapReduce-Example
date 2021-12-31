package com.ramesh.hranalysis; 

	import java.io.IOException;
	import java.text.DecimalFormat;

	import org.apache.hadoop.io.DoubleWritable;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;

	public class AvgSatisReducer_KPI1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
	{
		public final void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException
		{
			double sum = 0.0;
			int count = 0;
//			Key => department  Value => 214.05,100.00,200.00,300.00.....
			for (final DoubleWritable val : values)
			{
				count++;
				sum += val.get();
			} 			 
			context.write(key, new DoubleWritable(sum/count)); 
		}
	}