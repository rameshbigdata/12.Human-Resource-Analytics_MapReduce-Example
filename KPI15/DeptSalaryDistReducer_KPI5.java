package com.ramesh.hranalysis; 

	import java.io.IOException;
	import java.text.DecimalFormat;

	import org.apache.hadoop.io.DoubleWritable;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;

	public class DeptSalaryDistReducer_KPI5 extends Reducer<Text, Text, Text, Text>
	{
		public final void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException
		{
			int count = 0;
			int a =0;
			int b =0;
			int c =0;
//			Key => department  Value => 214.05,100.00,200.00,300.00.....
			for(Text val:values)
			{
				if(val.toString().equals("low"))
					a++;
				
				else if(val.toString().equals("medium"))
					b++;
				
				else if(val.toString().equals("high"))
					c++;
			}
			
			String low = String.valueOf(a);
			String medium = String.valueOf(b);
			String high = String.valueOf(c);
			
			
		context.write(key,new Text(low+" "+medium+" "+high));	 
		}
	}