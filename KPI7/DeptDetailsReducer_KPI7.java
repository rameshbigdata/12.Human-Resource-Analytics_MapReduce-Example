package com.ramesh.hranalysis;
  

	import java.io.IOException;
import java.text.DecimalFormat;

	import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	public class DeptDetailsReducer_KPI7 extends Reducer<Text, Text, Text, Text>
	{
		public final void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException
		{
//Department wise average satisfaction level, average working hours and no of employee who left company.
			 
			int count = 0;
			double avgSatis=0.0;
			int avgWrk=0;
			int left=0;
//			Key => department  Value => 214.05,100.00,200.00,300.00.....
			for (final Text val : values)
			{
				String[] value = val.toString().split(",");			
				count++;
				avgSatis=avgSatis+Double.parseDouble(value[0]);
				avgWrk=avgWrk+Integer.parseInt(value[1]);
				if (Integer.parseInt(value[2])==1)
				{
					left++;
				}
			} 
			String avgSatisfaction=String.valueOf(avgSatis/count);
			String avgWrking=String.valueOf(avgWrk/count);
			String lef=String.valueOf(left);
			context.write(key, new Text(avgSatisfaction+'\t'+avgWrking+'\t'+lef)); 
		}
	}