package com.ramesh.hranalysis; 

	import java.io.IOException;
import java.text.DecimalFormat;

	import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	public class DeptSalaryBaseLeftReducer extends Reducer<Text, Text, Text, Text>
	{
		public final void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException
		{
//Department wise average satisfaction level, average working hours and no of employee who left company.
			 
			int count = 0;
			double avgSatis=0.0;
			double avgeval=0.0;
			int salLow=0;
			int salMedium=0;
			int salHigh=0;
			int left=0;
//			Key => department  Value => 214.05,100.00,200.00,300.00.....
			for (final Text val : values)
			{
				String[] value = val.toString().split(",");			
				count++;
				avgSatis=avgSatis+Double.parseDouble(value[0]);
				avgeval=avgeval+Double.parseDouble(value[1]);

				if (Integer.parseInt(value[2])==0)
				{
					left++;
					if(value[3].contains("low"))
					{
						salLow++;
					}
					else if(value[3].contains("medium"))
					{
						salMedium++;
					}
					else
					{
						salHigh++;
					}
				}
			} 
			String avgSatisfaction=String.valueOf(avgSatis/count);
			String avgevalion=String.valueOf(avgeval/count);
			String lowLef=String.valueOf((double)salLow*100/left);
			String mediumLef=String.valueOf((double)salMedium*100/left);
			String highLef=String.valueOf((double)salHigh*100/left);
			context.write(key, new Text(avgSatisfaction+'\t'+avgevalion+'\t'+left+'\t'+"Low sal percentage"+lowLef+'\t'+"Medium sal percentage"+mediumLef+'\t'+"High sal percentage"+highLef)); 
		}
	}