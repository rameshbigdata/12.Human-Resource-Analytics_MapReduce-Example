package com.ramesh.hranalysis; 

	import java.io.IOException;
import java.text.DecimalFormat;

	import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	public class DeptAvgSatisWorkhrLeftEmpReducer_KPI10 extends Reducer<Text, Text, Text, Text>
	{
		public final void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException
		{
			float a = 0.0f;
			float b= 0.0f;
			float total_num = 0;
			int isleft;
			int count =0;
			for(Text val:values)
			{
				total_num++;
				String []tokens  = val.toString().split(",");
				a+=Float.parseFloat(tokens[0]);
				b+=Float.parseFloat(tokens[1]);
				
				isleft = Integer.parseInt(tokens[6]);
				
				if(isleft==1)
				{
					count++;
				}
				
				
		    }
			
			float avg_satisfaction = a/total_num;
			float avg_evaluation = b/total_num;
			
			float percent_left = (count/total_num)*100;
			
			context.write(key, new Text(  String.valueOf(avg_satisfaction)+"\t"+String.valueOf(avg_evaluation)+"\t"+String.valueOf(percent_left)+"%") );
			
			// key                             values
			// dept salary                avg_satisfaction       avg_evaluation     percent_left
			
			
			
			
			
			
		}
		
		
		
	}