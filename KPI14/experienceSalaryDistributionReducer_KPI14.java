package com.ramesh.hranalysis;
 
	import java.io.*;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.Reducer.Context;


	public class experienceSalaryDistributionReducer_KPI14 extends Reducer<Text,Text,Text,Text> {

		//key                values
	//sales 10           [low,high,low,medium .....]	
		
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
		{
			long low_count=0;
			long medium_count=0;
			long high_count=0;
			
			for(Text val:values)
			{
				String temp = val.toString();
				
				if(temp.equals("low"))
					low_count++;
				
				else if(temp.equals("medium"))
					medium_count++;
				
				else if(temp.equals("high")) 
					high_count++;
				
				
			}
			
		context.write(key, new Text( String.valueOf(low_count) + "\t" + String.valueOf(medium_count) + "\t" + String.valueOf(high_count)));
		           
			
			
			
		}
		
		
		
		
	}