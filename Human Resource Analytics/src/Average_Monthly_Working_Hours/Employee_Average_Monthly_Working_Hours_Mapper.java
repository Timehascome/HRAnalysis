package Average_Monthly_Working_Hours;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Employee_Average_Monthly_Working_Hours_Mapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] values = value.toString().split(",");
		String dept = values[8].toString();
		String avgMonthlyHours = values[3].toString();
		int avgMonHours = Integer.parseInt(avgMonthlyHours);
		context.write(new Text(dept),new IntWritable(avgMonHours));
		
		
		
	}
}
