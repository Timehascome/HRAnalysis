package average_satisfaction_level_individual_Department;

import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_Satisfaction_Level extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
//	satisfaction_level	last_evaluation	number_project	average_montly_hours	time_spend_company	   Work_accident	  left	   promotion_last_5years	department	   salary
//	      0.38		    			0.53								2							157									3							  0	   					1					0								sales			low
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] valueString = value.toString().split(",");
		String dept = valueString[8].toString();
		String satisfactionLevel = valueString[0];
		
		context.write(new Text(dept), new DoubleWritable(Double.parseDouble(satisfactionLevel)));
		
	}
	
}
