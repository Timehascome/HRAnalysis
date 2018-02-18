package emp_Left_In_Indiviual_Dept;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Employee_Left_In_Indiviual_Department_Mapper extends Mapper<LongWritable,Text,Text,Text>{

	//	satisfaction_level	last_evaluation	number_project	average_montly_hours	time_spend_company	   Work_accident	  left	   promotion_last_5years	department	   salary
	//    0.38		    			0.53								2							157									3							  0	   					1					0								sales			low

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] valueString = value.toString().split(",");
		String dept = valueString[8].toString();
		String left = valueString[6].toString();
		context.write(new Text(dept), new Text(left));

	}
}