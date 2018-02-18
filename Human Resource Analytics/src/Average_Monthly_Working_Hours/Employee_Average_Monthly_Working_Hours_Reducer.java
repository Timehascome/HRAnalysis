package Average_Monthly_Working_Hours;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Employee_Average_Monthly_Working_Hours_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
		IntWritable result = new IntWritable();

		int sum = 0 , count = 0,avg=0;
		for (IntWritable val : value)
		{
			sum += val.get();
			count++;
		}
		avg = sum/count;
		
		result.set(avg);
		context.write(key, result);
		
	}

}
