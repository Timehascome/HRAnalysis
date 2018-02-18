package average_satisfaction_level_individual_Department;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_Satisfaction_Level extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{

		int sum = 0;
		int count = 0;
		for(DoubleWritable x : values){
			sum += x.get();
			count++;
		}
		
		int average = sum/count;
		context.write(key, new DoubleWritable(average));
	}
}
