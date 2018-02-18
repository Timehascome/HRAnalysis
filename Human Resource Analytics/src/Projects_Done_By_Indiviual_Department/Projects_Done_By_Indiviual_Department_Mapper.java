package Projects_Done_By_Indiviual_Department;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Projects_Done_By_Indiviual_Department_Mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	public void map(Text key,IntWritable values,Context context) throws IOException, InterruptedException{
		String[] value = values.toString().split(",");
		String dept = value[8].toString();
		String projDon = value[2].toString();
		int projDone =Integer.parseInt(projDon);
		context.write(new Text(dept), new IntWritable(projDone));
		
		
	}

}
