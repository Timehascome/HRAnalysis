package emp_Left_In_Indiviual_Dept;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Employee_Left_In_Indiviual_Department_Reducer extends Reducer<Text,Text,Text,IntWritable>{
	
	public void reduce (Text key,Iterable<Text> value,Context context){
		 int count = 0;

	 for(Text x: value){
		 String finalLeft = x.toString();
		 if(finalLeft.equalsIgnoreCase("1")){
			 count++;
		 }
	 }
	 
	 try{
	 context.write(key, new IntWritable(count));
	 } catch(IOException a){

	 } catch(InterruptedException b){
		 
	 }
}

}
