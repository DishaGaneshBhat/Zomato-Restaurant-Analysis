package aa.bb;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OnlineOrder {
    
    public enum Online_Counter{
       ONLINE,
       NONLINE
   };  
     		
public static void main(String[] args)throws Exception{
Configuration conf=new Configuration();
Job job=new Job(conf,"onlineorder");
job.setJarByClass(OnlineOrder.class);
job.setMapperClass(MapForOnlCount.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setNumReduceTasks(0);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
// get all the job related counters
Counters cn=job.getCounters();
// Find the specific counters that you want to print
Counter c1=cn.findCounter(Online_Counter.ONLINE);
System.out.println(c1.getDisplayName()+":"+c1.getValue());
Counter c2=cn.findCounter(Online_Counter.NONLINE);
System.out.println(c2.getDisplayName()+":"+c2.getValue());
/* We can get all the available counters from CounterGroup instance and print them all in loop*/
for (CounterGroup group : cn) {
System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
System.out.println(" number of counters in this group: " + group.size());
for (Counter counter : group) {
System.out.println(" - "  + ": " + counter.getName() + ": "+counter.getValue());
}
}
}
public static class MapForOnlCount extends Mapper<LongWritable, Text, Text, IntWritable>{
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	    String data = value.toString();
	    String[] field = data.split(",");

				
		                if(field[1].contains("Yes"))
	                        {
	                          context.getCounter(Online_Counter.ONLINE).increment(1);
	  
	                        }
	                         if(field[1].contains("No"))
	                        {
	                          context.getCounter(Online_Counter.NONLINE).increment(1);
	  
	                        }
	           }              
	   }          
}
			


