package ee.ff;

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


	public class RestType {
	    
	    public enum Restte_Counter{
	    	CASUALDINING,
	    	CAFE,
	    	QUICKBITES,
	    	DELIVERY,
	    	MESS,
	    	DESERTPARLOR,
	    	BAKERY,
	    	PUB,
	    	BAR,
	    	BEVERAGESHOP,
	    	CONFECTIONERY,
	    	SWEETSHOP,
	    	FOODTRUCK,
	    	MICROBREWARY,
	    	LOUNGE,
	    	FOODCOURT,
	    	KIOSK
	   };  
	     		
	public static void main(String[] args)throws Exception{
	Configuration conf=new Configuration();
	Job job=new Job(conf,"resttype");
	job.setJarByClass(RestType.class);
	job.setMapperClass(MapForResu.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setNumReduceTasks(0);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);
	// get all the job related counters
	Counters cn=job.getCounters();
	// Find the specific counters that you want to print
	Counter c1=cn.findCounter(Restte_Counter.QUICKBITES);
	System.out.println(c1.getDisplayName()+":"+c1.getValue());
	Counter c2=cn.findCounter(Restte_Counter.CASUALDINING);
	System.out.println(c2.getDisplayName()+":"+c2.getValue());
	Counter c3=cn.findCounter(Restte_Counter.CAFE);
	System.out.println(c3.getDisplayName()+":"+c3.getValue());
	Counter c4=cn.findCounter(Restte_Counter.DELIVERY);
	System.out.println(c4.getDisplayName()+":"+c4.getValue());
	Counter c5=cn.findCounter(Restte_Counter.MESS);
	System.out.println(c5.getDisplayName()+":"+c5.getValue());
	Counter c6=cn.findCounter(Restte_Counter.DESERTPARLOR);
	System.out.println(c6.getDisplayName()+":"+c6.getValue());
	Counter c7=cn.findCounter(Restte_Counter.BAKERY);
	System.out.println(c7.getDisplayName()+":"+c7.getValue());
	Counter c8=cn.findCounter(Restte_Counter.BAR);
	System.out.println(c8.getDisplayName()+":"+c8.getValue());
	Counter c9=cn.findCounter(Restte_Counter.PUB);
	System.out.println(c9.getDisplayName()+":"+c9.getValue());
	Counter c10=cn.findCounter(Restte_Counter.BEVERAGESHOP);
	System.out.println(c10.getDisplayName()+":"+c10.getValue());
	Counter c11=cn.findCounter(Restte_Counter.CONFECTIONERY);
	System.out.println(c11.getDisplayName()+":"+c11.getValue());
	Counter c12=cn.findCounter(Restte_Counter.SWEETSHOP);
	System.out.println(c12.getDisplayName()+":"+c12.getValue());
	Counter c13=cn.findCounter(Restte_Counter.FOODTRUCK);
	System.out.println(c13.getDisplayName()+":"+c13.getValue());
	Counter c14=cn.findCounter(Restte_Counter.MICROBREWARY);
	System.out.println(c14.getDisplayName()+":"+c14.getValue());
	Counter c15=cn.findCounter(Restte_Counter.LOUNGE);
	System.out.println(c15.getDisplayName()+":"+c15.getValue());
	Counter c16=cn.findCounter(Restte_Counter.FOODCOURT);
	System.out.println(c16.getDisplayName()+":"+c16.getValue());
	Counter c17=cn.findCounter(Restte_Counter.KIOSK);
	System.out.println(c17.getDisplayName()+":"+c17.getValue());
	
	/* We can get all the available counters from CounterGroup instance and print them all in loop*/
	for (CounterGroup group : cn) {
	System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
	System.out.println(" number of counters in this group: " + group.size());
	for (Counter counter : group) {
	System.out.println(" - "  + ": " + counter.getName() + ": "+counter.getValue());
	}
	}
	}
	public static class MapForResu extends Mapper<LongWritable, Text, Text, IntWritable>{
		   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		    String data = value.toString();
		    String[] field = data.split(",");

					
			                if(field[6].contains("Casual Dining"))
		                        {
		                          context.getCounter(Restte_Counter.CASUALDINING).increment(1);
		  
		                        }
		                         if(field[6].contains("Cafe"))
		                        {
		                          context.getCounter(Restte_Counter.CAFE).increment(1);
		  
		                        }
		                         if(field[6].contains("Quick Bites"))
			                        {
			                          context.getCounter(Restte_Counter.QUICKBITES).increment(1);
			  
			                        }
		                         if(field[6].contains("Delivery"))
			                        {
			                          context.getCounter(Restte_Counter.DELIVERY).increment(1);
			  
			                        }
		                         if(field[6].contains("Mess"))
			                        {
			                          context.getCounter(Restte_Counter.MESS).increment(1);
			  
			                        }
		                         if(field[6].contains("Dessert Parlor"))
			                        {
			                          context.getCounter(Restte_Counter.DESERTPARLOR).increment(1);
			  
			                        }
		                         if(field[6].contains("Bakery"))
			                        {
			                          context.getCounter(Restte_Counter.BAKERY).increment(1);
			  
			                        }
		                         if(field[6].contains("Pub"))
			                        {
			                          context.getCounter(Restte_Counter.PUB).increment(1);
			  
			                        }
		                         if(field[6].contains("Bar"))
			                        {
			                          context.getCounter(Restte_Counter.BAR).increment(1);
			  
			                        }
		                         if(field[6].contains("Beverage Shop"))
			                        {
			                          context.getCounter(Restte_Counter.BEVERAGESHOP).increment(1);
			  
			                        }
		                         if(field[6].contains("Confectionery"))
			                        {
			                          context.getCounter(Restte_Counter.CONFECTIONERY).increment(1);
			  
			                        }
		                         if(field[6].contains("Sweet Shop"))
			                        {
			                          context.getCounter(Restte_Counter.SWEETSHOP).increment(1);
			  
			                        }
		                         if(field[6].contains("Food Truck"))
			                        {
			                          context.getCounter(Restte_Counter.FOODTRUCK).increment(1);
			  
			                        }
		                         if(field[6].contains("Microbrewery"))
			                        {
			                          context.getCounter(Restte_Counter.MICROBREWARY).increment(1);
			  
			                        }
		                         if(field[6].contains("Lounge"))
			                        {
			                          context.getCounter(Restte_Counter.LOUNGE).increment(1);
			  
			                        }
		                         if(field[6].contains("Food Court"))
			                        {
			                          context.getCounter(Restte_Counter.FOODCOURT).increment(1);
			  
			                        }
		                         if(field[6].contains("Kiosk"))
			                        {
			                          context.getCounter(Restte_Counter.KIOSK).increment(1);
			  
			                        }
		                         
		                         
		           }              
		   }          
	}
				

