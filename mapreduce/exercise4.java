import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


​
public class exercise4 extends Configured implements Tool {
 
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
        
 
	    //mapper function    	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
	        
		    // Converts the entire line to a string
	        String line = value.toString();

	        // Initialize tokenizer that splits columns
	        String[] tokens = line.split(",");

	        String name = tokens[2].trim();
	        String duration = tokens[3].trim();		
	        String year = tokens[165].trim();
				          
            //key as artist name, value as duration
	        context.collect(new Text(name),new Text(duration));	  
		             

	    }//mapper
    }//map class 
   
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {
	 
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			//We now must iterate through the array of values to find the maximum integer for each key (artist)
			Double max_dur = 0.0; // Java variable initialization
			
			while (values.hasNext())	{
        		Double parsing = Double.parseDouble(values.next().toString()); 
          		if (max_dur < parsing) { //Simple code block for finding the maximum of list. No fancy algorithms.
            		max_dur = parsing;
          	    }
        	}
        	output.collect(key, new DoubleWritable(max_dur)); //collect the key (artist) and the maximum duration
        }
      
	}

	public static class myPartitioner implements Partitioner<Text, Text> {

		public void configure(JobConf job) {
        }

    	public int getPartition(Text key, Text value, int numReduceTasks) {
             
             //ABCDE0123456789 to one server 1
    		 //FGHIJ to one server 2
    		 //KLMNO to one server 3
    		 //PQRST to one server 4
    		 //UVWXYZ to one server 5

    		 // get artist name initial
             char initial = key.toString().charAt(0);

    		 String letter = "ABCDEFGHIJKLMNOPQRSTUVWXXZ";
             char e = letter.charAt(4);
             char j = letter.charAt(9);
             char o = letter.charAt(14);
             char t = letter.charAt(19);
             char z = letter.charAt(25);


	        if(numReduceTasks == 0) {
	            return 0;
	        }

	        if(Character.compare(initial,e)<=0){
	            return 0; 
	        } else if (Character.compare(initial,e)>0 && Character.compare(initial,j)<=0){
	            return 1;
	        } else if (Character.compare(initial,j)>0 && Character.compare(initial,o)<=0){
	            return 2;
	        } else if (Character.compare(initial,o)>0 && Character.compare(initial,t)<=0){
	            return 3;
	        } else { 
	            return 4;
	        }
	    }
    }

    public int run(String[] args) throws Exception {
		JobConf job = new JobConf(getConf(), exercise4.class);
		job.setJobName("exercise4");
	​
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);

	   	job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
			
	    job.setPartitionerClass(myPartitioner.class);
	    job.setNumReduceTasks(5);
	​
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		JobClient.runJob(job);
		return 0;
    }
​
    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise4(), args);
		System.exit(res);
    }
}

