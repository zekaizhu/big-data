import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
​
public class exercise3 extends Configured implements Tool {
 
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    	
	    private Text mykey=new Text();
	    private Text myvalue=new Text();


	    // fucntion to validate year is integer between 1-2020
	    public static Integer CheckYear(String year) {
	        
	        Integer valid_year = 0;
	        Integer year_to_valid;
	        
		    try{
	            year_to_valid = Integer.parseInt(year);
	            if(year_to_valid >= 2000 && year_to_valid <= 2010){
	                valid_year = year_to_valid; 
	            }
	        }catch (NumberFormatException nfe){

	        }

	       return valid_year;
	    }

	    //mapper function    	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
	        


		    // Converts the entire line to a string
	        String line = value.toString();

	        // Initialize tokenizer that splits columns
	        String[] tokens = line.split(",");

	        String title = tokens[0].trim();
	        String name = tokens[2].trim();
	        String duration = tokens[3].trim();
	        String year = tokens[165].trim();
			

	        // combine title+name+duration
	        String comb_key;  
	        
	        if(CheckYear(year) != 0){
	                comb_key = title + ", "+name+", "+duration;
	                mykey.set(comb_key);
	    			myvalue.set("");
	    			context.collect(mykey,myvalue);
	    	}
	            

	    }//mapper
    }//class 
   
   public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), exercise3.class);
	conf.setJobName("exercise3");
​
	conf.setMapOutputKeyClass(Text.class);
	conf.setMapOutputValueClass(Text.class);
	
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);
	conf.setMapperClass(Map.class);	
    conf.setNumReduceTasks(0);
   
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
		
​
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	JobClient.runJob(conf);
	return 0;
    }
​
    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new exercise3(), args);
	System.exit(res);
    }
}