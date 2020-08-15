import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise1 extends Configured implements Tool {

                                                                    //Mapper Input Key: Byte Offset of Line (LongWritable)
                                                                    //Mapper Input Value: line of file (Text)
                                                                    //Mapper Output Key: Word+year (Text)
                                                                    //Mapper Output Value: volumes (Text)
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    	
        // fucntion to validate year is integer between 1-2020
	    public static Integer CheckYear(String year) {
            
            Integer valid_year = 0;
            Integer year_to_valid;
            
		    try{
	            year_to_valid = Integer.parseInt(year);
	            if(year_to_valid > 0 && year_to_valid <= 2020){
	                valid_year = year_to_valid; 
	            }
	        }catch (NumberFormatException nfe){

	        }

	       return valid_year;
	    }
    
	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	        
	        // Converts the entire line to a string
            String line = value.toString();

            // Initialize tokenizer that splits words. In unigram file, each line has 4 cols
            // whilw each line has 5 cols in bigram file
	        String[] tokens = line.split("\\s+");

	        String word;
	        String word1;  //first word in bigram 
			String word2;  //second word in bigram
			String year;
			String volumes;
			Boolean match1 = false;   //match "nu"
            Boolean match2 = false;   //match "chi"
            Boolean match3 = false;   //match "haw"

	        // combine substring in a word with year
	        String comb_key;  

	        // Unigram:
	        if (tokens.length == 4){
			    word = tokens[0].trim().toLowerCase();
			    year = tokens[1].trim();
			    volumes = tokens[3].trim();

                if(CheckYear(year) != 0){
                	if(word.contains("nu")){
                		comb_key = year + ",nu";
                		output.collect(new Text(comb_key), new Text(volumes));
                	}

                	if(word.contains("chi")){
                		comb_key = year + ",chi";
                		output.collect(new Text(comb_key), new Text(volumes));
                	}

                	if(word.contains("haw")){
                		comb_key = year + ",haw";
                		output.collect(new Text(comb_key), new Text(volumes));
                	}
                }	        
	        }

	        //Bigram:
	        if (tokens.length == 5){
			    word1 = tokens[0].trim().toLowerCase();
			    word2 = tokens[1].trim().toLowerCase();
			    year = tokens[2].trim();
			    volumes = tokens[4].trim();

                if(CheckYear(year) != 0){
                	if(word1.contains("nu")){
                		comb_key = year + ",nu";
                		output.collect(new Text(comb_key), new Text(volumes));
                	}

                	if(word1.contains("chi")){
                		comb_key = year + ",chi";
                		output.collect(new Text(comb_key), new Text(volumes));
                	}

                	if(word1.contains("haw")){
                		comb_key = year + ",haw";
                		output.collect(new Text(comb_key), new Text(volumes));
                	}

                	if(word2.contains("nu")){
                		comb_key = year + ",nu";
                		output.collect(new Text(comb_key), new Text(volumes));
                	}

                	if(word2.contains("chi")){
                		comb_key = year + ",chi";
                		output.collect(new Text(comb_key), new Text(volumes));
                	}

                	if(word2.contains("haw")){
                		comb_key = year + ",haw";
                		output.collect(new Text(comb_key), new Text(volumes));
                	}
                }	        
	        }
	    }
    }


    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            // sum up the volumes for each substring per year
            // count times substring appeared per year(1 for unigram, 2 for bigram)
            double sum = 0.0, count = 0.0, avg = 0.0;
 
	        while (values.hasNext()) {
				sum += Double.parseDouble(values.next().toString());
			    count++; 
		    }

		    avg = sum/count;
	        output.collect(key, new DoubleWritable(avg));
        }
    }

    public int run(String[] args) throws Exception {

	    JobConf job= new JobConf(getConf(), exercise1.class);
	    job.setJobName("exercise1");
   
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
  

	    job.setInputFormat(TextInputFormat.class);
	    job.setOutputFormat(TextOutputFormat.class);  
        
        //file path for input and output
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //set comma for output key/value separator
        job.set("mapred.textoutputformat.ignoreseparator", "true"); 
        job.set("mapred.textoutputformat.separator", ","); 
    
	    JobClient.runJob(job);
	    return 0;
    }

    public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new exercise1(), args);
	    System.exit(res);
    }
}