import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class exercise2 extends Configured implements Tool { 

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {


		private static final IntWritable one = new IntWritable(1);
        private String volumns;

		public void map(LongWritable key, Text value, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException {
		    
			 // Converts the entire line to a string
            String line = value.toString();

            // Initialize tokenizer that splits words. In unigram file, each line has 4 cols
            // whilw each line has 5 cols in bigram file
            String[] tokens = line.split("\\s+");

            if (tokens.length == 4){
                volumns =  tokens[3].trim();
            }
            
            if (tokens.length == 5){
                volumns =  tokens[4].trim();
            }

            output.collect(one, new Text(volumns));
		

		}
	}

		
    public static class AvgCombiner extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            
            Double sumofSquare = 0.0, sum = 0.0, count = 0.0;
        

            while (values.hasNext()) {
                sumofSquare +=  (Math.pow(Double.parseDouble(values.next().toString()),2));
                sum += Double.parseDouble(values.next().toString());
                count++;
            }
         
            output.collect(key, new Text(String.valueOf(sumofSquare)+","+String.valueOf(sum)+","+String.valueOf(count)));
        }
    }

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, DoubleWritable, Text> {
        
        Double sd, sumofSquare = 0.0, sum = 0.0, count = 0.0, meanSquare=0.0;
        Text myvalue = new Text();

		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
		 		    
		    myvalue.set("");
		    
		    while (values.hasNext()) {
                Text triplet = values.next();
                String[] tokens = triplet.toString().split(",");

                sumofSquare +=  Double.parseDouble(tokens[0]);
                sum += Double.parseDouble(tokens[1]);
                count += Double.parseDouble(tokens[2]);
            }
             
            meanSquare = Math.pow((sum/count),2);
		    sd = Math.sqrt((sumofSquare/count - meanSquare));

		    output.collect(new DoubleWritable(sd),myvalue);
	    }
    }

    public int run(String[] args) throws Exception {
        JobConf job= new JobConf(getConf(), exercise2.class);
        job.setJobName("exercise2");
   
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
    
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(AvgCombiner.class);  
  

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);  
        
        //file path for input and output
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

    
        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new exercise2(), args);
        System.exit(res);
    }
}




