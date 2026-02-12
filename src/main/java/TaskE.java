import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class TaskE {


     //Mapper class for Task B
    public static class TaskEMapper extends Mapper<Object, Text, IntWritable, Text> {
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            //take the record it and split it by column
            String line = value.toString();
            String[] attr = line.split(",");

            if(attr[0].equals("ActionId")) return;

            //Write to the reducer the id of the account that did the action
            context.write(new IntWritable(Integer.parseInt(attr[1])), new Text("viewed"));

            //Write to the reducer the id of the account that was acted upon
            context.write(new IntWritable(Integer.parseInt(attr[2])), new Text("pageViewed"));
            
        }
    }

    //Reducer class for Task B
    public static class TaskEReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            IntWritable actions = new IntWritable(0);
            IntWritable pageViewed = new IntWritable(0);

            //Iterate through the values
            for (Text val : values) {

                if(val.toString().equals("viewed")) {
                    actions.set(actions.get() + 1);
                }

                if(val.toString().equals("pageViewed")) {
                    pageViewed.set(pageViewed.get() + 1);
                }
            }

            //This is where we write the key value pair
            context.write(key, new Text(actions.toString() + ", " + pageViewed.toString()));   
        }
    }

    //Main method
    public static void main(String[] args) throws Exception {

        //Create a new job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task E");
        
        //Set the job class
        job.setJarByClass(TaskE.class);
        
        //Set the mapper class
        job.setMapperClass(TaskEMapper.class);
        
        //Set the reducer class
        job.setReducerClass(TaskEReducer.class);

        //Set the output key and value classes
        //This is where you set the data types for the key and value output
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        //Set the input and output paths
        //This is where you set the input and output paths
        FileInputFormat.addInputPath(job, new Path("/assignment1/activity.csv"));
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        
        //Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }   


}
