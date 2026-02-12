import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//NOT DONE YET NEED TO IMPLIMENT GETTING NICKNAME FROM ACCOUNTS CSV
//Should have a combiner
public class TaskD {

    //Mapper class for Task D
    public static class TaskDMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        
        //This wills store the filename, which will allow us to join
        //in the reducer
        private Text filename;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filename = new Text(fileSplit.getPath().getName());
        }

        private final static IntWritable one = new IntWritable(1);
        private IntWritable accountID = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            //take the record it and split it by column
            String line = value.toString();
            String[] attr = line.split(",");

            //if the file is follows.csv
            if(filename.toString().equals("follows.csv")) {
                //Make sure we skip the header
                if(attr[0].equals("Colrel")) return;

                //set the key to the id of the second ID in the follow
                accountID.set(Integer.parseInt(attr[2]));

                //This is where we write the key value pair
                context.write(accountID, one);
            }   

            //if the file is accounts.csv
            if(filename.toString().equals("accounts.csv")) {
                //Make sure we skip the header
                if(attr[0].equals("id")) return;

                //set the key to the id of the second ID in the follow
                accountID.set(Integer.parseInt(attr[0]));

                //This is where we write the key value pair
                //WE WANT TO SEND THE NICKNAME HERE BUT NOT YET
                context.write(accountID, one);  
            }   
        }
    }

    //Reducer class for Task D
    public static class TaskDReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
            //track the count for each Follower
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }   
            
            //write the key value pair
            context.write(key, new IntWritable(sum));
        }
    }

    //Main method
    public static void main(String[] args) throws Exception {

        //Create a new job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task D");
        
        //Set the job class
        job.setJarByClass(TaskD.class);
        
        //Set the mapper class
        job.setMapperClass(TaskDMapper.class);
        
        //Set the reducer class
        job.setReducerClass(TaskDReducer.class);

        //Set the output key and value classes
        //This is where you set the data types for the key and value output
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        //Set the input and output paths
        //This is where you set the input and output paths
        FileInputFormat.addInputPath(job, new Path("/assignment1/accounts.csv"));
        FileInputFormat.addInputPath(job, new Path("/assignment1/follows.csv"));
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        
        //Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }   


}
