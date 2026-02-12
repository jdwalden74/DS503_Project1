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



public class TaskB {

    //This is a helper class to store the data in a structured way
    public static class DataTuple implements Writable {

        private Text filename;
        private IntWritable count;
        private Text NickName;
        private Text JobTitle;
        
        //MAKE THEM ALWAYS FILL EVERY FIELD EVEN WHEN NOT INPUT
        public DataTuple(){
            this.count = new IntWritable();
            this.NickName = new Text();
            this.JobTitle = new Text();

            //empty field
            this.filename = new Text();
        }
        
        //For summing up counts from activity 
        public DataTuple(Text filename, IntWritable count    ){
            this.filename = filename;
            this.count = count;

            //empty fields
            this.NickName = new Text();
            this.JobTitle = new Text();
        }

        //For joining with the User data
        public DataTuple(Text filename, Text NickName, Text JobTitle){
            this.filename = filename;
            this.NickName = NickName;
            this.JobTitle = JobTitle;

            //empty field
            this.count = new IntWritable();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            filename.write(out);
            NickName.write(out);
            JobTitle.write(out);
            count.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            filename.readFields(in);
            NickName.readFields(in);
            JobTitle.readFields(in);
            count.readFields(in);
        }

        public void setFilename(Text filename){
            this.filename = filename;
        }

        public void setCount(IntWritable count){
            this.count = count;
        }

        public void setNickName(Text NickName){
            this.NickName = NickName;
        }

        public void setJobTitle(Text JobTitle){
            this.JobTitle = JobTitle;
        }   

        public Text getFilename(){
            return this.filename;
        }

        public IntWritable getCount(){
            return this.count;
        }

        public Text getNickName(){
            return this.NickName;
        }

        public Text getJobTitle(){
            return this.JobTitle;
        }

    }

     //Mapper class for Task B
    public static class TaskBMapper extends Mapper<Object, Text, Text, DataTuple> {

        //This wills tore the filename, which will allow us to join
        //in the reducer
        private Text filename;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filename = new Text(fileSplit.getPath().getName());
        }
        
        private final static IntWritable one = new IntWritable(1);
        private Text id = new Text();
        private Text nickname = new Text();
        private Text jobTitle = new Text();
        
        //This is the ID column for accessed page
        private int targetColumn_Activity = 2; 
        private int targetColumn_User = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            //take the record it and split it by column
            String line = value.toString();
            String[] attr = line.split(",");

            //Check if the file is the User file
            if(filename.toString().equals("accounts.csv")){
                 //Make sure we skip the header
                if(attr[targetColumn_User].equals("id")) return;

                //set the key to the page and the nickname and job title
                id.set(attr[targetColumn_User]);
                nickname.set(attr[targetColumn_User + 1]);
                jobTitle.set(attr[targetColumn_User + 2]);

                //create the data tuple
                DataTuple data = new DataTuple(filename, nickname, jobTitle);

                //This is where we set the key value pair
                context.write(id, data);

                return;
            }   

            //Check if the file is the Activity file
            if(filename.toString().equals("activity.csv")){

                //Make sure we skip the header
                if(attr[targetColumn_Activity].equals("WhatPage")) return;

                //set the key to the page and the value to 1
                id.set(attr[targetColumn_Activity]);

                //create the data tuple
                DataTuple data = new DataTuple(filename, one);

                //This is where we set the key value pair
                context.write(id, data);

                return;
            } else {
                throw new IOException("Unknown file: " + filename);
            }
            
        }
    }

    //Reducer class for Task B
    public static class TaskBReducer extends Reducer<Text,DataTuple,Text,Text> {

        public void reduce(Text key, Iterable<DataTuple> values, Context context) throws IOException, InterruptedException {
            
            String nickName = "";
            String jobTitle = "";
            int sum = 0;

            //Iterate through the values

            for (DataTuple val : values) {
                if(val.getFilename().toString().equals("activity.csv")){
                    sum += val.getCount().get();
                } else if(val.getFilename().toString().equals("accounts.csv")){
                    nickName = val.getNickName().toString();
                    jobTitle = val.getJobTitle().toString();
                }
            }

            // Create a fresh tuple for output
            DataTuple outputTuple = new DataTuple();
            outputTuple.setCount(new IntWritable(sum));
            outputTuple.setNickName(new Text(nickName));
            outputTuple.setJobTitle(new Text(jobTitle));

            // Always output all fields
            context.write(
                key,
                new Text(
                    nickName + "\t" + jobTitle + "\t" + sum
                )
            );
        }
    }

    //Main method
    public static void main(String[] args) throws Exception {

        //Create a new job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task B");
        
        //Set the job class
        job.setJarByClass(TaskB.class);
        
        //Set the mapper class
        job.setMapperClass(TaskBMapper.class);
        
        //Set the reducer class
        job.setReducerClass(TaskBReducer.class);

        //Set the output key and value classes
        //This is where you set the data types for the key and value output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Set the map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataTuple.class);
        
        //Set the input and output paths
        //This is where you set the input and output paths
        FileInputFormat.addInputPath(job, new Path("/assignment1/accounts.csv"));
        FileInputFormat.addInputPath(job, new Path("/assignment1/activity.csv"));
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        
        
        //Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }   

}
