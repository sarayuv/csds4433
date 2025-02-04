import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskF {

    // Mapper for friends.csv
    public static class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3 && !fields[0].equals("FriendRel")) { // Skip header
                String personID = fields[1].trim();
                String myFriend = fields[2].trim();
                context.write(new Text(personID), new Text("FRIEND:" + myFriend));
            }
        }
    }

    // Mapper for access_logs.csv
    public static class AccessLogsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3 && !fields[0].equals("AccessID")) { // Skip header
                String byWho = fields[1].trim();
                String whatPage = fields[2].trim();
                context.write(new Text(byWho), new Text("ACCESS:" + whatPage));
            }
        }
    }

    // Mapper for pages.csv
    public static class PagesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 2 && !fields[0].equals("PersonID")) { // Skip header
                String personID = fields[0].trim();
                String name = fields[1].trim();
                context.write(new Text(personID), new Text("NAME:" + name));
            }
        }
    }

    // Reducer
    public static class FriendsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> friends = new HashSet<>();
            Set<String> accessedPages = new HashSet<>();
            Set<String> personNames = new HashSet<>();  // This Set will ensure distinct names

            // Loop through the values to process FRIEND, ACCESS, and NAME data
            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts[0].equals("FRIEND")) {
                    friends.add(parts[1]); // MyFriend
                } else if (parts[0].equals("ACCESS")) {
                    accessedPages.add(parts[1]); // WhatPage
                } else if (parts[0].equals("NAME")) {
                    personNames.add(parts[1]); // Store the person's name
                }
            }

            // Identify friends whose pages were NOT accessed
            for (String friend : friends) {
                if (!accessedPages.contains(friend)) {
                    // Ensure distinct person names
                    for (String personName : personNames) {
                        context.write(key, new Text(personName)); // Output PersonID and Name
                    }
                    break;  // Since personName will be the same for each key, we can break after writing
                }
            }
        }
    }


    // Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Friends Analysis");
        job.setJarByClass(TaskF.class);

        // Input paths
        MultipleInputs.addInputPath(job, new Path("/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project1V6/input/friends.csv"),
                TextInputFormat.class, FriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project1V6/input/access_logs.csv"),
                TextInputFormat.class, AccessLogsMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project1V6/input/pages.csv"),
                TextInputFormat.class, PagesMapper.class);

        // Output path
        FileOutputFormat.setOutputPath(job, new Path("/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project1V6/output_TaskF"));

        job.setReducerClass(FriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
