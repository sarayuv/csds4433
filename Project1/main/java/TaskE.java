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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskE {

    // Mapper Class
    public static class FavoritesMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Skip the header row
            if (line.startsWith("AccessID")) {
                return;
            }

            String[] fields = line.split(",");
            if (fields.length == 5) { // Ensure the row has exactly 5 columns
                String byWho = fields[1].trim();    // ByWho (person ID)
                String whatPage = fields[2].trim(); // WhatPage (page ID)
                context.write(new Text(byWho), new Text(whatPage)); // Emit (ByWho, WhatPage)
            }
        }

    }

    // Reducer Class
    public static class FavoritesReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> distinctPages = new HashSet<>(); // To store unique page IDs
            int totalAccesses = 0; // To count total accesses

            for (Text value : values) {
                distinctPages.add(value.toString()); // Add page ID to the set
                totalAccesses++; // Increment total accesses
            }

            // Emit (ByWho, (Total Accesses, Distinct Pages))
            String output = totalAccesses + "\t" + distinctPages.size();
            context.write(key, new Text(output));
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Favorites Analysis");

        job.setJarByClass(TaskE.class);
        job.setMapperClass(FavoritesMapper.class);
        job.setReducerClass(FavoritesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Hardcoded input and output paths
        Path inputPath = new Path("/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project1V6/input/access_logs.csv");
        Path outputPath = new Path("/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project1V6/output_TaskE");

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
