import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskC {

    // Mapper Class
    public static class CitizensMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text countryCode = new Text();
        private boolean isHeader = true; // Flag to skip the header row

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false; // Skip the first row (header)
                System.out.println("Skipping header: " + value.toString()); // Debugging
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length == 5) { // Ensure the row has exactly 5 columns
                String country = fields[3].trim(); // Extract the "Country Code" column
                countryCode.set(country);
                System.out.println("Processing row: " + value.toString() + " | Country Code: " + country); // Debugging
                context.write(countryCode, one); // Emit (Country Code, 1)
            } else {
                System.out.println("Skipping malformed row: " + value.toString()); // Debugging
            }
        }
    }
    // Reducer Class
    public static class CitizensReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get(); // Sum up the counts for each country
            }
            result.set(sum);
            context.write(key, result); // Emit (Country Code, total count)
        }
    }

    // Main Driver Method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); // Explicitly set local mode
        conf.set("fs.defaultFS", "file:///"); // Set default filesystem to local
        System.out.println("Hadoop Configuration: " + conf);

        // Define input and output paths
        Path inputPath = new Path("/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project1V6/input/pages.csv");
        Path outputPath = new Path("/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project1V6/output_TaskC");
        System.out.println("Input Path: " + inputPath);
        System.out.println("Output Path: " + outputPath);

        // Create and configure the job
        Job job = Job.getInstance(conf, "Citizens Per Country");
        job.setJarByClass(TaskC.class);
        job.setMapperClass(CitizensMapper.class);
        job.setReducerClass(CitizensReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Print job configuration
        System.out.println("Job Configuration: " + job.getConfiguration());

        // Run the job and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}