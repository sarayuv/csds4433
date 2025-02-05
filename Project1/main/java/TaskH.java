import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.*;


public class TaskH {


    // Mapper for friends.csv
    public static class MorePopularMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
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
                // get person id
                String pid = fields[1].trim();
                context.write(new Text(pid), new Text("f;"));
            } else {
                System.out.println("Skipping malformed row: " + value.toString()); // Debugging
            }
        }
    }


    // Mapper for pages.csv
    public static class NameMapper extends Mapper<Object, Text, Text, Text> {

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
                // get person id
                String perId = fields[0].trim();
                String name = fields[1].trim();
                context.write(new Text(perId), new Text("n;" + name));
            } else {
                System.out.println("Skipping malformed row: " + value.toString()); // Debugging
            }
        }
    }


    // Reducer Class
    public static class MorePopularReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private HashMap<String, Integer> friends = new HashMap<>();
        private HashMap<String, String> people = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            String name = "";

            for(Text val : values){
                String[] parts = val.toString().split(";");
                if(parts[0].equals("f")){
                    count++;
                } else if(parts[0].equals("n")){
                    name = parts[1];
                }
            }

            friends.put(key.toString(), count);
            people.put(key.toString(), name);

        }

        protected void sort(Context context) throws IOException, InterruptedException {
            int average = 0;

            for(int num : friends.values()){
                average += num;
            }

            average = average / friends.size();

            for(Map.Entry<String, Integer> entry : friends.entrySet()){
                if(entry.getValue() > average){
                    context.write(new Text(entry.getKey() + "- " + people.get(entry.getKey())), new IntWritable(entry.getValue()));
                }
            }

        }

    }


    // Main Driver Method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); // Explicitly set local mode
        conf.set("fs.defaultFS", "file:///"); // Set default filesystem to local
        System.out.println("Hadoop Configuration: " + conf);

        // Create and configure the job
        Job job = Job.getInstance(conf, "Popular Users");
        job.setJarByClass(TaskH.class);

        // Define input and output paths
        MultipleInputs.addInputPath(job, new Path("friends.csv"), TextInputFormat.class, MorePopularMapper.class);
        MultipleInputs.addInputPath(job, new Path("pages.csv"), TextInputFormat.class, NameMapper.class);
        Path outputPath = new Path("output_TaskH");
        System.out.println("Output Path: " + outputPath);

//        job.setMapperClass(MostAccessedMapper.class);
        job.setReducerClass(MorePopularReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileOutputFormat.setOutputPath(job, outputPath);

        // Print job configuration
        System.out.println("Job Configuration: " + job.getConfiguration());


        // Run the job and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
