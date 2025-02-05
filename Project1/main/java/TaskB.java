import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.io.IOException;
import java.util.*;


public class TaskB {


    // Mapper for access_logs.csv
    public static class MostAccessedMapper extends Mapper<Object, Text, Text, Text> {

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
                // get page id
                String pid = fields[2].trim();
                context.write(new Text(pid), new Text("a;"));
            } else {
                System.out.println("Skipping malformed row: " + value.toString()); // Debugging
            }
        }
    }

    // Mapper for pages.csv
    public static class PageMapper extends Mapper<Object, Text, Text, Text> {

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
                String nat = fields[3].trim();
                context.write(new Text(perId), new Text("d;" + name + "," + nat));
            } else {
                System.out.println("Skipping malformed row: " + value.toString()); // Debugging
            }
        }
    }


    // Reducer Class
    public static class MostAccessedReducer extends Reducer<Text, Text, Text, Text> {

        private TreeMap<String, String> pages = new TreeMap<>(Collections.reverseOrder());

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            String deets = "";
            for(Text val : values){
                String[] parts = val.toString().split(";");
                if(parts[0].equals("a")){
                    count++;
                } else if(parts[0].equals("d")){
                    deets = parts[1];
                }
            }

            pages.put(Integer.toString(count), deets);

            if(pages.size() > 10){
                pages.remove(pages.lastKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            for(Map.Entry<String, String> entry : pages.entrySet()){
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
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
        Job job = Job.getInstance(conf, "Top 10 Accessed Pages");
        job.setJarByClass(TaskB.class);
        job.setMapperClass(MostAccessedMapper.class);
        job.setReducerClass(MostAccessedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Define input and output paths
        MultipleInputs.addInputPath(job, new Path("access_logs.csv"), TextInputFormat.class, MostAccessedMapper.class);
        MultipleInputs.addInputPath(job, new Path("pages.csv"), TextInputFormat.class, PageMapper.class);
        Path outputPath = new Path("output_TaskB");
        System.out.println("Output Path: " + outputPath);

        // Set input and output paths
        FileOutputFormat.setOutputPath(job, outputPath);


        // Print job configuration
        System.out.println("Job Configuration: " + job.getConfiguration());


        // Run the job and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
