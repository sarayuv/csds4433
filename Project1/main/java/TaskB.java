import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
            // Expecting columns: 0: log_id, 1: user_id, 2: page_id, 3: time_spent, 4: timestamp
            if (fields.length == 5) {
                String pageId = fields[2].trim();
                context.write(new Text(pageId), new Text("a;"));
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
            if (fields.length == 5) {
                String pageId   = fields[0].trim();
                String name     = fields[1].trim();
                String nat      = fields[3].trim();

                context.write(new Text(pageId), new Text("d;" + name + "," + nat));
            } else {
                System.out.println("Skipping malformed row: " + value.toString()); // Debugging
            }
        }
    }

    public static class PageCount {
        int count;
        String details;

        public PageCount(int count, String details) {
            this.count = count;
            this.details = details;
        }
    }

    // Reducer
    public static class MostAccessedReducer extends Reducer<Text, Text, Text, Text> {

        private PriorityQueue<PageCount> topPages;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            topPages = new PriorityQueue<>(10, Comparator.comparingInt(pc -> pc.count));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String details = "";

            for (Text val : values) {
                String[] parts = val.toString().split(";");

                if (parts.length > 0) {
                    if (parts[0].equals("a")) {
                        count++;
                    } else if (parts[0].equals("d")) {
                        details = parts[1];
                    }
                }
            }

            if (topPages.size() < 10) {

                topPages.add(new PageCount(count, details));
            } else {

                if (!topPages.isEmpty() && topPages.peek().count < count) {
                    topPages.poll();
                    topPages.add(new PageCount(count, details));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            List<PageCount> resultList = new ArrayList<>();
            while (!topPages.isEmpty()) {
                resultList.add(topPages.poll());
            }

            Collections.reverse(resultList);

            for (PageCount pc : resultList) {
                context.write(new Text(String.valueOf(pc.count)), new Text(pc.details));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); // Run in local mode
        conf.set("fs.defaultFS", "file:///"); // Use local filesystem

        Job job = Job.getInstance(conf, "Top 10 Accessed Pages");
        job.setJarByClass(TaskB.class);

        // Input paths for both CSVs
        MultipleInputs.addInputPath(job, new Path("access_logs.csv"), TextInputFormat.class, MostAccessedMapper.class);
        MultipleInputs.addInputPath(job, new Path("pages.csv"), TextInputFormat.class, PageMapper.class);

        // Reducer configuration
        job.setReducerClass(MostAccessedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Output path
        Path outputPath = new Path("output_TaskB");
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
