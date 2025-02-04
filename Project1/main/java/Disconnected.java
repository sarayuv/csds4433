import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

public class Disconnected {
    public static class DisconnectedMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] attributes = line.split(",");

            if (attributes.length >= 4) {
                try {
                    String dateOfFriendship = attributes[3].trim();
                    int year = Integer.parseInt(dateOfFriendship.split("-")[0]);
                    if (year >= 2020) {
                        context.write(new Text(attributes[1]), one);
                    }
                } catch (NumberFormatException e) {
                    // Skip invalid date formats
                }
            }
        }
    }

    public static class DisconnectedReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final HashSet<String> activeUsers = new HashSet<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            activeUsers.add(key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path inputPath = new Path(conf.get("userFile"));

            for (String user : getUsers(inputPath, conf)) {
                if (!activeUsers.contains(user)) {
                    context.write(new Text(user), new IntWritable(0));
                }
            }
        }

        private HashSet<String> getUsers(Path userFilePath, Configuration conf) throws IOException {
            HashSet<String> allUsers = new HashSet<>();
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(userFilePath)));
            String line;

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields.length >= 2) {
                    allUsers.add(fields[0]);
                }
            }
            reader.close();
            return allUsers;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("userFile", args[1]);

        Job job = new Job(conf, "Disconnected Users");
        job.setJarByClass(Disconnected.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(DisconnectedMapper.class);
        job.setReducerClass(DisconnectedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (job.waitForCompletion(true)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
