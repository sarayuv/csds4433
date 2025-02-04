import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class SameNationality {

    public static class SameNationalityMapper extends Mapper<Object, Text, Text, Text> {
        private static final String NATIONALITY = "Grenada";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] attributes = line.split(",");

            if (attributes.length >= 5) {
                String nationality = attributes[2].trim();
                String name = attributes[1].trim();
                String data = attributes[4].trim();
                if (NATIONALITY.equalsIgnoreCase(nationality)) {
                    context.write(new Text(name), new Text(data));
                }
            }
        }
    }

    public static class SameNationalityReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Same Nationality");
        job.setJarByClass(SameNationality.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(SameNationalityMapper.class);
        job.setReducerClass(SameNationalityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
