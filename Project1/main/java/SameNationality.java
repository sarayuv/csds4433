import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

// TASK A

public class SameNationality {

    public static class SameNationalityMapper extends Mapper<Object, Text, Text, Text> {
        private static final String NATIONALITY = "Grenada";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length >= 5) {
                String nationality = fields[2].trim();
                String name = fields[1].trim();
                String data = fields[4].trim();

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
}
