import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TaskC {
    public static class TaskCMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text countryCode = new Text();
        private boolean isHeader = true;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                String country = fields[3].trim();
                countryCode.set(country);
                context.write(countryCode, one);
            }
        }
    }

    public static class TaskCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
