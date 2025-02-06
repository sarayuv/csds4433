import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TaskH {
    public static class MorePopularMapper extends Mapper<Object, Text, Text, Text> {
        private boolean isHeader = true;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                String pid = fields[1].trim();
                context.write(new Text(pid), new Text("f;"));
            }
        }
    }

    public static class NameMapper extends Mapper<Object, Text, Text, Text> {
        private boolean isHeader = true;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                String perId = fields[0].trim();
                String name = fields[1].trim();
                context.write(new Text(perId), new Text("n;" + name));
            }
        }
    }

    public static class MorePopularReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String name = "Unknown";

            for (Text val : values) {
                String valStr = val.toString();
                String[] parts = valStr.split(";", 2);

                if (parts[0].equals("f")) {
                    count++;
                } else if (parts[0].equals("n") && parts.length > 1) {
                    name = parts[1];
                }
            }

            context.write(new Text(key.toString() + " - " + name), new IntWritable(count));
        }
    }
}
