import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TaskE {
    public static class TaskEMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (line.startsWith("AccessID")) {
                return;
            }

            String[] fields = line.split(",");
            if (fields.length == 5) {
                String byWho = fields[1].trim();
                String whatPage = fields[2].trim();
                context.write(new Text(byWho), new Text(whatPage));
            }
        }

    }

    public static class TaskEReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> distinctPages = new HashSet<>();
            int totalAccesses = 0;

            for (Text value : values) {
                distinctPages.add(value.toString());
                totalAccesses++;
            }

            String output = totalAccesses + "\t" + distinctPages.size();
            context.write(key, new Text(output));
        }
    }
}
