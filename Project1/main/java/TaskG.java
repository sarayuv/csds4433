import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TaskG {
    public static class TaskGMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private static final long MILLIS_IN_14_DAYS = 14L * 24 * 60 * 60 * 1000;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String personId = fields[1];
            String accessTimeStr = fields[3].trim();

            if (key.get() == 0 && value.toString().contains("FriendRel")) {
                return;
            }

            try {
                Date accessTime = dateFormat.parse(accessTimeStr);
                long currentTime = System.currentTimeMillis();
                if (currentTime - accessTime.getTime() <= MILLIS_IN_14_DAYS) {
                    context.write(new Text(personId), new Text(accessTimeStr));
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class PagesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 2) return;

            String personId = fields[0].trim();
            String name = fields[1].trim();

            if (key.get() == 0 && value.toString().contains("personId")) {
                return;
            }

            context.write(new Text(personId), new Text("name:" + name));
        }
    }

    public static class TaskGReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean isActive = false;
            String name = "Unknown";

            for (Text val : values) {
                String valStr = val.toString();
                if (valStr.equals("active")) {
                    isActive = true;
                } else if (valStr.startsWith("name:")) {
                    name = valStr.substring(5);
                }
            }

            if (!isActive) {
                context.write(key, new Text(name));
            }
        }
    }
}
