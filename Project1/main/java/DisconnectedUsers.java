import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

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

    public static class TaskGReducer extends Reducer<Text, Text, Text, Text> {
        private final Set<String> recentAccessIds = new HashSet<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            recentAccessIds.add(key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new FileReader("input/pages.csv"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                String personId = fields[0];
                String name = fields[1];
                if (!recentAccessIds.contains(personId)) {
                    context.write(new Text(personId), new Text(name));
                }
            }
            br.close();
        }
    }
}
