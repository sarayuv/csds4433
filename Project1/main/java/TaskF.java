import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TaskF {
    public static class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3 && !fields[0].equals("FriendRel")) {
                String personID = fields[1].trim();
                String myFriend = fields[2].trim();
                context.write(new Text(personID), new Text("FRIEND:" + myFriend));
            }
        }
    }

    public static class AccessLogsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3 && !fields[0].equals("AccessID")) {
                String byWho = fields[1].trim();
                String whatPage = fields[2].trim();
                context.write(new Text(byWho), new Text("ACCESS:" + whatPage));
            }
        }
    }

    public static class PagesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 2 && !fields[0].equals("PersonID")) {
                String personID = fields[0].trim();
                String name = fields[1].trim();
                context.write(new Text(personID), new Text("NAME:" + name));
            }
        }
    }

    public static class FriendsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> friends = new HashSet<>();
            Set<String> accessedPages = new HashSet<>();
            Set<String> personNames = new HashSet<>();

            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts[0].equals("FRIEND")) {
                    friends.add(parts[1]);
                } else if (parts[0].equals("ACCESS")) {
                    accessedPages.add(parts[1]);
                } else if (parts[0].equals("NAME")) {
                    personNames.add(parts[1]);
                }
            }

            for (String friend : friends) {
                if (!accessedPages.contains(friend)) {
                    for (String personName : personNames) {
                        context.write(key, new Text(personName));
                    }
                    break;
                }
            }
        }
    }
}
