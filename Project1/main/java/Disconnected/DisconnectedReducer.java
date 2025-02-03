package Disconnected;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DisconnectedReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final HashSet<String> activeUsers = new HashSet<>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
                allUsers.add(fields[0]); // Assuming first column is personID
            }
        }
        reader.close();
        return allUsers;
    }
}
