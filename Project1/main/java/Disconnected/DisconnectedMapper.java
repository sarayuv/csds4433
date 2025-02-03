package Disconnected;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DisconnectedMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);

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
