package Disconnected;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Disconnected {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("userFile", args[1]);

        Job job = new Job(conf, "Disconnected Users");
        job.setJarByClass(Disconnected.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("output/taskD"));

        job.setMapperClass(DisconnectedMapper.class);
        job.setReducerClass(DisconnectedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (job.waitForCompletion(true)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}

