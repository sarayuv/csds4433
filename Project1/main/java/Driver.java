import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * HOW TO RUN IN TERMINAL
 * hadoop jar your-jar-file.jar /input/path1 /output/path1 /input/path2 /output/path2
 */

public class Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Task A
        Job jobA = Job.getInstance(conf, "Same Nationality");
        jobA.setJarByClass(SameNationality.class);
        FileInputFormat.addInputPath(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[1]));
        jobA.setMapperClass(SameNationality.SameNationalityMapper.class);
        jobA.setReducerClass(SameNationality.SameNationalityReducer.class);
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);

        long timeStartA = System.currentTimeMillis();
        boolean jobASuccess = jobA.waitForCompletion(true);
        long timeFinishA = System.currentTimeMillis();
        double secondsA = (timeFinishA - timeStartA) / 1000.0;

        System.out.println("Same Nationality job " + (jobASuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsA + " seconds");

        // Task B

        // Task C

        // Task D
        Job jobD = Job.getInstance(conf, "Connectedness Factor");
        jobD.setJarByClass(ConnectednessFactor.class);
        FileInputFormat.addInputPath(jobD, new Path(args[2]));
        FileOutputFormat.setOutputPath(jobD, new Path(args[3]));
        jobD.setMapperClass(ConnectednessFactor.ConnectednessFactorMapper.class);
        jobD.setReducerClass(ConnectednessFactor.ConnectednessFactorReducer.class);
        jobD.setOutputKeyClass(Text.class);
        jobD.setOutputValueClass(IntWritable.class);

        long timeStartD = System.currentTimeMillis();
        boolean jobDSuccess = jobD.waitForCompletion(true);
        long timeFinishD = System.currentTimeMillis();
        double secondsD = (timeFinishD - timeStartD) / 1000.0;

        System.out.println("Connectedness Factor job " + (jobDSuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsD + " seconds");

        // Task E

        // Task F

        // Task G
        Job jobG = Job.getInstance(conf, "Disconnected Users");
        jobG.setJarByClass(DisconnectedUsers.class);
        FileInputFormat.addInputPath(jobG, new Path(args[4]));
        FileOutputFormat.setOutputPath(jobG, new Path(args[5]));
        jobG.setMapperClass(DisconnectedUsers.DisconnectedUsersMapper.class);
        jobG.setReducerClass(DisconnectedUsers.DisconnectedUsersReducer.class);
        jobG.setOutputKeyClass(Text.class);
        jobG.setOutputValueClass(Text.class);

        long timeStartG = System.currentTimeMillis();
        boolean jobGSuccess = jobG.waitForCompletion(true);
        long timeFinishG = System.currentTimeMillis();
        double secondsG = (timeFinishG - timeStartG) / 1000.0;

        System.out.println("Disconnected Users job " + (jobGSuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsG + " seconds");

        // Task H

        // Final Exit
        if (jobASuccess && jobDSuccess && jobGSuccess) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
