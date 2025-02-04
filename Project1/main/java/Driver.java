import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
        FileInputFormat.addInputPath(jobA, new Path(args[2]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[3]));
        jobA.setMapperClass(SameNationality.SameNationalityMapper.class);
        jobA.setReducerClass(SameNationality.SameNationalityReducer.class);
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);
        boolean jobASuccess = jobA.waitForCompletion(true);
        System.out.println("Same Nationality job " + (jobASuccess ? "Succeeded" : "Failed"));

        // Task B

        // Task C

        // Task D

        // Task E

        // Task F

        // Task G
        Job jobG = Job.getInstance(conf, "Connectedness Factor");
        jobA.setJarByClass(ConnectednessFactor.class);
        FileInputFormat.addInputPath(jobA, new Path(args[4]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[5]));
        jobA.setMapperClass(ConnectednessFactor.ConnectednessFactorMapper.class);
        jobA.setReducerClass(ConnectednessFactor.ConnectednessFactorReducer.class);
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);
        boolean jobGSuccess = jobA.waitForCompletion(true);
        System.out.println("Connectedness Factor job " + (jobGSuccess ? "Succeeded" : "Failed"));

        // Task H

        // Final Exit
        if (jobASuccess && jobGSuccess) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
