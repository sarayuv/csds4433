import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Task A
        Job jobA = Job.getInstance(conf, "Same Nationality");
        jobA.setJarByClass(TaskA.class);
        FileInputFormat.addInputPath(jobA, new Path("/user/cs4433/project1/input/pages.csv"));
        FileOutputFormat.setOutputPath(jobA, new Path("/user/cs4433/project1/output/taskA"));

        jobA.setMapperClass(TaskA.TaskAMapper.class);
        jobA.setReducerClass(TaskA.TaskAReducer.class);
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);

        long timeStartA = System.currentTimeMillis();
        boolean jobASuccess = jobA.waitForCompletion(true);
        long timeFinishA = System.currentTimeMillis();
        double secondsA = (timeFinishA - timeStartA) / 1000.0;

        // Task B
        Job jobB = Job.getInstance(conf, "Top 10 Accessed Pages");
        jobB.setJarByClass(TaskB.class);
        MultipleInputs.addInputPath(jobB, new Path("/user/cs4433/project1/input/access_logs.csv"), TextInputFormat.class, TaskB.MostAccessedMapper.class);
        MultipleInputs.addInputPath(jobB, new Path("/user/cs4433/project1/input/pages.csv"), TextInputFormat.class, TaskB.PageMapper.class);
        FileOutputFormat.setOutputPath(jobB, new Path ("/user/cs4433/project1/output/TaskB"));

        jobB.setMapperClass(TaskB.MostAccessedMapper.class);
        jobB.setReducerClass(TaskB.MostAccessedReducer.class);
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);

        long timeStartB = System.currentTimeMillis();
        boolean jobBSuccess = jobB.waitForCompletion(true);
        long timeFinishB = System.currentTimeMillis();
        double secondsB = (timeFinishB - timeStartB) / 1000.0;

        // Task C
        Job jobC = Job.getInstance(conf, "Citizens Per Country");
        jobC.setJarByClass(TaskC.class);
        FileInputFormat.addInputPath(jobC, new Path("/user/cs4433/project1/input/pages.csv"));
        FileOutputFormat.setOutputPath(jobC, new Path("/user/cs4433/project1/output/taskC"));

        jobC.setMapperClass(TaskC.TaskCMapper.class);
        jobC.setReducerClass(TaskC.TaskCReducer.class);
        jobC.setOutputKeyClass(Text.class);
        jobC.setOutputValueClass(IntWritable.class);

        long timeStartC = System.currentTimeMillis();
        boolean jobCSuccess = jobC.waitForCompletion(true);
        long timeFinishC = System.currentTimeMillis();
        double secondsC = (timeFinishC - timeStartC) / 1000.0;

        // Task D
        Job jobD = Job.getInstance(conf, "Connectedness Factor");
        jobD.setJarByClass(TaskD.class);
        FileInputFormat.addInputPath(jobD, new Path("/user/cs4433/project1/input/access_logs.csv"));
        FileOutputFormat.setOutputPath(jobD, new Path("/user/cs4433/project1/output/taskD"));

        jobD.setMapperClass(TaskD.TaskDMapper.class);
        jobD.setReducerClass(TaskD.TaskDReducer.class);
        jobD.setOutputKeyClass(Text.class);
        jobD.setOutputValueClass(IntWritable.class);

        long timeStartD = System.currentTimeMillis();
        boolean jobDSuccess = jobD.waitForCompletion(true);
        long timeFinishD = System.currentTimeMillis();
        double secondsD = (timeFinishD - timeStartD) / 1000.0;

        // Task E
        Job jobE = Job.getInstance(conf, "Favorites Analysis");
        jobE.setJarByClass(TaskE.class);
        FileInputFormat.addInputPath(jobE, new Path("/user/cs4433/project1/input/access_logs.csv"));
        FileOutputFormat.setOutputPath(jobE, new Path("/user/cs4433/project1/output/taskE"));

        jobE.setMapperClass(TaskE.TaskEMapper.class);
        jobE.setReducerClass(TaskE.TaskEReducer.class);
        jobE.setOutputKeyClass(Text.class);
        jobE.setOutputValueClass(Text.class);

        long timeStartE = System.currentTimeMillis();
        boolean jobESuccess = jobE.waitForCompletion(true);
        long timeFinishE = System.currentTimeMillis();
        double secondsE = (timeFinishE - timeStartE) / 1000.0;

        // Task F
        Job jobF = Job.getInstance(conf, "Friends Analysis");
        jobF.setJarByClass(TaskF.class);
        MultipleInputs.addInputPath(jobF, new Path("/user/cs4433/project1/input/friends.csv"), TextInputFormat.class, TaskF.FriendsMapper.class);
        MultipleInputs.addInputPath(jobF, new Path("/user/cs4433/project1/input/access_logs.csv"), TextInputFormat.class, TaskF.AccessLogsMapper.class);
        MultipleInputs.addInputPath(jobF, new Path("/user/cs4433/project1/input/pages.csv"), TextInputFormat.class, TaskF.PagesMapper.class);
        FileOutputFormat.setOutputPath(jobF, new Path("/user/cs4433/project1/output/TaskF"));

        jobF.setReducerClass(TaskF.FriendsReducer.class);
        jobF.setOutputKeyClass(Text.class);
        jobF.setOutputValueClass(Text.class);

        long timeStartF = System.currentTimeMillis();
        boolean jobFSuccess = jobF.waitForCompletion(true);
        long timeFinishF = System.currentTimeMillis();
        double secondsF = (timeFinishF - timeStartF) / 1000.0;

        // Task G
        Job jobG = Job.getInstance(conf, "Disconnected Users");
        jobG.setJarByClass(TaskG.class);

        MultipleInputs.addInputPath(jobG, new Path("/user/cs4433/project1/input/friends.csv"), TextInputFormat.class, TaskG.TaskGMapper.class);
        MultipleInputs.addInputPath(jobG, new Path("/user/cs4433/project1/input/pages.csv"), TextInputFormat.class, TaskG.PagesMapper.class);
        FileOutputFormat.setOutputPath(jobG, new Path("/user/cs4433/project1/output/taskG"));

        jobG.setReducerClass(TaskG.TaskGReducer.class);
        jobG.setOutputKeyClass(Text.class);
        jobG.setOutputValueClass(Text.class);

        long timeStartG = System.currentTimeMillis();
        boolean jobGSuccess = jobG.waitForCompletion(true);
        long timeFinishG = System.currentTimeMillis();
        double secondsG = (timeFinishG - timeStartG) / 1000.0;

        // Task H
        Job jobH = Job.getInstance(conf, "Popular Users");
        jobH.setJarByClass(TaskH.class);
        MultipleInputs.addInputPath(jobH, new Path("/user/cs4433/project1/input//friends.csv"), TextInputFormat.class, TaskH.MorePopularMapper.class);
        MultipleInputs.addInputPath(jobH, new Path("/user/cs4433/project1/input/pages.csv"), TextInputFormat.class, TaskH.NameMapper.class);
        FileOutputFormat.setOutputPath(jobH, new Path("/user/cs4433/project1/output/taskH"));

        jobH.setMapperClass(TaskH.MorePopularMapper.class);
        jobH.setReducerClass(TaskH.MorePopularReducer.class);
        jobH.setMapOutputKeyClass(Text.class);
        jobH.setMapOutputValueClass(Text.class);
        jobH.setOutputKeyClass(Text.class);
        jobH.setOutputValueClass(IntWritable.class);

        long timeStartH = System.currentTimeMillis();
        boolean jobHSuccess = jobH.waitForCompletion(true);
        long timeFinishH = System.currentTimeMillis();
        double secondsH = (timeFinishH - timeStartH) / 1000.0;

        // Final Prints
        System.out.println("Same Nationality job " + (jobASuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsA + " seconds");

        System.out.println("Top 10 Accessed Pages job " + (jobBSuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsB + " seconds");

        System.out.println("Citizens Per Country job " + (jobCSuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsC + " seconds");

        System.out.println("Connectedness Factor job " + (jobDSuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsD + " seconds");

        System.out.println("Favorites Analysis job " + (jobESuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsE + " seconds");

        System.out.println("Friends Analysis job " + (jobFSuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsF + " seconds");

        System.out.println("Disconnected Users job " + (jobGSuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsG + " seconds");

        System.out.println("Popular Users job " + (jobHSuccess ? "Succeeded" : "Failed"));
        System.out.println(secondsH + " seconds");

        // Final Exit
        if (jobASuccess && jobBSuccess && jobCSuccess && jobDSuccess && jobESuccess && jobFSuccess && jobGSuccess && jobHSuccess) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
