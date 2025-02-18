import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import javax.xml.soap.Text;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * running the program:
 * hadoop jar KMeans.jar KMeansDriver /user/hadoop/dataset.txt /user/hadoop/output 10 /user/hadoop/seeds.txt
 */

public class KMeans {
    static class Point {
        int x, y;

        Point(int x, int y) {
            this.x = x;
            this.y = y;
        }

        double distanceTo(Point other) {
            return Math.sqrt(Math.pow(this.x - other.x, 2) + Math.pow(this.y - other.y, 2));
        }

        @Override
        public String toString() {
            return x + "," + y;
        }

        public static Point fromString(String s) {
            String[] parts = s.split(",");
            return new Point(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        }
    }

    // MAPPER
    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        private List<Point> centroids = new ArrayList<>();

        // Initialize the centroids from the seed file
        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            String seedFilePath = conf.get("seedFile");
            if (seedFilePath == null) {
                throw new IOException("Seed file path not provided in configuration");
            }

            Path seedPath = new Path(seedFilePath);
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(seedPath);
            BufferedReader reader = new BufferedReader(new FileReader(seedPath.toString()));

            String line;
            while ((line = reader.readLine()) != null) {
                centroids.add(Point.fromString(line));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Point point = Point.fromString(value.toString());
            int closestCentroid = findClosestCentroid(point);
            context.write(new IntWritable(closestCentroid), new Text(point.toString()));
        }

        private int findClosestCentroid(Point point) {
            double minDistance = Double.MAX_VALUE;
            int closestCentroid = -1;

            for (int i = 0; i < centroids.size(); i++) {
                double dist = point.distanceTo(centroids.get(i));
                if (dist < minDistance) {
                    minDistance = dist;
                    closestCentroid = i;
                }
            }
            return closestCentroid;
        }
    }

    // REDUCER
    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumX = 0, sumY = 0, count = 0;

            // Compute the new centroid
            for (Text val : values) {
                Point point = Point.fromString(val.toString());
                sumX += point.x;
                sumY += point.y;
                count++;
            }

            // Compute the new centroid
            if (count > 0) {
                int newX = sumX / count;
                int newY = sumY / count;
                context.write(key, new Text(newX + "," + newY));
            }
        }
    }

    // OPTIMIZED REDUCER WITH COMBINER
    public static class KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumX = 0, sumY = 0, count = 0;

            for (Text val : values) {
                Point point = Point.fromString(val.toString());
                sumX += point.x;
                sumY += point.y;
                count++;
            }

            // Emit partial results
            if (count > 0) {
                context.write(key, new Text(sumX + "," + sumY + "," + count));
            }
        }
    }

    // DRIVER
    public static class KMeansDriver {
        private static List<Point> previousCentroids = new ArrayList<>();

        public static void main(String[] args) throws Exception {
            if (args.length < 3) {
                System.err.println("Usage: KMeansDriver <input path> <output path> <num iterations> <seed file>");
                System.exit(-1);
            }

            Configuration conf = new Configuration();
            conf.set("seedFile", "/user/cs4433/project2/input/seeds.txt");  // Path to the seed file with initial centroids

            Path input = new Path(args[0]);
            Path output = new Path(args[1]);
            int maxIterations = Integer.parseInt(args[2]);
            boolean converged = false;

            for (int i = 0; i < maxIterations; i++) {
                Job job = Job.getInstance(conf, "KMeans Iteration " + i);
                job.setJarByClass(KMeansDriver.class);

                // Set the paths for input and output
                FileInputFormat.addInputPath(job, input);
                FileOutputFormat.setOutputPath(job, output);

                // Set the Mapper and Reducer
                job.setMapperClass(KMeansMapper.class);
                job.setCombinerClass(KMeansCombiner.class); // Optional optimization using combiner
                job.setReducerClass(KMeansReducer.class);

                // Set output key/value types
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(Text.class);

                if (!job.waitForCompletion(true)) {
                    System.exit(1);
                }

                // Check for convergence after this iteration
                if (checkConvergence(input, output)) {
                    converged = true;
                    break;
                }

                // Update input for the next iteration (use the output from this iteration as input)
                input = output;
                output = new Path("iteration_" + (i + 1));  // For the next iteration output
            }

            if (converged) {
                System.out.println("KMeans converged early.");
            } else {
                System.out.println("KMeans completed " + maxIterations + " iterations.");
            }
        }

        // convergence checking logic
        private static boolean checkConvergence(Path input, Path output) throws IOException {
            List<Point> currentCentroids = readCentroidsFromFile(output);

            // Check if centroids have changed significantly
            double threshold = 0.001;  // Define the threshold for convergence
            boolean converged = true;

            if (previousCentroids.size() == currentCentroids.size()) {
                for (int i = 0; i < previousCentroids.size(); i++) {
                    double distance = previousCentroids.get(i).distanceTo(currentCentroids.get(i));
                    if (distance > threshold) {
                        converged = false;
                        break;
                    }
                }
            } else {
                converged = false;  // If the number of centroids is different, they haven't converged
            }

            // Store the current centroids as the previous centroids for the next iteration
            previousCentroids = currentCentroids;

            return converged;
        }

        // Utility to read centroids from the output file
        private static List<Point> readCentroidsFromFile(Path output) throws IOException {
            List<Point> centroids = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new FileReader(output.toString() + "/part-r-00000"));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                Point centroid = Point.fromString(parts[1]);
                centroids.add(centroid);
            }
            return centroids;
        }
    }
}
