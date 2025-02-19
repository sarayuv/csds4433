import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * running the program
 * a) single-iteration K-means -> set MAX_ITERATIONS = 1
 * b) multi-iteration K-means -> set MAX_ITERATIONS = 6
 * c) advanced K-means with convergence check -> set MAX_ITERATIONS = 20
 * d) output variations -> run with centers or points as a command-line argument to control the output
 *    hadoop jar KMeans.jar KMeans centers  # Output cluster centers
 *    hadoop jar KMeans.jar KMeans points   # Output clustered data points
 */

public class KMeans {
    private static final String SEED_PATH = "input/seeds.txt";
    private static final String DATA_PATH = "input/dataset.txt";
    private static final String OUTPUT_PATH = "output/";
    private static final int MAX_ITERATIONS = 20;
    private static final double THRESHOLD = 0.001;

    // Mapper class
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            Path centroidPath = new Path(SEED_PATH);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidPath)));
            String line;
            while ((line = br.readLine()) != null) {
                centroids.add(parsePoint(line));
            }
            br.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double[] point = parsePoint(value.toString());
            int nearestCentroid = findNearestCentroid(point);
            context.write(new IntWritable(nearestCentroid), value);
        }

        private int findNearestCentroid(double[] point) {
            int nearestIndex = 0;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double distance = 0.0;
                for (int j = 0; j < point.length; j++) {
                    distance += Math.pow(point[j] - centroids.get(i)[j], 2);
                }
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestIndex = i;
                }
            }
            return nearestIndex;
        }

        private double[] parsePoint(String line) {
            String[] tokens = line.split(",");
            double[] point = new double[tokens.length];
            for (int i = 0; i < tokens.length; i++) {
                point[i] = Double.parseDouble(tokens[i]);
            }
            return point;
        }
    }

    // Reducer class
    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<double[]> points = new ArrayList<>();
            int dimensions = 0;

            for (Text value : values) {
                double[] point = parsePoint(value.toString());
                dimensions = point.length;
                points.add(point);
            }

            double[] newCentroid = new double[dimensions];
            for (double[] point : points) {
                for (int i = 0; i < dimensions; i++) {
                    newCentroid[i] += point[i];
                }
            }
            for (int i = 0; i < dimensions; i++) {
                newCentroid[i] /= points.size();
            }

            context.write(key, new Text(arrayToString(newCentroid)));
        }

        private static double[] parsePoint(String line) {
            String[] tokens = line.split(",");
            double[] point = new double[tokens.length];
            for (int i = 0; i < tokens.length; i++) {
                point[i] = Double.parseDouble(tokens[i]);
            }
            return point;
        }

        private static String arrayToString(double[] arr) {
            StringBuilder sb = new StringBuilder();
            for (double val : arr) {
                sb.append(val).append(",");
            }
            return sb.substring(0, sb.length() - 1);
        }
    }

    // Combiner class (for optimization)
    public static class KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<double[]> points = new ArrayList<>();
            int dimensions = 0;

            for (Text value : values) {
                double[] point = KMeansReducer.parsePoint(value.toString());
                dimensions = point.length;
                points.add(point);
            }

            double[] newCentroid = new double[dimensions];
            for (double[] point : points) {
                for (int i = 0; i < dimensions; i++) {
                    newCentroid[i] += point[i];
                }
            }
            for (int i = 0; i < dimensions; i++) {
                newCentroid[i] /= points.size();
            }

            context.write(key, new Text(KMeansReducer.arrayToString(newCentroid)));
        }
    }

    // Method to check convergence
    private static boolean checkConvergence(String prevPath, String newPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path prevCentroidPath = new Path(prevPath + "/part-r-00000");
        Path newCentroidPath = new Path(newPath + "/part-r-00000");

        if (!fs.exists(prevCentroidPath) || !fs.exists(newCentroidPath)) {
            return false;
        }

        BufferedReader prevReader = new BufferedReader(new InputStreamReader(fs.open(prevCentroidPath)));
        BufferedReader newReader = new BufferedReader(new InputStreamReader(fs.open(newCentroidPath)));

        String prevLine, newLine;
        while ((prevLine = prevReader.readLine()) != null && (newLine = newReader.readLine()) != null) {
            double[] prevCentroid = KMeansReducer.parsePoint(prevLine.split("\t")[1]);
            double[] newCentroid = KMeansReducer.parsePoint(newLine.split("\t")[1]);

            double distance = 0.0;
            for (int i = 0; i < prevCentroid.length; i++) {
                distance += Math.pow(prevCentroid[i] - newCentroid[i], 2);
            }

            if (distance > THRESHOLD) {
                prevReader.close();
                newReader.close();
                return false;
            }
        }

        prevReader.close();
        newReader.close();
        return true;
    }

    // Main method to run KMeans
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("centroid.path", SEED_PATH);

        boolean converged = false;
        int iteration = 0;

        while (!converged && iteration < MAX_ITERATIONS) {
            Job job = Job.getInstance(conf, "KMeans Iteration " + iteration);
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class); // Add combiner for optimization
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(DATA_PATH));
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + iteration));
            job.waitForCompletion(true);

            converged = checkConvergence(OUTPUT_PATH + (iteration - 1), OUTPUT_PATH + iteration);
            iteration++;
        }

        // Output variations
        if (args.length > 0 && args[0].equals("centers")) {
            // Output only cluster centers and convergence status
            System.out.println("Converged: " + converged);
            Path finalCentroidPath = new Path(OUTPUT_PATH + (iteration - 1) + "/part-r-00000");
            FileSystem fs = FileSystem.get(conf);
            BufferedReader finalReader = new BufferedReader(new InputStreamReader(fs.open(finalCentroidPath)));
            String line;
            while ((line = finalReader.readLine()) != null) {
                System.out.println(line);
            }
            finalReader.close();
        } else if (args.length > 0 && args[0].equals("points")) {
            // Output the final clustered data points along with their cluster centers
            Path finalOutputPath = new Path(OUTPUT_PATH + (iteration - 1) + "/part-r-00000");
            FileSystem fs = FileSystem.get(conf);
            BufferedReader finalReader = new BufferedReader(new InputStreamReader(fs.open(finalOutputPath)));
            String line;
            while ((line = finalReader.readLine()) != null) {
                System.out.println(line);
            }
            finalReader.close();
        }
    }
}
