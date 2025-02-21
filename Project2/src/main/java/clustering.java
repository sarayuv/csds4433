import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
public class clustering {
    // Define constants for the number of clusters (K) and max iterations (R)
    public static final int K = 3; // Number of clusters
    public static final int R = 20; // Max iterations
    public static final double THRESHOLD = 0.01; // Convergence threshold

    // Define a class to represent a point in the dataset
    public static class Point implements Writable {
        public double x;
        public double y;

        public Point() {}

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(x);
            out.writeDouble(y);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            x = in.readDouble();
            y = in.readDouble();
        }

        @Override
        public String toString() {
            return x + "," + y;
        }
    }

    // Define a class to represent a cluster centroid
    public static class Centroid extends Point {
        public int id;

        public Centroid() {}

        public Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }

        @Override
        public String toString() {
            return id + "," + super.toString();
        }
    }

    // Mapper class for K-means
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
        private List<Centroid> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Hardcoded seeds path
            Path seedsPath = new Path("/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project2/input/seeds.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(seedsPath)))) {
                String line;
                int id = 0;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    double x = Double.parseDouble(parts[0]);
                    double y = Double.parseDouble(parts[1]);
                    centroids.add(new Centroid(id++, x, y));
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the point from the input line
            String[] parts = value.toString().split(",");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);
            Point point = new Point(x, y);

            // Find the nearest centroid
            int nearestCentroidId = findNearestCentroid(point);

            // Emit the nearest centroid ID and the point
            context.write(new IntWritable(nearestCentroidId), point);
        }

        private int findNearestCentroid(Point point) {
            int nearestCentroidId = -1;
            double minDistance = Double.MAX_VALUE;

            for (Centroid centroid : centroids) {
                double distance = Math.pow(point.x - centroid.x, 2) + Math.pow(point.y - centroid.y, 2);
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCentroidId = centroid.id;
                }
            }

            return nearestCentroidId;
        }
    }

    // Reducer class for K-means
    public static class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Centroid> {
        @Override
        protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            double sumX = 0.0;
            double sumY = 0.0;
            int count = 0;

            // Calculate the new centroid by averaging the points
            for (Point point : values) {
                sumX += point.x;
                sumY += point.y;
                count++;
            }

            double newX = sumX / count;
            double newY = sumY / count;

            // Emit the new centroid
            context.write(key, new Centroid(key.get(), newX, newY));
        }
    }

    // Main control program
    public static void main(String[] args) throws Exception {
        // Hardcoded input and output paths
        String datasetPath = "/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project2/input/dataset.txt";
        String outputPath = "/Users/kashvi/Desktop/kAsHvI/college/Big Data/Project2/output";

        Configuration conf = new Configuration();

        // Delete the output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }

        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(clustering.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Point.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(datasetPath)); // Input dataset
        FileOutputFormat.setOutputPath(job, new Path(outputPath)); // Output directory

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
