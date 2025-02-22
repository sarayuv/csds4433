import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class KMeans {
    public static final double THRESHOLD = 0.01; // Convergence threshold

    // Define a class to represent a point in the dataset
    public static class Point {
        public double x;
        public double y;

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public String toString() {
            return x + "," + y;
        }

        // Calculate Euclidean distance between two points
        public double distanceTo(Point other) {
            return Math.sqrt(Math.pow(x - other.x, 2) + Math.pow(y - other.y, 2));
        }
    }

    // Define a class to represent a cluster centroid
    public static class Centroid extends Point {
        public int id;

        public Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }

        @Override
        public String toString() {
            return "Cluster ID: " + id + ", Coordinates: " + super.toString();
        }
    }

    // Read points from a file
    public static List<Point> readPoints(String filePath) throws IOException {
        List<Point> points = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Split the line on tabs or commas
                String[] parts = line.split("[\t,]"); // Handles both tabs and commas
                double x = Double.parseDouble(parts[0]);
                double y = Double.parseDouble(parts[1]);
                points.add(new Point(x, y));
            }
        }
        return points;
    }

    // Read centroids from a file
    public static List<Centroid> readCentroids(String filePath) throws IOException {
        List<Centroid> centroids = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int id = 0;
            while ((line = reader.readLine()) != null) {
                // Split the line on tabs or commas
                String[] parts = line.split("[\t,]"); // Handles both tabs and commas
                double x = Double.parseDouble(parts[0]);
                double y = Double.parseDouble(parts[1]);
                centroids.add(new Centroid(id++, x, y));
            }
        }
        return centroids;
    }

    // Mapper class for K-means
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
        private final List<Centroid> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read centroids from the seeds file
            Path seedsPath = new Path("input/seeds.txt");
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
                double distance = point.distanceTo(centroid);
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

    // Combiner class for optimization
    public static class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {
        @Override
        protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            double sumX = 0.0;
            double sumY = 0.0;
            int count = 0;

            // Aggregate points locally to reduce data sent to the reducer
            for (Point point : values) {
                sumX += point.x;
                sumY += point.y;
                count++;
            }

            double avgX = sumX / count;
            double avgY = sumY / count;

            // Emit the aggregated point
            context.write(key, new Point(avgX, avgY));
        }
    }

    // Main control program
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: KMeansClustering <dataset path> <seeds path> <output path> <R>");
            System.exit(1);
        }

        // Parse command-line arguments
        String datasetPath = args[0]; // Input dataset path
        String seedsPath = args[1]; // Path to the seeds file
        String outputPath = args[2]; // Path to the output directory
        int R = Integer.parseInt(args[3]); // Maximum number of iterations

        // Read points and centroids
        List<Point> points = readPoints(datasetPath);
        List<Centroid> centroids = readCentroids(seedsPath);

        // Run K-means clustering
        boolean converged = false;
        for (int iteration = 0; iteration < R; iteration++) { // Max 20 iterations
            List<Centroid> newCentroids = new ArrayList<>();

            // Assign points to the nearest centroid
            for (Centroid centroid : centroids) {
                double sumX = 0.0;
                double sumY = 0.0;
                int count = 0;

                for (Point point : points) {
                    if (centroid.id == findNearestCentroid(point, centroids).id) {
                        sumX += point.x;
                        sumY += point.y;
                        count++;
                    }
                }

                // Calculate the new centroid
                if (count > 0) {
                    double newX = sumX / count;
                    double newY = sumY / count;
                    newCentroids.add(new Centroid(centroid.id, newX, newY));
                } else {
                    newCentroids.add(centroid); // Keep the old centroid if no points are assigned
                }
            }

            // Check for convergence
            converged = true;
            for (int i = 0; i < centroids.size(); i++) {
                if (centroids.get(i).distanceTo(newCentroids.get(i)) > THRESHOLD) {
                    converged = false;
                    break;
                }
            }

            if (converged) {
                System.out.println("Converged at iteration " + (iteration + 1));
                break;
            }

            // Update centroids for the next iteration
            centroids = newCentroids;
        }

        // Write the final centroids to the output file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath + "/centroids.txt"))) {
            for (Centroid centroid : centroids) {
                writer.write(centroid.toString());
                writer.newLine();
            }
        }
    }

    // Find the nearest centroid for a given point
    private static Centroid findNearestCentroid(Point point, List<Centroid> centroids) {
        Centroid nearest = null;
        double minDistance = Double.MAX_VALUE;

        for (Centroid centroid : centroids) {
            double distance = point.distanceTo(centroid);
            if (distance < minDistance) {
                minDistance = distance;
                nearest = centroid;
            }
        }

        return nearest;
    }
}
