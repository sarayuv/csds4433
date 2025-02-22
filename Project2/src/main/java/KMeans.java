import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class KMeans {
    public static final double THRESHOLD = 0.01;

    public static class Point implements Writable {
        public double x;
        public double y;

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

        public double euclideanDistance(Point other) {
            return Math.sqrt(Math.pow(x - other.x, 2) + Math.pow(y - other.y, 2));
        }
    }

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

    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
        private final List<Centroid> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Read centroids from seeds.txt
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

            // Find nearest centroid
            int nearestCentroidId = findNearestCentroid(point);

            // Output the nearest centroid ID and the point
            context.write(new IntWritable(nearestCentroidId), point);
        }

        private int findNearestCentroid(Point point) {
            int nearestCentroidId = -1;
            double minDistance = Double.MAX_VALUE;

            for (Centroid centroid : centroids) {
                double distance = point.euclideanDistance(centroid);
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCentroidId = centroid.id;
                }
            }

            return nearestCentroidId;
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Centroid> {
        @Override
        protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            double sumX = 0.0;
            double sumY = 0.0;
            int count = 0;

            // Calculate new centroid
            for (Point point : values) {
                sumX += point.x;
                sumY += point.y;
                count++;
            }

            double newX = sumX / count;
            double newY = sumY / count;

            // Output new centroid
            context.write(key, new Centroid(key.get(), newX, newY));
        }
    }

    public static class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {
        @Override
        protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            double sumX = 0.0;
            double sumY = 0.0;

            // Aggregate points locally to reduce data sent to reducer
            for (Point point : values) {
                sumX += point.x;
                sumY += point.y;
            }

            // Output aggregated point
            context.write(key, new Point(sumX, sumY));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: KMeans <dataset path> <seeds path> <output path> <R> [--advanced]");
            System.exit(1);
        }

        // Parse command-line arguments
        String datasetPath = args[0];
        String seedsPath = args[1];
        String outputPath = args[2];
        int R = Integer.parseInt(args[3]);

        // Determine if advanced algorithm is being used
        boolean isAdvancedAlgorithm = args.length > 4 && args[4].equals("--advanced");

        // Redirect System.out to a file
        PrintStream fileOut = new PrintStream(Files.newOutputStream(Paths.get(outputPath + "/output.txt")));
        System.setOut(fileOut);

        // Read points and centroids
        List<Point> points = readPoints(datasetPath);
        List<Centroid> centroids = readCentroids(seedsPath);

        // Run K-means clustering
        boolean converged = false;
        for (int iteration = 0; iteration < R; iteration++) {
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

                // Calculate new centroid
                if (count > 0) {
                    double newX = sumX / count;
                    double newY = sumY / count;
                    newCentroids.add(new Centroid(centroid.id, newX, newY));
                } else {
                    // Keep old centroid if no points are assigned
                    newCentroids.add(centroid);
                }
            }

            // Check for convergence (only in advanced algorithm)
            if (isAdvancedAlgorithm) {
                converged = true;
                for (int i = 0; i < centroids.size(); i++) {
                    if (centroids.get(i).euclideanDistance(newCentroids.get(i)) > THRESHOLD) {
                        converged = false;
                        break;
                    }
                }

                if (converged) {
                    System.out.println("Converged at iteration " + (iteration + 1));
                    break;
                }
            }

            // Update centroids for next iteration
            centroids = newCentroids;
        }

        // Output Variation (a): Cluster centers with convergence status
        if (isAdvancedAlgorithm) {
            System.out.println("Cluster Centers:");
            for (Centroid centroid : centroids) {
                System.out.println(centroid);
            }
            System.out.println("Converged: " + converged);
        }

        // Output Variation (b): Final clustered data points along with their cluster centers
        if (isAdvancedAlgorithm) {
            System.out.println("Final Clustered Data Points:");
            for (Point point : points) {
                Centroid nearestCentroid = findNearestCentroid(point, centroids);
                System.out.println("Point: " + point + ", Cluster: " + nearestCentroid.id);
            }
        }

        // Write final centroids to output file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath + "/centroids.txt"))) {
            for (Centroid centroid : centroids) {
                writer.write(centroid.toString());
                writer.newLine();
            }
        }

        fileOut.close();
    }

    public static List<Point> readPoints(String filePath) throws IOException {
        List<Point> points = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("[\t,]");
                double x = Double.parseDouble(parts[0]);
                double y = Double.parseDouble(parts[1]);
                points.add(new Point(x, y));
            }
        }
        return points;
    }

    public static List<Centroid> readCentroids(String filePath) throws IOException {
        List<Centroid> centroids = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int id = 0;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("[\t,]");
                double x = Double.parseDouble(parts[0]);
                double y = Double.parseDouble(parts[1]);
                centroids.add(new Centroid(id++, x, y));
            }
        }
        return centroids;
    }

    private static Centroid findNearestCentroid(Point point, List<Centroid> centroids) {
        Centroid nearest = null;
        double minDistance = Double.MAX_VALUE;

        for (Centroid centroid : centroids) {
            double distance = point.euclideanDistance(centroid);
            if (distance < minDistance) {
                minDistance = distance;
                nearest = centroid;
            }
        }

        return nearest;
    }
}
