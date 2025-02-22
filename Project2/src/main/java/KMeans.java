import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
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

        public Point() {
            this(0.0, 0.0);
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
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path seedsPath = new Path("/user/cs4433/project2/input/seeds.txt");

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
            String[] parts = value.toString().split(",");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);
            Point point = new Point(x, y);

            // Find nearest centroid
            int nearestCentroidId = findNearestCentroid(point);

            // Output (centroid ID, point)
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

    public static class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {
        @Override
        protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            double sumX = 0.0;
            double sumY = 0.0;

            // Aggregate points locally
            for (Point point : values) {
                sumX += point.x;
                sumY += point.y;
            }

            // Output aggregated point
            context.write(key, new Point(sumX, sumY));
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Centroid> {
        @Override
        protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            double sumX = 0.0;
            double sumY = 0.0;
            int count = 0;

            // Aggregate points
            for (Point point : values) {
                sumX += point.x;
                sumY += point.y;
                count++;
            }

            // Compute new centroid
            double newX = sumX / count;
            double newY = sumY / count;

            // Output new centroid
            context.write(key, new Centroid(key.get(), newX, newY));
        }
    }

    public static List<Point> readPoints(FileSystem fs, Path filePath) throws IOException {
        List<Point> points = new ArrayList<>();
        try (FSDataInputStream inputStream = fs.open(filePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                double x = Double.parseDouble(parts[0]);
                double y = Double.parseDouble(parts[1]);
                points.add(new Point(x, y));
            }
        }
        return points;
    }

    public static List<Centroid> readCentroids(FileSystem fs, Path filePath) throws IOException {
        List<Centroid> centroids = new ArrayList<>();
        try (FSDataInputStream inputStream = fs.open(filePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            int id = 0;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
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

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 4) {
            System.err.println("Usage: hadoop/bin/hadoop jar <jar path> KMeans <R> [--advanced]");
            System.exit(1);
        }

        int R = Integer.parseInt(args[1]);

        // Determine if advanced algorithm is being used
        boolean isAdvancedAlgorithm = args.length == 3 && args[2].equals("--advanced");

        Configuration conf = new Configuration();

        Path inputPath = new Path("/user/cs4433/project2/input/dataset.txt");
        Path outputPath = new Path("/user/cs4433/project2/output");

        // Delete output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // Redirect System.out to a file
        Path outputFilePath = new Path(outputPath, "output.txt");
        try (FSDataOutputStream outputStream = fs.create(outputFilePath); PrintStream fileOut = new PrintStream(outputStream)) {
            System.setOut(fileOut);

            // Read points and centroids
            List<Point> points = readPoints(fs, inputPath);
            List<Centroid> centroids = readCentroids(fs, new Path("/user/cs4433/project2/input/seeds.txt"));

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

            // Write final centroids to output file in HDFS
            Path centroidsOutputPath = new Path(outputPath, "centroids.txt");
            try (FSDataOutputStream centroidsOutputStream = fs.create(centroidsOutputPath);
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(centroidsOutputStream))) {
                for (Centroid centroid : centroids) {
                    writer.write(centroid.toString());
                    writer.newLine();
                }
            }
        }

        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeans.class);

        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Point.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
