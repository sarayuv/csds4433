import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class data_gen {
    private static final int POINTS = 3000;
    private static final int MIN_VAL = 0;
    private static final int MAX_VAL = 5000;
    private static final int SEED_MIN = 0;
    private static final int SEED_MAX = 10000;

    private static void generateDataset(String fileName, int size, int minVal, int maxVal) {
        Random random = new Random();

        try (FileWriter writer = new FileWriter(fileName)) {
            for (int i = 0; i < size; i++) {
                int x = minVal + random.nextInt(maxVal - minVal + 1);
                int y = minVal + random.nextInt(maxVal - minVal + 1);
                writer.write(x + "," + y + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing dataset: " + e.getMessage());
        }
    }

    private static void generateSeeds(String fileName, int K, int minVal, int maxVal) {
        Random random = new Random();

        try (FileWriter writer = new FileWriter(fileName)) {
            for (int i = 0; i < K; i++) {
                int x = minVal + random.nextInt(maxVal - minVal + 1);
                int y = minVal + random.nextInt(maxVal - minVal + 1);
                writer.write(x + "," + y + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing seeds: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java data_gen <K> <dataset.txt> <seeds.txt>");
            System.exit(1);
        }

        int K = Integer.parseInt(args[0]); // Number of clusters
        String dataFile = args[1]; // Dataset file path
        String seedFile = args[2]; // Seeds file path

        // Generate dataset and seeds
        generateDataset(dataFile, POINTS, MIN_VAL, MAX_VAL);
        generateSeeds(seedFile, K, SEED_MIN, SEED_MAX);

        System.out.println("Dataset and seeds generated successfully.");
    }
}
