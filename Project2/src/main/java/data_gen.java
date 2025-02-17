import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * compile: javac data_gen.java
 * run: java data_gen 5 dataset.txt seeds.txt
 */

public class data_gen {
    private static final int POINTS = 3000;
    private static final int MIN_VAL = 0;
    private static final int MAX_VAL = 5000;
    private static final int SEED_MIN = 0;
    private static final int SEED_MAX = 10000;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println();
            System.exit(1);
        }

        int K = Integer.parseInt(args[0]);
        String dataFile = args[1];
        String seedFile = args[2];

        generate(dataFile, POINTS, MIN_VAL, MAX_VAL);
        generateSeeds(seedFile, K, SEED_MIN, SEED_MAX);
    }

    private static void generate(String fileName, int size, int minVal, int maxVal) {
        Random random = new Random();

        try (FileWriter writer = new FileWriter(fileName)) {
            for (int i = 0; i < size; i++) {
                int x = minVal + random.nextInt(maxVal - minVal + 1);
                int y = minVal + random.nextInt(maxVal - minVal + 1);
                writer.write(x + "," + y + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing data: " + e.getMessage());
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
}
