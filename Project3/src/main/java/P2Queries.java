import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class P2Queries {
    public static void main(String[] args) {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQL - P2 Customer Purchases Analysis")
                .master("local[*]") // Run locally using all available cores
                .getOrCreate();

        // Define input file paths
        String customersPath = "src/main/data/customers.csv";
        String purchasesPath = "src/main/data/purchases.csv";

        // Define output directory
        String outputDir = "output/Problem2/";

        // Read customers data
        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(customersPath);

        // Read purchases data
        Dataset<Row> purchases = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(purchasesPath);

        // Register DataFrames as temporary SQL tables
        customers.createOrReplaceTempView("customers");
        purchases.createOrReplaceTempView("purchases");

        // Query 1: Filter purchases with TransTotal <= 100
        String query1 = "SELECT * FROM purchases WHERE TransTotal <= 100";
        Dataset<Row> T1 = spark.sql(query1);
        T1.write().option("header", "true").csv(outputDir + "T1");

        // Query 2: Calculate min, max, and median TransTotal for each TransNumItems
        String query2 = "SELECT TransNumItems, " +
                "MIN(TransTotal) AS min_total, " +
                "MAX(TransTotal) AS max_total, " +
                "percentile_approx(TransTotal, 0.5) AS median_total " +
                "FROM purchases " +
                "WHERE TransTotal <= 100 " +
                "GROUP BY TransNumItems";
        Dataset<Row> result2 = spark.sql(query2);
        result2.write().option("header", "true").csv(outputDir + "T2");

        // Query 3: Calculate total items and total amount for customers aged 18-21
        String query3 = "SELECT c.CustID, c.Age, " +
                "SUM(p.TransNumItems) AS total_items, " +
                "SUM(p.TransTotal) AS total_amount " +
                "FROM customers c " +
                "JOIN purchases p ON c.CustID = p.CustID " +
                "WHERE c.Age BETWEEN 18 AND 21 AND p.TransTotal <= 100 " +
                "GROUP BY c.CustID, c.Age";
        Dataset<Row> T3 = spark.sql(query3);
        T3.write().option("header", "true").csv(outputDir + "T3");

        // Query 4: Find customers who spent more than their salary
        String query4 = "SELECT c.CustID, " +
                "SUM(p.TransTotal) AS total_spent, " +
                "c.Salary, c.Address " +
                "FROM customers c " +
                "JOIN purchases p ON c.CustID = p.CustID " +
                "WHERE p.TransTotal <= 100 " +
                "GROUP BY c.CustID, c.Salary, c.Address " +
                "HAVING SUM(p.TransTotal) > c.Salary";
        Dataset<Row> result4 = spark.sql(query4);
        result4.write().option("header", "true").csv(outputDir + "T4");

        // Stop the SparkSession
        spark.stop();
    }
}
