import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkSQLQueries {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQL - P2 Customer Purchases Analysis")
                .getOrCreate();

        String customersPath = "src/main/data/customers.csv";
        String purchasesPath = "src/main/data/purchases.csv";

       
        Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(customersPath);

      
        Dataset<Row> purchases = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(purchasesPath);

        // register DataFrame as a temporary SQL table
        customers.createOrReplaceTempView("customers");
        purchases.createOrReplaceTempView("purchases");

        // Query 1
        String query1 = "SELECT * FROM purchases WHERE TransTotal <= 100";
        Dataset<Row> T1 = spark.sql(query1);
        T1.write().option("header", "true").csv("src/main/data/T1.csv");

        // Query 2
        String query2 = "SELECT TransNumItems, " +
                "MIN(TransTotal) AS min_total, " +
                "MAX(TransTotal) AS max_total, " +
                "percentile_approx(TransTotal, 0.5) AS median_total " +
                "FROM purchases " +
                "WHERE TransTotal <= 100 " +
                "GROUP BY TransNumItems";
        Dataset<Row> result2 = spark.sql(query2);
        result2.show();

        // Query 3
        String query3 = "SELECT c.CustID, c.Age, " +
                "SUM(p.TransNumItems) AS total_items, " +
                "SUM(p.TransTotal) AS total_amount " +
                "FROM customers c " +
                "JOIN purchases p ON c.CustID = p.CustID " +
                "WHERE c.Age BETWEEN 18 AND 21 AND p.TransTotal <= 100 " +
                "GROUP BY c.CustID, c.Age";
        Dataset<Row> T3 = spark.sql(query3);
        T3.write().option("header", "true").csv("src/main/data/T3.csv");

        // Query 4
        String query4 = "SELECT c.CustID, " +
                "SUM(p.TransTotal) AS total_spent, " +
                "c.Salary, c.Address " +
                "FROM customers c " +
                "JOIN purchases p ON c.CustID = p.CustID " +
                "WHERE p.TransTotal <= 100 " +
                "GROUP BY c.CustID, c.Salary, c.Address " +
                "HAVING SUM(p.TransTotal) > c.Salary";
        Dataset<Row> result4 = spark.sql(query4);
        result4.show();

        spark.stop();
    }
}
