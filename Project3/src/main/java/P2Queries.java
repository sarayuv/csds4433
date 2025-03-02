import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkSQLQueries {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQL - P2 Customer Purchases Analysis")
                .getOrCreate();

        String filePath = "src/main/data/customers_and_purchases.csv";
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath);

        // register DataFrame as a temporary SQL table
        df.createOrReplaceTempView("data_table");

        // Query 1
        String query1 = "SELECT * FROM data_table WHERE TransTotal <= 100";
        Dataset<Row> result1 = spark.sql(query1);
        result1.write().option("header", "true").csv("src/main/data/T1.csv");

        // Query 2
        String query2 = "SELECT TransNumItems, " +
                "MIN(TransTotal) AS min_total, " +
                "MAX(TransTotal) AS max_total, " +
                "percentile_approx(TransTotal, 0.5) AS median_total " +
                "FROM data_table " +
                "WHERE TransTotal <= 100 " +
                "GROUP BY TransNumItems";
        Dataset<Row> result2 = spark.sql(query2);
        result2.show();

        spark.stop();
    }
}
