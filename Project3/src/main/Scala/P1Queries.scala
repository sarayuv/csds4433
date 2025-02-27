import org.apache.spark.{SparkConf, SparkContext}

object Queries {
  def main(args: Array[String]): Unit = {
    // Query 1
    val conf = new SparkConf().setAppName("Query1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // load the Meta-Event file into an RDD
    val metaEventFile = "src/main/data/Meta-Event.csv"
    val metaEventRDD = sc.textFile(metaEventFile)

    // remove header from the RDD
    val header = metaEventRDD.first()
    val dataRDD = metaEventRDD.filter(line => line != header)

    // split each line into columns and filter rows where pi.test = "sick"
    val sickPeopleRDD = dataRDD.map { line =>
      val columns = line.split(",")
      (columns(0), columns(1), columns(2), columns(3)) // (id, name, table, test)
    }.filter { case (_, _, _, test) => test == "sick" }

    // save results
    val outputDir = "output/sick_people"
    sickPeopleRDD.map { case (id, name, table, test) =>
      s"$id,$name,$table,$test" // convert to CSV format
    }.saveAsTextFile(outputDir)

    sc.stop()
  }
}
