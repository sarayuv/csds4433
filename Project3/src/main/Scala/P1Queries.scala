import org.apache.spark.{SparkConf, SparkContext}

object Queries {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Query1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // load the Meta-Event file into an RDD
    val metaEventFile = "src/main/data/Meta-Event.csv"
    val metaEventRDD = sc.textFile(metaEventFile)
    // remove header from the RDD
    val headerm = metaEventRDD.first()
    val mDataRDD = metaEventRDD.filter(line => line != headerm)

    // load the Meta-Event file into an RDD
    val metaEventNoDisFile = "src/main/data/Meta-Event-No-Disclosure.csv"
    val metaEventNoDisRDD = sc.textFile(metaEventNoDisFile)
    // remove header from the RDD
    val headermn = metaEventNoDisRDD.first()
    val mndDataRDD = metaEventNoDisRDD.filter(line => line != headermn)

    // load the Reported-Illnesses file into an RDD
    val reportedIllnessesFile = "src/main/data/Reported-Illnesses.csv"
    val reportedIllnessesRDD = sc.textFile(reportedIllnessesFile)
    // remove header from the RDD
    val headerri = reportedIllnessesRDD.first()
    val riDataRDD = reportedIllnessesRDD.filter(line => line != headerri)


    // Query 1
    // split each line into columns and filter rows where pi.test = "sick"
    val sickPeopleRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(0), columns(1), columns(2), columns(3)) // (id, name, table, test)
    }.filter { case (_, _, _, test) => test == "sick" }

    // save results
    val outputDir1 = "output/sick_people"
    sickPeopleRDD.map { case (id, name, table, test) =>
      s"$id,$name,$table,$test" // convert to CSV format
    }.saveAsTextFile(outputDir1)


    // Query 2
    val idRDD = mndDataRDD.map { line =>
      val col = line.split(",")
      (col(0), col(1), col(2)) // (id, name, table)
    }

    val illRDD = reportedIllnessesRDD.map { line =>
      val col = line.split(",")
      (col(0), col(1)) // (id, test)
    }

    val join2RDD = idRDD.join(illRDD).map {
      case (id, name, table) => (id, name, table)
    }

    // save results
    val outputDir2 = "output/query2"
    join2RDD.map { case (id, name, table) =>
      s"$id,$name,$table" // convert to CSV format
    }.saveAsTextFile(outputDir2)


    // Query 3
    val tableRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(0), columns(1), columns(2), columns(3)) // (id, name, table, test)
    }.filter { case (_, _, _, test) => test == "sick"
    }.map { case (_, _, table, _) => (table, 1)}

    val notSickRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(0), columns(1), columns(2), columns(3)) // (id, name, table, test)
    }.filter { case (_, _, _, test) => test == "not-sick"
    }.map { case (id, name, table, _) => (table, (id, name))}

    val sickTablejoin = tableRDD.join(notSickRDD).map {
      case (_, id, name, _) => (id, name)
    }.distinct()

    // save results
    val outputDir3 = "output/query3"
    sickTablejoin.map { case (id, name) =>
      s"$id,$name" // convert to CSV format
    }.saveAsTextFile(outputDir3)


    // Query 4
    val healthyTableRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(0), columns(1), columns(2), columns(3)) // (id, name, table, test)
    }.map { case (_, _, table, test) => test => (table, test)
    }.groupByKey().map { case (table, tests) =>
      val sick = tests.exists(_ == "sick")
      val num = tests.size
      val flag = if (sick) "concern" else "healthy"
      (table, num, flag)
    }

    // save results
    val outputDir4 = "output/query4"
    Tablejoin.map { case (table, num, flag) =>
      s"$table,$num,$flag" // convert to CSV format
    }.saveAsTextFile(outputDir4)

// Query 5


    sc.stop()
  }
}
