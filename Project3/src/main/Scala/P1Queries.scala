import org.apache.spark.{SparkConf, SparkContext}

object P1Queries {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Query").setMaster("local[*]")
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
    val outputDir1 = "output/Problem1/query1"
    sickPeopleRDD.map { case (id, name, table, test) =>
      s"$id,$name,$table,$test" // convert to CSV format
    }.saveAsTextFile(outputDir1)


    // Query 2
    val idRDD = mndDataRDD.map { line =>
      val col = line.split(",")
      (col(0), (col(1), col(2))) // (id, (name, table))
    }

    val illRDD = riDataRDD.map { line =>
      val col = line.split(",")
      (col(0), col(1)) // (id, test)
    }

    val join2RDD = idRDD.join(illRDD).map {
      case (id, ((name, table), test)) => (id, name, table, test)
    }

    // save results
    val outputDir2 = "output/Problem1/query2"
    join2RDD.map { case (id, name, table, test) =>
      s"$id,$name,$table,$test" // convert to CSV format
    }.saveAsTextFile(outputDir2)


    // Query 3
    val tableRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(2), 1) // (table, 1)
    }.filter { case (_, test) => test == "sick" }

    val notSickRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(2), (columns(0), columns(1))) // (table, (id, name))
    }.filter { case (_, test) => test == "not-sick" }

    val sickTablejoin = tableRDD.join(notSickRDD).map {
      case (table, (_, (id, name))) => (id, name)
    }.distinct()

    // Save results
    val outputDir3 = "output/Problem1/query3"
    sickTablejoin.map { case (id, name) =>
      s"$id,$name" // Convert to CSV format
    }.saveAsTextFile(outputDir3)


    // Query 4
    val healthyTableRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(2), columns(3)) // (table, test)
    }.groupByKey().map { case (table, tests) =>
      val sick = tests.exists(_ == "sick")
      val num = tests.size
      val flag = if (sick) "concern" else "healthy"
      (table, num, flag)
    }

    // save results
    val outputDir4 = "output/Problem1/query4"
    healthyTableRDD.map { case (table, num, flag) =>
      s"$table,$num,$flag" // convert to CSV format
    }.saveAsTextFile(outputDir4)


    
    // Query 5
    val illRDD = riDataRDD.map { line =>
      val columns = line.split(",")
      (columns(0), 1) // (id, 1)
    }

    val values = mndDataRDD.map { line =>
      val columns = line.split(",")
      (columns(0), (columns(2), columns(1))) // (id, (table, name))
    }

    val sickTRDD = values.join(illRDD).map {case (_, _, table, _) => (table, 1)}

    val unsickPeopleJoin = values.map{
        case (id, (name, table)) => (table, (id, name))
      }.leftOuterJoin(sickTRDD).map {
      case (id, (_, name)) => (id, name)
    }

    val healthyShareTable = unsickPeopleJoin.filter{
      case (_, ((id, name), test) => test == "not-sick"
    }.map { case (_, ((id, name), _)) => (id, name)
    }.distinct()


    sc.stop()
  }
}
