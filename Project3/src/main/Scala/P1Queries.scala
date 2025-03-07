import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}

object P1Queries {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("P1Queries").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val megaEventFile = "src/main/data/Mega-Event.csv"
    val megaEventRDD = sc.textFile(megaEventFile)
    val headerm = megaEventRDD.first()
    val mDataRDD = megaEventRDD.filter(line => line != headerm)

    val megaEventNoDisFile = "src/main/data/Mega-Event-No-Disclosure.csv"
    val megaEventNoDisRDD = sc.textFile(megaEventNoDisFile)
    val headermn = megaEventNoDisRDD.first()
    val mndDataRDD = megaEventNoDisRDD.filter(line => line != headermn)

    val reportedIllnessesFile = "src/main/data/Reported-Illnesses.csv"
    val reportedIllnessesRDD = sc.textFile(reportedIllnessesFile)
    val headerri = reportedIllnessesRDD.first()
    val riDataRDD = reportedIllnessesRDD.filter(line => line != headerri)


    // Query 1
    val sickPeopleRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(0), columns(1), columns(2), columns(3))
    }.filter { case (_, _, _, test) => test == "sick" }

    val outputDir1 = "output/Problem1/query1"
    sickPeopleRDD.map { case (id, name, table, test) =>
      s"$id,$name,$table,$test"
    }.saveAsTextFile(outputDir1)


    // Query 2
    val idRDD = mndDataRDD.map { line =>
      val col = line.split(",")
      (col(0), (col(1), col(2)))
    }

    val illRDD = riDataRDD.map { line =>
      val col = line.split(",")
      (col(0), col(1))
    }

    val join2RDD = idRDD.join(illRDD).map {
      case (id, ((name, table), test)) => (id, name, table, test)
    }

    val outputDir2 = "output/Problem1/query2"
    join2RDD.map { case (id, name, table, test) =>
      s"$id,$name,$table,$test"
    }.saveAsTextFile(outputDir2)


    // Query 3
    val tableRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(2), columns(3))
    }.filter { case (_, test) => test == "sick" }

    val notSickRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(2), (columns(0), columns(1)))
    }

    val sickTablejoin = tableRDD
      .distinct()
      .join(notSickRDD)
      .map {
        case (table, (_, (id, name))) => (id, name)
      }
      .distinct()

    val outputDir3 = "output/Problem1/query3"
    sickTablejoin.map { case (id, name) =>
      s"$id,$name"
    }.saveAsTextFile(outputDir3)


    // Query 4
    val healthyTableRDD = mDataRDD.map { line =>
      val columns = line.split(",")
      (columns(2), columns(3))
    }.groupByKey().map { case (table, tests) =>
      val sick = tests.exists(_ == "sick")
      val num = tests.size
      val flag = if (sick) "concern" else "healthy"
      (table, num, flag)
    }

    val outputDir4 = "output/Problem1/query4"
    healthyTableRDD.map { case (table, num, flag) =>
      s"$table,$num,$flag"
    }.saveAsTextFile(outputDir4)


    // Query 5
    val values = mndDataRDD.map { line =>
      val columns = line.split(",")
      (columns(0), (columns(2), columns(1)))
    }

    val illRDDMapped = illRDD.map { case (id, _) => (id, 1) }

    val sickPeopleTables = values.join(illRDDMapped).map {
      case (id, ((table, name), _)) => table
    }.distinct()

    val healthyPeople = values.leftOuterJoin(illRDDMapped).filter {
      case (id, ((table, name), testOpt)) => testOpt.isEmpty
    }.map {
      case (id, ((table, name), _)) => (table, (id, name))
    }

    val sickTablesWithFlag = sickPeopleTables.map(table => (table, 1))

    val healthyPeopleAtSickTables = healthyPeople.join(sickTablesWithFlag).map {
      case (table, ((id, name), _)) => (id, name)
    }.distinct()

    val outputDir5 = "output/Problem1/query5"
    healthyPeopleAtSickTables.map { case (id, name) =>
      s"$id,$name"
    }.saveAsTextFile(outputDir5)

    
    sc.stop()
  }
}
