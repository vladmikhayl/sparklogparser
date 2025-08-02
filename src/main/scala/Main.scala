import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]): Unit = {

    val targetDocId = "ACC_45616"

    val spark = SparkSession.builder()
      .appName("LogProcessor")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val logsPath = "src/main/resources/logs/*"

    spark.conf.set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", 10000)

    val logs = readLogs(spark, logsPath).cache()

    val cardSearchCount = countCardSearches(logs, targetDocId)

    println(s"[METRIC 1] Кол-во раз, когда документ $targetDocId искали через карточку: $cardSearchCount")

    val qsDocOpensDF = countQSDocOpensByDay(logs)
      .toDF("document_id", "date", "open_count")
      .select($"date", $"document_id", $"open_count")
      .orderBy($"date", $"document_id")
      .cache()

    println("[METRIC 2] Открытия документов, найденных через быстрый поиск (QS):")

    qsDocOpensDF.show(100, truncate = false)

    qsDocOpensDF.write
      .option("header", "true")
      .mode("overwrite")
      .csv("output/qs_doc_opens")

    println("[METRIC 2] Полная таблица сохранена в формате csv (см. папку output/qs_doc_opens)")

  }

  private def readLogs(spark: SparkSession, path: String): Dataset[Seq[String]] = {
    import spark.implicits._

    val cores = Runtime.getRuntime.availableProcessors()

    spark.sparkContext
      .wholeTextFiles(path, minPartitions = cores)
      .values
      .map(_.split("\n").map(_.trim).toSeq)
      .toDS()
  }

  private def countCardSearches(
                                 logs: Dataset[Seq[String]],
                                 targetDocId: String
                               ): Long = {
    import logs.sparkSession.implicits._

    logs
      .filter(_.exists(line => line.startsWith("$0") && line.contains("ACC_45616")))
      .map { sessionLines =>
      var count = 0
      var insideSearch = false

      for (line <- sessionLines) {
        if (line.startsWith("CARD_SEARCH_START")) {
          insideSearch = true
        } else if (insideSearch && line.startsWith("$0") && line.contains(targetDocId)) {
          count += 1
        } else if (line.startsWith("CARD_SEARCH_END")) {
          insideSearch = false
        }
      }

      count
    }.reduce(_ + _)
  }

  private def countQSDocOpensByDay(logs: Dataset[Seq[String]]): Dataset[(String, String, Long)] = {
    import logs.sparkSession.implicits._

    val rawEvents = logs.flatMap { sessionLines =>
        val qsSearchIds = scala.collection.mutable.Set[String]()
        val docOpenCounts = scala.collection.mutable.ListBuffer[(String, String, Int)]()
        var expectingSearchId = false

        // Оставляем только полезные строки: QS, результаты поиска и DOC_OPEN
        val usefulLines = sessionLines.filter { line =>
          line.startsWith("QS ") ||
            line.matches("^-?\\d+\\s+\\S+.*") || // результаты QS
            line.startsWith("DOC_OPEN")
        }

        for (line <- usefulLines) {
          if (expectingSearchId && line.matches("^-?\\d+\\s+\\S+.*")) {
            val searchId = line.split("\\s+").headOption
            searchId.foreach(qsSearchIds.add)
            expectingSearchId = false
          }
          else if (line.startsWith("QS ")) {
            expectingSearchId = true
          }
          else if (line.startsWith("DOC_OPEN")) {
            val parts = line.split("\\s+")
            if (parts.length >= 4) {
              val timestamp = parts(1)
              val qsId = parts(2)
              val docId = parts(3)
              val date = timestamp.split("_").headOption.getOrElse("UNKNOWN_DATE")

              if (qsSearchIds.contains(qsId)) {
                docOpenCounts.append((docId, date, 1))
              }
            }
          } else {
            expectingSearchId = false
          }
        }

        docOpenCounts
      }

    rawEvents
      .toDF("document_id", "date", "count")
      .groupBy($"document_id", $"date")
      .agg(sum("count").as("open_count"))
      .as[(String, String, Long)]
  }

}
