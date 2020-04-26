import java.util.{Calendar, Properties}

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.linkedin.relevance.isolationforest.IsolationForest
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {
  val jdbcUrl = "jdbc:mysql://35.242.238.93:3306/bigdata" // "jdbc:mysql://104.155.100.184:3306/data"
  val username = "root"
  val password = "Aa1234567" // "liorseantal821"
  val tableName = "sample_data_table2"
  val KAFKA_TOPIC = "KAFKA_TOPIC"

  private val spark = SparkSession.builder
    .config("spark.sql.shuffle.partitions", "8") // Development Environment
    .config("spark.sql.codegen.wholeStage", "false")
    .master("local[*]") // Development Environment
    .appName("DiseaseDataC")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    //Subscribe to the Kafka stream
    var df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") // Development environment
      .option("subscribe", KAFKA_TOPIC)
      .option("maxOffsetsPerTrigger", "1000") // Development environment
      .option("startingOffsets", "earliest") // Development environment
      .load()

    //Unmarshalling the JSON records into a structured table
    val tableSchema = StructType(
      Array(StructField("datetime", TimestampType),
        StructField("kw", StringType),
        StructField("region", StringType),
        StructField("value", DoubleType)
      ))

    //| datetime | region | kw | value |
    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json")
      .withColumn("JSON", from_json(col("JSON"), tableSchema))
      .select("JSON.*")
    // +-------------------+------+-------------------+-----+
    // |           datetime|region|                 kw|value|
    // +-------------------+------+-------------------+-----+
    // |2020-01-01 00:01:00|    US|              Virus|  9.0|
    // |2020-01-01 00:01:00|    US|           Vomiting|  0.0|
    // |2020-01-01 00:02:00|    US|           Vomiting|  4.0|
    // |2020-01-01 00:02:00|    CN|              Mucus|  0.0|
    // |      ......       |    ..|       .......     | ... |


    // Summarize the minutely/hourly/daily data into a uniform daily table
    // Handle late data (up to 1 hour late)
    //| window | date | region | kw | value |
    df = df.withColumn("kw", regexp_replace($"kw", " ", "_"))
      .where($"region" === "US") // DEBUG
      .withWatermark("datetime", "1 hour") // Wait upto 1 hour for late data
      .groupBy(window($"datetime", "1 day"), // 1 day window (take only the current day)
        to_date($"datetime", fmt = "yyyy-MM-dd").as("date"), // Sum by date (remove time)
        $"region", $"kw")
      .agg(sum("value").as("value"))

    // +----------------------------------------+-----------+------+---------------+-----+
    // |                                 window |      date |region|             kw|value|
    // +----------------------------------------+-----------+------+---------------+-----+
    // |2020-01-01T00:00:00-2004-01-01T23:59:59 |2020-01-01 |    US|          Virus| 39.0|
    // |2020-01-01T00:00:00-2004-01-01T23:59:59 |2020-01-01 |    US|       Vomiting| 47.0|
    // |2020-01-01T00:00:00-2004-01-01T23:59:59 |2020-01-01 |    CN|          Mucus| 12.5|
    // |                ......                  |  ......   |    ..|     ......    | ... |


    df = df.drop("window")
    // Sink1 - Persist the data
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        persistBatchAtSql(batchDF, batchId)
      }
      .outputMode("append")
      .start()

    // Sink2 - trigger a batch processing on the
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        handleBatchProcessing(batchDF, batchId)
      }
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def persistBatchAtSql(batchDf: DataFrame, batchId: Long): Unit = {
    // save raw data in MySQL database
    batchDf.write.format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password)
      .mode(SaveMode.Append)
      .save()
  }


  def handleBatchProcessing(batchDf: DataFrame, batchId: Long): Unit = {
    println(Calendar.getInstance().getTime) // DEBUG
    val batchCount = batchDf.count()
    if (batchCount == 0) return // if the batch is empty - do nothing (happens only on development environment
    if (batchCount < 3) return // ensure history available, only for debug (create the table in production)
    // batchDf:
    // +-----------+------+---------------+-----+
    // |      date |region|             kw|value|
    // +-----------+------+---------------+-----+
    // |2020-01-01 |    US|          Virus| 39.0|
    // |2020-01-01 |    US|       Vomiting| 47.0|
    // |2020-01-01 |    CN|          Mucus| 12.5|
    // |  ......   |    ..|     ......    | ... |

    // fetch relevant history from the persisted database
    val thisDate = batchDf.first().getAs[java.sql.Date]("date")
    // thisDate = 2020-01-01

    // fetch history from DB
    val historyDf = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password)
      .load().where($"date" < lit(thisDate))
    // historyDf:
    // +-----------+------+---------------+-----+
    // |      date |region|             kw|value|
    // +-----------+------+---------------+-----+
    // |2019-12-31 |    US|          Virus| 19.5|
    // |2019-12-31 |    US|       Vomiting| 48.6|
    // |2019-12-31 |    CN|          Mucus| 20.2|
    // |  ......   |    ..|     ......    | ... |

    println("history dataframe")
    historyDf.show() // DEBUG

    // Combine the historical Dataframe with the newly received last day's Dataframe
    var df = batchDf.union(historyDf)
    //     .where($"region" === "US") // DEBUG
    println("batch dataframe + history dataframe")
    df.show() // DEBUG
    // +-----------+------+---------------+-----+
    // |      date |region|             kw|value|
    // +-----------+------+---------------+-----+
    // |2020-01-01 |    US|          Virus| 39.0|
    // |2020-01-01 |    US|       Vomiting| 47.0|
    // |2020-01-01 |    CN|          Mucus| 12.5|
    // |  ......   |    ..|     ......    | ... |
    // |2019-12-31 |    US|          Virus| 19.5|
    // |2019-12-31 |    US|       Vomiting| 48.6|
    // |2019-12-31 |    CN|          Mucus| 20.2|
    // |  ......   |    ..|     ......    | ... |

    // Calculate Trends using Moving Average
    df = df.withColumn("movingAverage", avg(col("value"))
      .over(Window.partitionBy(col("kw"), col("region"))
        .orderBy(col("date").asc)
        .rowsBetween(Long.MinValue, 0)))

    println("dataframe with trends")
    df.show()
    // +-----------+------+---------------+-----+-------------+
    // |      date |region|             kw|value|movingAverage|
    // +-----------+------+---------------+-----+-------------+
    // |2020-01-01 |    US|          Virus| 39.0|         30.5|
    // |2020-01-01 |    US|       Vomiting| 47.0|         14.1|
    // |2020-01-01 |    CN|          Mucus| 12.5|         12.2|
    // |  ......   |    ..|     ......    | ... |         ... |
    // |2019-12-31 |    US|          Virus| 19.5|         30.3|
    // |2019-12-31 |    US|       Vomiting| 48.6|         14.9|
    // |2019-12-31 |    CN|          Mucus| 20.2|         12.1|
    // |  ......   |    ..|     ......    | ... |         ... |


    // Remove the Trends
    // the average value could be zero, dividing by zero causing NaN
    // clean NaN and nulls with 1 (avg)
    df = df.withColumn("value", df.col("value") / df.col("movingAverage"))
      .drop("movingAverage")
      .withColumn("value",
        when($"value".isNull.or($"value" === lit(Double.NaN)), 1)
          .otherwise(col("value")))
    println("dataframe after remove trends")
    df.show()
    // +-----------+------+---------------+-----+
    // |      date |region|             kw|value|
    // +-----------+------+---------------+-----+
    // |2020-01-01 |    US|          Virus| 1.27|
    // |2020-01-01 |    US|       Vomiting| 3.33|
    // |2020-01-01 |    CN|          Mucus| 1.02|
    // |  ......   |    ..|     ......    | ... |
    // |2019-12-31 |    US|          Virus| 0.64|
    // |2019-12-31 |    US|       Vomiting| 3.26|
    // |2019-12-31 |    CN|          Mucus| 1.66|
    // |  ......   |    ..|     ......    | ... |


    // Pivot the table by KW - Value
    df = df.groupBy($"date", $"region")
      .pivot("kw")
      .agg(first("value"))
      .sort("date")
      .na.fill(0) // fill missing/NaN values with zero
    println("pivoted dataframe after union")
    df.show()
    // +-----------+------+------+---------+------+-----+
    // |      date |region| Virus| Vomiting| Mucus| ... |
    // +-----------+------+------+---------+------+-----+
    // |2020-01-01 |    US|  1.27|     3.33|  1.55| ... |
    // |2020-12-31 |    US|  0.64|     3.26|  1.44| ... |
    // |  ......   |   ...|  ... |     ... |  ... | ... |
    // |2020-01-01 |    CN|  7.29|    1.633|  1.02| ... |
    // |2019-12-31 |    CN|  5.99|    1.022|  1.66| ... |
    // |  ......   |   ...|  ... |     ... |  ... | ... |

    //Add One-Hot month feature
    val monthList = List("is_January", "is_February", "is_March", "is_April", "is_May", "is_June",
      "is_July", "is_August", "is_September", "is_October", "is_November", "is_December")
    val getMonthStrUdf = udf((monthNum: Int) => monthList(monthNum - 1)) //from month num to str
    val isMonthDf = df.withColumn("monthNumber", month(col("date")))
      .withColumn("monthNumber", getMonthStrUdf($"monthNumber"))
      .select($"date", $"region", $"monthNumber")
      .groupBy($"date", $"region")
      .pivot($"monthNumber")
      .count().na.fill(0)
    println("one hot month features dataframe")
    isMonthDf.show()
    // one hot month features dataframe
    // +----------+------+-----------+----------+
    // |      date|region|is_December|is_January|
    // +----------+------+-----------+----------+
    // |2020-01-01|    US|          0|         1|
    // |2019-12-31|    US|          1|         0|
    // +----------+------+-----------+----------+


    df = df.join(isMonthDf, Seq("date", "region"))
    println("dataframe after join withOne-Hot month features ")
    df.show()
    // +-----------+------+------+---------+------+-----+-----------+----------+-----+
    // |      date |region| Virus| Vomiting| Mucus| ... |is_December|is_January| ... |
    // +-----------+------+------+---------+------+-----+-----------+----------+-----+
    // |2020-01-01 |    US|  1.27|     3.33|  1.55| ... |          0|         1| ... |
    // |2020-12-31 |    US|  0.64|     3.26|  1.44| ... |          1|         0| ... |
    // |  ......   |   ...|  ... |     ... |  ... | ... |       ... |      ... | ... |
    // |2020-01-01 |    CN|  7.29|    1.633|  1.02| ... |          0|         1| ... |
    // |2019-12-31 |    CN|  5.99|    1.022|  1.66| ... |          1|         0| ... |
    // |  ......   |   ...|  ... |     ... |  ... | ... |       ... |      ... | ... |

    //convert features columns into one features vector
    val featuresColumns = df.drop("date", "region").columns //[Virus,Vomiting,Mucus,...]
    val assembler = new VectorAssembler()
      .setInputCols(featuresColumns) //all feature columns in df
      .setOutputCol("features")
    df = assembler
      .transform(df)
      .select("features", "date", "region") //optional
    df.show()
    // +-----------+------+------+--------------------------+
    // |      date |region| Virus|                  features|
    // +-----------+------+------+--------------------------+
    // |2020-01-01 |    US|  1.27| [1.27,3.33,...,0, 1,...] |
    // |2020-12-31 |    US|  0.64| [0.64,3.26,...,1, 0,...] |
    // |  ......   |   ...|  ... |                      ... |
    // |2020-01-01 |    CN|  7.29| [7.29,1.633,..,0, 1,...] |
    // |2019-12-31 |    CN|  5.99| [5.99,1.022,..,1, 0,...] |
    // |  ......   |   ...|  ... |                      ... |

    // The dataframe is ready for machine learning
    val regions = batchDf.select($"region").dropDuplicates().collect // regions = [US, CN, IL, ...]
    // foreach region execute Isolation Forest
    regions.par.foreach(r => {
      val region = r(0).toString // US
      println(s"starting $region")
      if (region != null) {
        val regionDf = df.filter($"region" === region)
        val historyDf = regionDf.where($"date" < lit(thisDate))
        val todayDF = regionDf.where($"date" === lit(thisDate))
        val numOfSamples = historyDf.count()
        if (numOfSamples >= 2) {
          println(s"starting IF $region")
          // region = US
          // +-----------+------+--------------------------+
          // |      date |region|                  features|
          // +-----------+------+--------------------------+
          // |2020-01-01 |    US| [1.27,3.33,...,0, 1,...] |
          // |2020-12-31 |    US| [0.64,3.26,...,1, 0,...] |
          // |  ......   |   ...|                      ... |
          val isolationForest = new IsolationForest()
            .setNumEstimators(100)
            .setBootstrap(false)
            .setMaxSamples(numOfSamples)
            .setMaxFeatures(1.0)
            .setFeaturesCol("features")
            .setPredictionCol("predictedLabel")
            .setScoreCol("outlierScore")
            .setContamination(0)
            .setRandomSeed(1)

          val isolationForestModel = isolationForest.fit(historyDf) // fit on historical data
          val dataWithScores = isolationForestModel.transform(todayDF) // transform the current data
          dataWithScores.show()
          // +-----------+------+--------------------------+--------------+------------+
          // |      date |region|                  features|predictedLabel|outlierScore|
          // +-----------+------+--------------------------+--------------+------------+
          // |2020-01-01 |    US| [1.27,3.33,...,0, 1,...] |             0|       0.011|
          // +-----------+------+--------------------------+--------------+------------+

          // save raw data in MySQL database
          dataWithScores
            .drop("features")
            .write.format("jdbc")
            .option("url", jdbcUrl)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", "ResultTable")
            .option("user", username)
            .option("password", password)
            .mode(SaveMode.Append)
            .save()

          println(s"ending $region")
        } else {
          println(s"not enough points in $region")
        }
      } else {
        println("null region")
      }
    })
  }
}

