package streaming

import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import quality.DataQualityMonitor

class EnhancedKafkaDataProduct(
                                spark: SparkSession,
                                bootstrapServers: String,
                                qualityMonitor: DataQualityMonitor
                              ) {
  import spark.implicits._

  private def readFromKafka(topic: String): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as value", "timestamp")
      .withColumn("topic", lit(topic))
  }


  def startStreamingWithQualityCheck(
                                      topic: String,
                                      outputPath: String,
                                      productName: String
                                    ): StreamingQuery = {
    val stream = readFromKafka(topic)

    // Заменяем mapBatch на foreachBatch для мониторинга качества
    val processedStream = stream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // Проверка качества для каждого микробатча
        val report = qualityMonitor.checkQuality(batchDF, productName)
        println(s"Quality report for batch $batchId: ${report.metrics}")

        // Сохранение данных
        batchDF.write
          .mode("append")
          .parquet(outputPath)
      }
      .option("checkpointLocation", s"$outputPath/checkpoints")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    processedStream
  }

  def joinStreams(
                   topic1: String,
                   topic2: String,
                   joinKey: String
                 ): DataFrame = {
    val stream1 = readFromKafka(topic1).withWatermark("timestamp", "1 minute")
    val stream2 = readFromKafka(topic2).withWatermark("timestamp", "1 minute")
      .withColumnRenamed(joinKey, s"${joinKey}_right")
      .withColumnRenamed("timestamp", "timestamp_right")

    stream1.join(
      stream2,
      expr(s"$joinKey = ${joinKey}_right AND timestamp >= timestamp_right - interval 5 minutes AND timestamp <= timestamp_right + interval 5 minutes"),
      "leftOuter"
    )
  }

  def aggregateWithState(
                          topic: String,
                          keyColumn: String,
                          aggExprs: Map[String, String]
                        ): StreamingQuery = {

    val aggregations = aggExprs.map { case (alias, expression) =>
      expr(expression).as(alias)
    }.toList

    readFromKafka(topic)
      .groupBy(col(keyColumn))
      .agg(aggregations.head, aggregations.tail: _*) // Правильный вызов agg
      .writeStream
      .outputMode("complete")
      .format("memory")
      .queryName(s"${topic}_aggregations")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()
  }
}