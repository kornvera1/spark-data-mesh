package streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaDataProduct(spark: SparkSession, bootstrapServers: String) {
  import spark.implicits._

  def readFromKafka(topic: String): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .load()
      .selectExpr("CAST(value AS STRING) as value", "timestamp")
      .withColumn("domain", lit(topic))
  }

  def startStreamingPipeline(topic: String, outputPath: String): Unit = {
    val stream = readFromKafka(topic)

    val query = stream.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", outputPath)
      .option("checkpointLocation", s"$outputPath/checkpoints")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    query.awaitTermination()
  }
}