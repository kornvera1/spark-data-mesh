package quality

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class DataQualityReport(
                              productName: String,
                              timestamp: Long,
                              metrics: Map[String, Double],
                              anomalies: Option[DataFrame] = None
                            )

class DataQualityMonitor(spark: SparkSession) {
  import spark.implicits._

  def checkQuality(df: DataFrame, productName: String): DataQualityReport = {
    val totalCount = df.count()

    val completenessMetrics = df.schema.fields.map { field =>
      val colName = field.name
      val nonNullCount = df.filter(col(colName).isNotNull).count()
      s"${colName}_completeness" -> nonNullCount.toDouble / totalCount
    }.toMap

    val uniquenessMetrics = df.schema.fields.map { field =>
      val colName = field.name
      val uniqueCount = df.select(colName).distinct().count()
      s"${colName}_uniqueness" -> uniqueCount.toDouble / totalCount
    }.toMap

    val typeValidationMetrics = df.schema.fields.flatMap { field =>
      field.dataType.typeName match {
        case "string" =>
          val emptyCount = df.filter(col(field.name) === "").count()
          Some(s"${field.name}_empty_strings" -> emptyCount.toDouble / totalCount)
        case "double" | "integer" | "long" =>
          val negativeCount = df.filter(col(field.name) < 0).count()
          Some(s"${field.name}_negative_values" -> negativeCount.toDouble / totalCount)
        case _ => None
      }
    }.toMap

    val allMetrics = completenessMetrics ++ uniquenessMetrics ++ typeValidationMetrics +
      ("total_records" -> totalCount.toDouble)

    val numericFields = df.schema.fields.filter { field =>
      field.dataType.typeName match {
        case "double" | "integer" | "long" => true
        case _ => false
      }
    }.map(_.name)

    val anomalies = if (numericFields.nonEmpty) {
      val statsDF = numericFields.foldLeft(df) { (tempDF, colName) =>
        tempDF.withColumn(s"${colName}_zscore",
          (col(colName) - mean(col(colName))) / stddev(col(colName)))
      }.filter(numericFields.map(colName =>
        abs(col(s"${colName}_zscore")) > 3).reduce(_ || _))

      Some(statsDF)
    } else None

    DataQualityReport(
      productName,
      System.currentTimeMillis(),
      allMetrics,
      anomalies
    )
  }

  def trackQualityOverTime(reports: Seq[DataQualityReport]): DataFrame = {
    val metricsDF = reports.map { report =>
      (report.productName, report.timestamp, report.metrics)
    }.toDF("product_name", "timestamp", "metrics")

    if (metricsDF.schema.fields.exists(_.name == "metrics")) {
      metricsDF
    } else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
        StructType(Seq(
          StructField("product_name", StringType),
          StructField("timestamp", LongType),
          StructField("metrics", MapType(StringType, DoubleType))
        )))
    }
  }
}