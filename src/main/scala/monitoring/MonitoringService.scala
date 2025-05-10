package monitoring

import org.apache.spark.sql.SparkSession
import quality.DataQualityReport

class MonitoringService(spark: SparkSession) {
  import spark.implicits._

  private val alertThresholds = Map(
    "completeness" -> 0.95,
    "uniqueness" -> 0.9,
    "total_records" -> 1000
  )

  def checkForAlerts(report: DataQualityReport): Option[String] = {
    val alerts = report.metrics.flatMap { case (metric, value) =>
      alertThresholds.collect {
        case (key, threshold) if metric.contains(key) && value < threshold =>
          s"Alert: $metric = $value is below threshold $threshold"
      }
    }

    if (alerts.nonEmpty) Some(alerts.mkString("\n")) else None
  }

  def logToMonitoringSystem(report: DataQualityReport): Unit = {
    println(s"Logging metrics to monitoring system: ${report.metrics}")
  }
}