package domains

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class SalesDataProduct(spark: SparkSession) {
  import spark.implicits._

  private val internalSalesData = Seq(
    ("2023-01-01", "prod1", 100, "cust1"),
    ("2023-01-02", "prod2", 200, "cust2")
  ).toDF("date", "product_id", "amount", "customer_id")

  def getSalesData: DataFrame = {
    internalSalesData
      .withColumn("domain", lit("sales"))
      .withColumn("timestamp", current_timestamp())
  }

  def getDailySales: DataFrame = {
    internalSalesData.groupBy("date").agg(sum("amount").as("daily_sales"))
  }
}