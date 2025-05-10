package domains

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class CustomerDataProduct(spark: SparkSession) {
  import spark.implicits._

  private val customerData = Seq(
    ("cust1", "John Doe", "john@example.com", "premium"),
    ("cust2", "Jane Smith", "jane@example.com", "standard")
  ).toDF("customer_id", "name", "email", "tier")

  def getCustomerData: DataFrame = {
    customerData
      .withColumn("domain", lit("customer"))
      .withColumn("timestamp", current_timestamp())
  }

  def getCustomerTiers: DataFrame = {
    customerData.groupBy("tier").agg(count("*").as("customers_count"))
  }
}