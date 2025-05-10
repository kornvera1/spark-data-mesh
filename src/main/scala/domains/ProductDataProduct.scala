package domains

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProductDataProduct(spark: SparkSession) {
  import spark.implicits._

  private val productData = Seq(
    ("prod1", "Laptop", "Electronics", 999.99),
    ("prod2", "Desk Chair", "Furniture", 199.99)
  ).toDF("product_id", "name", "category", "price")

  def getProductData: DataFrame = {
    productData
      .withColumn("domain", lit("product"))
      .withColumn("timestamp", current_timestamp())
  }

  def getProductsByCategory: DataFrame = {
    productData.groupBy("category").agg(count("*").as("products_count"))
  }
}