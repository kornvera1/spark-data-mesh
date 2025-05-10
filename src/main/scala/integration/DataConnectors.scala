package integration

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class JDBCConnector(spark: SparkSession, url: String, user: String, password: String) {
  def readTable(table: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()
  }
}

class APIConnector(spark: SparkSession) {
  import spark.implicits._

  def fetchFromAPI(endpoint: String, params: Map[String, String]): DataFrame = {
    Seq(
      ("api1", params.getOrElse("param1", ""), 42),
      ("api2", params.getOrElse("param2", ""), 100)
    ).toDF("source", "param", "value")
  }
}

class S3Connector(spark: SparkSession, bucket: String) {
  def readFromS3(path: String): DataFrame = {
    spark.read
      .option("basePath", s"s3a://$bucket")
      .parquet(s"s3a://$bucket/$path")
  }
}