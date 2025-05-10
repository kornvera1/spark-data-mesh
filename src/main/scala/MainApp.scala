import org.apache.spark.sql.SparkSession
import security.User
import streaming.EnhancedKafkaDataProduct

object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EnhancedDataMesh")
      .master("local[*]")
      .getOrCreate()

    val dataMesh = new DataMeshPlatform(spark)

    val analyst = User("user1", List("analytics"), List("analytics-team"))
    val salesUser = User("user2", List("sales"), List("sales-team"))

    val searchResults = dataMesh.catalog.searchDataProducts("sales")
    searchResults.show()

    dataMesh.getDataProduct("sales", analyst).foreach(_.show())
    dataMesh.getDataProduct("customer", salesUser).foreach(_.show())

    val kafkaProduct = new EnhancedKafkaDataProduct(
      spark,
      "localhost:9092",
      dataMesh.qualityMonitor // Теперь доступ через dataMesh.qualityMonitor
    )

    val joinedStream = kafkaProduct.joinStreams("sales", "customers", "customer_id")

    val salesAggregations = kafkaProduct.aggregateWithState(
      "sales",
      "product_id",
      Map(
        "total_sales" -> "sum(amount)",
        "avg_sales" -> "avg(amount)",
        "count" -> "count(*)"
      )
    )

    spark.streams.awaitAnyTermination()
    spark.stop()
  }
}
