package metadata

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime

case class DataProductMetadata(
                                name: String,
                                domain: String,
                                owner: String,
                                description: String,
                                schema: String,
                                createdAt: LocalDateTime,
                                updatedAt: LocalDateTime,
                                qualityMetrics: Map[String, String],
                                accessPolicies: List[String]
                              )

class DataCatalog(spark: SparkSession) {
  import spark.implicits._

  private var catalog = Seq.empty[DataProductMetadata].toDF()

  def registerDataProduct(metadata: DataProductMetadata): Unit = {
    val newEntry = Seq(metadata).toDF()
    catalog = if (catalog.isEmpty) newEntry else catalog.union(newEntry)
  }

  def searchDataProducts(query: String): DataFrame = {
    catalog.filter(
      col("name").contains(query) ||
        col("description").contains(query) ||
        col("domain").contains(query)
    )
  }

  def getProductMetadata(name: String): Option[DataProductMetadata] = {
    catalog.filter(col("name") === name)
      .as[DataProductMetadata]
      .collect()
      .headOption
  }

  def listDomainProducts(domain: String): DataFrame = {
    catalog.filter(col("domain") === domain)
  }
}