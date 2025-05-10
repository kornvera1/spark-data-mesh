import domains.{CustomerDataProduct, ProductDataProduct, SalesDataProduct}
import metadata.{DataCatalog, DataProductMetadata}
import org.apache.spark.sql.{DataFrame, SparkSession}
import quality.{DataQualityMonitor, DataQualityReport}
import security.{AccessManager, DomainBasedPolicy, User}

import java.time.LocalDateTime

class DataMeshPlatform(spark: SparkSession) {
  val catalog = new DataCatalog(spark)
  private val accessManager = new AccessManager(List(new DomainBasedPolicy))
  val qualityMonitor = new DataQualityMonitor(spark)

  private val salesDomain = new SalesDataProduct(spark)
  private val customerDomain = new CustomerDataProduct(spark)
  private val productDomain = new ProductDataProduct(spark)

  private val dataProducts: Map[String, () => DataFrame] = Map(
    "sales" -> salesDomain.getSalesData _,
    "customer" -> customerDomain.getCustomerData _,
    "product" -> productDomain.getProductData _,
    "daily_sales" -> salesDomain.getDailySales _,
    "customer_tiers" -> customerDomain.getCustomerTiers _,
    "products_by_category" -> productDomain.getProductsByCategory _
  )

  registerDataProducts()

  private def registerDataProducts(): Unit = {
    catalog.registerDataProduct(
      DataProductMetadata(
        name = "sales",
        domain = "sales",
        owner = "sales-team@company.com",
        description = "Raw sales data",
        schema = "date:date, product_id:string, amount:double, customer_id:string",
        createdAt = LocalDateTime.now(),
        updatedAt = LocalDateTime.now(),
        qualityMetrics = Map.empty,
        accessPolicies = List("sales", "analytics")
      )
    )

    catalog.registerDataProduct(
      DataProductMetadata(
        name = "customer",
        domain = "customer",
        owner = "customer-team@company.com",
        description = "Customer information",
        schema = "customer_id:string, name:string, email:string, tier:string",
        createdAt = LocalDateTime.now(),
        updatedAt = LocalDateTime.now(),
        qualityMetrics = Map.empty,
        accessPolicies = List("customer", "analytics")
      )
    )

    catalog.registerDataProduct(
      DataProductMetadata(
        name = "product",
        domain = "product",
        owner = "product-team@company.com",
        description = "Product catalog",
        schema = "product_id:string, name:string, category:string, price:double",
        createdAt = LocalDateTime.now(),
        updatedAt = LocalDateTime.now(),
        qualityMetrics = Map.empty,
        accessPolicies = List("product", "analytics")
      )
    )
  }

  def getDataProduct(name: String, user: User): Option[DataFrame] = {
    for {
      metadata <- catalog.getProductMetadata(name)
      if accessManager.checkAccess(user, metadata)
      dfFunc <- dataProducts.get(name)
      df = dfFunc()
    } yield {
      val qualityReport = qualityMonitor.checkQuality(df, name)
      logQualityReport(qualityReport)
      accessManager.filterData(user, df, metadata)
    }
  }

  private def logQualityReport(report: DataQualityReport): Unit = {
    println(s"Quality report for ${report.productName}:")
    report.metrics.foreach { case (k, v) => println(s"  $k: $v") }
  }

  def getSalesWithCustomerInfo(): DataFrame = {
    val sales = salesDomain.getSalesData
    val customers = customerDomain.getCustomerData

    sales.join(customers, "customer_id")
  }
}