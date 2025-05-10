
# Data Mesh Prototype with Spark
## Project Overview

This project implements a Data Mesh architecture prototype where:
- Data is treated as a product
- Domain teams own their data
- Platform provides self-serve infrastructure
- Federated governance is applied

## Technology Stack

- **Language**: Scala 2.12/2.13
- **Processing**: Apache Spark 3.3+
- **Streaming**: Kafka
- **Metadata**: Custom catalog
- **Quality**: Custom monitoring
- **Access**: RBAC model

## Quick Start

### Prerequisites

1. JDK 8/11
2. Scala 2.12+
3. Spark 3.3+
4. Kafka (for streaming)

### Installation

```bash
git clone https://github.com/your-repo/data-mesh-prototype.git
cd data-mesh-prototype
sbt compile
sbt "runMain MainApp"
```

## Project Structure

```
src/
├── main/
│   ├── scala/
│   │   ├── domains/          # Domain data products
│   │   ├── metadata/         # Data catalog
│   │   ├── quality/          # Data quality
│   │   ├── security/         # Access control
│   │   └── streaming/       # Streaming pipelines
└── test/                    # Unit tests
```

## Key Features

1. **Domain-oriented data products**
```scala
class SalesDataProduct(spark: SparkSession) {
  def getSalesData(): DataFrame = {
    // Domain-specific implementation
  }
}
```

2. **Data discovery**
```scala
catalog.searchDataProducts("sales")
```

3. **Quality monitoring**
```scala
qualityMonitor.checkQuality(df, "sales-product")
```

4. **Access control**
```scala
accessManager.checkAccess(user, metadata)
```

## Example Usage

```scala
val spark = SparkSession.builder()
  .appName("DataMeshExample")
  .getOrCreate()

val dataMesh = new DataMeshPlatform(spark)
val user = User("analyst1", List("analyst"), List("data-team"))

// Get data product
val salesData = dataMesh.getDataProduct("sales", user)

// Stream processing
val kafkaStream = new EnhancedKafkaDataProduct(
  spark,
  "localhost:9092",
  dataMesh.qualityMonitor
)
```