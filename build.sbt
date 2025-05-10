name := "spark-data-mesh"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-streaming" % "3.5.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.0",
  "org.apache.kafka" % "kafka-clients" % "3.5.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
  "com.github.javafaker" % "javafaker" % "1.0.2",
  "com.typesafe" % "config" % "1.4.2",
  "io.delta" %% "delta-core" % "2.4.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.spark" %% "spark-mllib" % "3.5.0",
  "com.github.mrpowers" %% "spark-daria" % "1.0.0",
  "org.scalanlp" %% "breeze" % "1.3",
  "org.scalanlp" %% "breeze-natives" % "1.3"
)