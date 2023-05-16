name := "vector-spark"
version := "0.1"
scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "3.6.0",
  "org.locationtech.geomesa" %% "geomesa-spark-jts" % "3.3.0",
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.apache.spark" %% "spark-mllib" % "3.3.0",
  "org.locationtech.geotrellis" %% "geotrellis-vector" % "3.6.0",
  "org.locationtech.geotrellis" %% "geotrellis-s3" % "3.6.0",
  "org.locationtech.geotrellis" %% "geotrellis-raster" % "3.6.0",
  "software.amazon.awssdk" % "s3" % "2.17.89",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1"
)
