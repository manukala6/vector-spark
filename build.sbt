name := "vector-spark"
version := "0.1"
scalaVersion := "2.12.17"

val SparkVersion = "3.3.2"
val SparkCompatibleVersion = "3.0"
val HadoopVersion = "3.2.2"
val SedonaVersion = "1.4.0"
val ScalaCompatibleVersion = "2.12"
val GeoTrellisVersion = "3.6.0"

// Change the dependency scope to "provided" when you run "sbt assembly"
val dependencyScope = "compile"
val geotoolsVersion = "1.4.0-28.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-hdfs" % HadoopVersion % dependencyScope,
  "org.apache.sedona" % "sedona-spark-shaded-".concat(SparkCompatibleVersion).concat("_").concat(ScalaCompatibleVersion) % SedonaVersion changing(),
  "org.apache.sedona" % "sedona-core-".concat(SparkCompatibleVersion).concat("_").concat(ScalaCompatibleVersion) % SedonaVersion changing(),
  "org.apache.sedona" % "sedona-sql-".concat(SparkCompatibleVersion).concat("_").concat(ScalaCompatibleVersion) % SedonaVersion changing(),
  "org.apache.sedona" % "sedona-viz-".concat(SparkCompatibleVersion).concat("_").concat(ScalaCompatibleVersion) % SedonaVersion changing(),
  "org.locationtech.geotrellis" %% "geotrellis-spark" % GeoTrellisVersion % dependencyScope,
  "org.locationtech.geotrellis" %% "geotrellis-vector" % GeoTrellisVersion % dependencyScope,
  "org.locationtech.geotrellis" %% "geotrellis-s3" % GeoTrellisVersion  % dependencyScope,
  "org.datasyslab" % "geotools-wrapper" % geotoolsVersion % "compile",
  "software.amazon.awssdk" % "s3" % "2.17.89",
)
