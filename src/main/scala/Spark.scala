import org.apache.spark.sql.SparkSession

object SparkSetup {
  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("vector-spark")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
      .getOrCreate()
  }
}