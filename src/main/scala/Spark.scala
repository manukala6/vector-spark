import org.apache.spark.sql.SparkSession

object Spark {
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("vector-spark")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    .getOrCreate()
}
