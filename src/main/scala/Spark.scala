import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.sedona.core.serde.SedonaKryoRegistrator

object SparkSetup {
  lazy val session: SparkSession = 
    SparkSession.builder()
      .appName("vector-spark")
      .master("local[*]")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .getOrCreate()
  
}