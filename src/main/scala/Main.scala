import org.apache.spark.rdd.RDD
import geotrellis.vector.io._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import SparkSetup._
import java.util.UUID
import org.locationtech.jts.geom.Geometry

object Main {

    def main(args: Array[String]): Unit = {

        // Set up spark session
        val spark: SparkSession = createSparkSession()
        val sc: SparkContext = spark.sparkContext

        // Input file URI
        println("Input file URI: ")
        val input_file: String = scala.io.StdIn.readLine()

        // Read the features
        val features = spark.read
            .format("geotrellis")
            .option("path", "input_file")
            .load()
            .select("geom")

        val rdd = features.rdd.map(row => row.getAs(Geometry(0)))

        spark.stop()

    }
}
