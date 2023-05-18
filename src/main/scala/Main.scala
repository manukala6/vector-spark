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
        val sc: SparkContext = session.sparkContext

        // Input file URI
        println("Input file URI: ")
        val input_file: String = scala.io.StdIn.readLine()


   
    }
}
