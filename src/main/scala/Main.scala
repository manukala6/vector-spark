import Spark.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}

object Main {

    def main(args: Array[String]): Unit = {

        // Input file URI
        val input_file: String = "data//nems_slim.csv"

        // Set up spark session
        val sc: SparkContext = spark.sparkContext
        val allowTopologyInvalidGeometries = true
        val skipSyntaxInvalidGeometries = false
        
        // Read points from TSV
        var pointDf = spark.read.format("csv").option("delimiter","/t").option("header", "true").load(input_file)
        println("Number of points: " + pointDf.count())

        // Generate 20x20 grid
        val gridRDD = Grid.generate20x20()
        println("Number of grid cells: " + gridRDD.countWithoutDuplicates())


        sc.stop()
    }
}
