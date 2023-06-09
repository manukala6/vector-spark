import Spark.spark
import org.locationtech.jts.geom.{Geometry, Polygon}
import org.apache.spark.SparkContext
import scala.jdk.CollectionConverters._
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.core.enums.GridType
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.enums.IndexType
import cats.instances.int
import org.apache.sedona.core._
object Main {

    def main(args: Array[String]): Unit = {

        // Input file URI
        //println("Input file URI: ")
        //val input_file: String = scala.io.StdIn.readLine()
        val input_file: String = "data\\gadm41_BRA_2"

        // Read in geospatial data

        // Set up spark session
        val sc: SparkContext = spark.sparkContext
        val allowTopologyInvalidGeometries = true
        val skipSyntaxInvalidGeometries = false
        val spatialRDD: SpatialRDD[Geometry] = ShapefileReader.readToGeometryRDD(sc, input_file)

        println("Number of records: " + spatialRDD.rawSpatialRDD.count())

        // convert rdd to dataframe
        var spatialDf = Adapter.toDf(spatialRDD, spark)

        // filter to Amazonas
        spatialDf = spatialDf.filter("NAME_1 = 'Amazonas'")
        // print number of rows
        println("Number of records in Amazonas: " + spatialDf.count())

        // Make 1x1 grid
        val gridRDD: SpatialRDD[Polygon] = Grid.generate1x1()
        val gridHeadRecs: List[Polygon] = gridRDD.rawSpatialRDD.take(2).asScala.toList
        gridHeadRecs.foreach(println)

        // Join grid to Amazonas data
        gridRDD.analyze()
        gridRDD.spatialPartitioning(GridType.KDBTREE)
        gridRDD.buildIndex(IndexType.RTREE, true)

        val adminRDD = Adapter.toSpatialRdd(spatialDf, "geometry")
        adminRDD.analyze()
        adminRDD.spatialPartitioning(gridRDD.getPartitioner)

        val useIndex = true
        val considerBoundaryIntersection = true
        val resultRDD = JoinQuery.SpatialJoinQueryFlat(adminRDD, gridRDD, useIndex, considerBoundaryIntersection)

        val intersectedJavaRDD = resultRDD.map(pair => pair._1.intersection(pair._2))
        val intersectedRDD = new SpatialRDD[Geometry]
        intersectedRDD.setRawSpatialRDD(intersectedJavaRDD)
        intersectedRDD.analyze()

        // save to file
        val intersectedDF = Adapter.toDf(intersectedRDD, spark)
        intersectedDF.write.format("geoparquet").save("output\\amazonas_1x1_grid")



        sc.stop()
    }
}
