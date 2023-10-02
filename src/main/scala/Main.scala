import Spark.spark
import spark.implicits._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.sedona.sql.utils.Adapter
import org.locationtech.jts.geom.{Geometry, Polygon}
import scala.jdk.CollectionConverters._
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialPartitioning._
import org.apache.sedona.core.enums.GridType
import org.apache.sedona.core.enums.IndexType
import cats.instances.tuple
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.locationtech.jts.io.WKTWriter
import org.apache.spark.rdd.RDD
import org.apache.sedona.core.formatMapper.WktReader


object Main {

    def main(args: Array[String]): Unit = {

        // Input file URI
        val input_file: String = "data//OBIS_anemone_occurrences_slim.tsv"

        // Set up spark session
        val sc: SparkContext = spark.sparkContext
        val wktColumn = 2
        val allowTopologyInvalidGeometries = true
        val skipSyntaxInvalidGeometries = false
        sc.setLogLevel("ERROR")
        SedonaSQLRegistrator.registerAll(spark)

        // Read points from TSV
        val rawDf = spark.read
            .format("csv")
            .option("header", "true")
            .option("delimiter", "\t")
            .load(input_file)
        val spatialDf = rawDf.withColumn("geometry", expr("ST_GeomFromWKT(geometry)"))
        println("First five rows of spatialDf: " + spatialDf.show(5))
        println("Number of points: " + spatialDf.count())

        // Convert to spatial RDD
        val spatialRDD = Adapter.toSpatialRdd(spatialDf, "geometry")

        // Generate 1x1 grid
        val gridRDD = Grid.generate1x1()
        println("Number of grid cells: " + gridRDD.countWithoutDuplicates())

        val gridHeadRecs: List[Polygon] = gridRDD.rawSpatialRDD.take(10).asScala.toList
        gridHeadRecs.foreach(println)

        // Spatial partitioning for both RDDs
        gridRDD.analyze()
        gridRDD.spatialPartitioning(GridType.KDBTREE)
        gridRDD.buildIndex(IndexType.RTREE, true)

        spatialRDD.analyze()        
        spatialRDD.spatialPartitioning(gridRDD.getPartitioner)

        // print first five elements of spatialRDD
        val spatialHeadRecs: List[Geometry] = spatialRDD.rawSpatialRDD.take(10).asScala.toList
        spatialHeadRecs.foreach(println)
        
        // Perform spatial join
        val resultRDD = JoinQuery.SpatialJoinQuery(spatialRDD, gridRDD, true, true)

        // Group by polygon and count points
        val countRDD = resultRDD.rdd
            .map { case (polygon, pointList) => (polygon, pointList.size()) }
        println("Number of grid cells with points: " + countRDD.count())
        println("First five rows of counts: " + countRDD.take(5).toList)

        //val countDF = Adapter.toDf(spatialRDD, spark)
        val countDF = countRDD.map {
            case (polygon: Polygon, count: Int) => 
                val wkt = new WKTWriter().write(polygon)
                s"$wkt\t$count"  // TSV format (tab-separated)
        }

        println("First five rows of countsDF: " + countRDD.take(5).toList)

        // Write to CSV
        countDF.coalesce(1).saveAsTextFile("output//joined_grid_11.tsv")

        // Write to shapefile
        //countDF.write.format("geoparquet").save("output//joined_grid_5.parquet")

        sc.stop()
    }
}