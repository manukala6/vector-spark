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


object Main {

    def main(args: Array[String]): Unit = {

        // Input file URI
        val input_file: String = "data//nems_slim.tsv"

        // Set up spark session
        val sc: SparkContext = spark.sparkContext
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
        val spatialDf = rawDf.withColumn("geom", expr("ST_Point(CAST(`decimalLongitude` AS Decimal(24,20)), CAST(`decimalLatitude` AS Decimal(24,20)))"))
        println("First five rows of spatialDf: " + spatialDf.show(5))
        println("Number of points: " + rawDf.count())

        // Convert to spatial RDD
        val spatialRDD = Adapter.toSpatialRdd(spatialDf, "geom")

        // Generate 20x20 grid
        val gridRDD = Grid.generate20x20()
        println("Number of grid cells: " + gridRDD.countWithoutDuplicates())

        val gridHeadRecs: List[Polygon] = gridRDD.rawSpatialRDD.take(10).asScala.toList
        gridHeadRecs.foreach(println)

        // Spatial partitioning for both RDDs
        gridRDD.analyze()
        gridRDD.spatialPartitioning(GridType.KDBTREE)
        gridRDD.buildIndex(IndexType.RTREE, true)

        spatialRDD.analyze()        
        spatialRDD.spatialPartitioning(gridRDD.getPartitioner)
        
        // Perform spatial join
        val result = JoinQuery.SpatialJoinQuery(spatialRDD, gridRDD, true, true)

        // Group by polygon and count points
        val counts = result.rdd
            .map(tuple => (tuple._2, 1))
            .reduceByKey(_ + _)
        println("Number of grid cells with points: " + counts.count())
        println("First five rows of counts: " + counts.take(5).toList)

        // Convert to dataframe
        val countDf = counts.map {
            case (polygon, count) => (polygon, count)
        }.toDF("geometry", "count")

        // Write to shapefile
        countDf.write
            .format("shapefile")
            .option("header", "true")
            .save("data//nems_slim_grid_20x20.shp")

        sc.stop()
    }
}
