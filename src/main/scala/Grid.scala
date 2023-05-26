import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory, Polygon}

import Spark.spark
import org.apache.spark.SparkContext

object Grid {

    val minLong = -180.0
    val maxLong = 180.0
    val minLat = -90.0
    val maxLat = 90.0

    def generate1x1(): SpatialRDD[Polygon] = {

        val coords = for {
            x <- minLong to maxLong by 1.0
            y <- minLat to maxLat by 1.0
        } yield (x, y)
        
        val geometryFactory = new GeometryFactory()
        val gridPolys = coords.map { case (x, y) => 
            val envelope = new Envelope(x, x + 1, y, y + 1)
            val poly = geometryFactory.toGeometry(envelope).asInstanceOf[Polygon]
            poly    
        }

        val gridRDD = new SpatialRDD[Polygon]
        val sc: SparkContext = spark.sparkContext
        gridRDD.rawSpatialRDD = sc.parallelize(gridPolys)
        gridRDD.analyze()

        gridRDD
    }

    def generate3x3(): SpatialRDD[Polygon] = {

        val coords = for {
            x <- minLong to maxLong by 3.0
            y <- minLat to maxLat by 3.0
        } yield (x, y)
        
        val geometryFactory = new GeometryFactory()
        val gridPolys = coords.map { case (x, y) => 
            val envelope = new Envelope(x, x + 3, y, y + 3)
            val poly = geometryFactory.toGeometry(envelope).asInstanceOf[Polygon]
            poly    
        }

        val gridRDD = new SpatialRDD[Polygon]
        val sc: SparkContext = spark.sparkContext
        gridRDD.rawSpatialRDD = sc.parallelize(gridPolys)
        gridRDD.analyze()

        gridRDD
    }

    def generate10x10(): SpatialRDD[Polygon] = {

        val coords = for {
            x <- minLong to maxLong by 10.0
            y <- minLat to maxLat by 10.0
        } yield (x, y)
        
        val geometryFactory = new GeometryFactory()
        val gridPolys = coords.map { case (x, y) => 
            val envelope = new Envelope(x, x + 10, y, y + 10)
            val poly = geometryFactory.toGeometry(envelope).asInstanceOf[Polygon]
            poly    
        }

        val gridRDD = new SpatialRDD[Polygon]
        val sc: SparkContext = spark.sparkContext
        gridRDD.rawSpatialRDD = sc.parallelize(gridPolys)
        gridRDD.analyze()

        gridRDD
    }
}
