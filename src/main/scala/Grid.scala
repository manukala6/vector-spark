import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Polygon, PrecisionModel, Envelope}

import Spark.spark
import org.apache.spark.SparkContext

object Grid {

    val minLong = -180.0
    val maxLong = 180.0
    val minLat = -80.0
    val maxLat = 80.0

    val srid = 4326
    
    def generate1x1(): SpatialRDD[Polygon] = {

        val coords = for {
            x <- minLong until maxLong by 1.0
            y <- minLat until maxLat by 1.0
        } yield (x, y)
        
        val geometryFactory = new GeometryFactory(new PrecisionModel(), srid)
        val gridPolys = coords.flatMap { case (x, y) => 
            val envelope = new Envelope(x, x + 1, y, y + 1)
            val poly = geometryFactory.toGeometry(envelope).asInstanceOf[Polygon]
            Some(poly)    
        }
        
        val gridRDD = new SpatialRDD[Polygon]
        val sc: SparkContext = spark.sparkContext
        gridRDD.rawSpatialRDD = sc.parallelize(gridPolys)
        gridRDD.analyze()

        gridRDD
    }

    def generate3x3(): SpatialRDD[Polygon] = {

        val coords = for {
            x <- minLong until maxLong by 3.0
            y <- minLat until maxLat by 3.0
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
            x <- minLong until maxLong by 10.0
            y <- minLat until maxLat by 10.0
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

    def generate20x20(): SpatialRDD[Polygon] = {

        val coords = for {
            x <- minLong until maxLong by 20.0
            y <- minLat until maxLat by 20.0
        } yield (x, y)
        
        val geometryFactory = new GeometryFactory(new PrecisionModel(), srid)
        val gridPolys = coords.flatMap { case (x, y) => 
            val envelope = new Envelope(x, x + 20, y, y + 20)
            val poly = geometryFactory.toGeometry(envelope).asInstanceOf[Polygon]
            Some(poly)    
        }
        
        val gridRDD = new SpatialRDD[Polygon]
        val sc: SparkContext = spark.sparkContext
        gridRDD.rawSpatialRDD = sc.parallelize(gridPolys)
        gridRDD.analyze()

        gridRDD
    }

    def generate10band(): SpatialRDD[Polygon] = {

        val coords = for {
            x <- -180.0 until 180.0 by 180.0
            y <- -80.0 until 80.0 by 10.0
        } yield (x, y)

        val geometryFactory = new GeometryFactory(new PrecisionModel(), srid)
        val gridPolys = coords.flatMap { case (x, y) => 
            val envelope = new Envelope(x, x + 20, y, y + 20)
            val poly = geometryFactory.toGeometry(envelope).asInstanceOf[Polygon]
            Some(poly)    
        }
        
        val gridRDD = new SpatialRDD[Polygon]
        val sc: SparkContext = spark.sparkContext
        gridRDD.rawSpatialRDD = sc.parallelize(gridPolys)
        gridRDD.analyze()

        gridRDD
    }
}
