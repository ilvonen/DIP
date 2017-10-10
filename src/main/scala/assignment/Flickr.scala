package assignment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

import java.io.StringReader
import com.opencsv.CSVReader

import java.util.Date
import java.text.SimpleDateFormat
import Math._


import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

case class Photo(id: String,
                 latitude: Double,
                 longitude: Double)
                 //datetime: Date)

                 
object Flickr extends Flickr {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("NYPD")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  
  /* control log level */
  // sc.setLogLevel("ERROR")
  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/photos/dataForBasicSolution.csv")
    val raw     = rawPhotos(lines)
    
    val tupleRdd = lines.map(l => {val a = l.split(","); (a(0), a(1), a(2), a(3))})
    //val parsedData = lines.map(l => {val a = l.split(","); (a(1).toDouble, a(2).toDouble)})
    //val parsedData = Vectors.sparse(lines.count(), tupleRdd.collect())
    
    //def vectorize(x:RDD[(Double,Double)], size: Int):Vector = {
      //val vec = Vectors.sparse(size, x.collect())
    //}
    //val parsedData = vectorize(lines.count(), tupleRdd) 
    
    //val numClusters = 2
    //val numIterations = 20
    //val clusters = KMeans.train(parsedData, numClusters, numIterations)
    
    
    //val parsedData = lines.map(s => Vectors.dense(s.split(",").map(_.toDouble))).cache()
    
    //val lines10 = lines.take(10)
    def parseDouble(s: String) = try { s.toDouble } catch { case _ => 0 }
    
    for ( a <- tupleRdd ){
      val b = a._2
      val c = parseDouble(b)
      
      val d = 25.650157
      
      if (c > d) {
        println(c)
      }
    }
    val count = lines.count()
    
    println(count)
    
    //val initialMeans = ???
    
    //val means   = kmeans(initialMeans, raw)
    println("TEST1")
  }
  
}


class Flickr extends Serializable {
  
/** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D
  
  /** K-means parameter: Number of clusters */
  def kmeansKernels = 16
  
  
  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 50

  //(lat, lon)
  def distanceInMeters(c1: (Double, Double), c2: (Double, Double)) = {
		val R = 6371e3
		val lat1 = toRadians(c1._1)
		val lon1 = toRadians(c1._2)
		val lat2 = toRadians(c2._1)
		val lon2 = toRadians(c2._2)
		val x = (lon2-lon1) * Math.cos((lat1+lat2)/2);
		val y = (lat2-lat1);
		Math.sqrt(x*x + y*y) * R;
  }
  
    
  /** Return the index of the closest mean */
  def findClosest(p: (Double, Double), centers: Array[(Double, Double)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = distanceInMeters(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  /** Average the vectors */
  def averageVectors(ps: Iterable[Photo]): (Double, Double) = ???
  
  def rawPhotos(lines: RDD[String]) /*: RDD[Photo]*/ = {    
    println("rawPhotos")
    
  }

    
  // @tailrec final def kmeans(means: Array[(Double, Double)], vectors: RDD[Photo], iter: Int = 1): Array[(Double, Double)] = ???

}
