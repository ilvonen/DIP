package assignment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag
import scala.util.Random

import java.io.StringReader
import com.opencsv.CSVReader

import java.util.Date
import java.text.SimpleDateFormat
import Math._


import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import java.util.Random
import java.io._
import java.nio.file._

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

   // val lines   = sc.textFile("src/main/resources/photos/dataForBasicSolution.csv")
    val lines   = sc.textFile("src/main/resources/photos/flickrDirtySimple.csv")
    //val lines   = sc.textFile("src/main/resources/photos/flickrDirtySimple_SYLK.csv")
    //val lines   = sc.textFile("src/main/resources/photos/DirtyTest.csv")
   // val lines   = sc.textFile("src/main/resources/photos/elbow.csv")
   
    
    
    val lines_without1 = lines.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
   
    
    val raw     = rawPhotos(lines_without1)

    def parseDouble(s: String) = try { s.toDouble } catch { case _ => 0 }

    val count = raw.count()
    //raw.collect.foreach(println)
    
    //val k = kmeansKernels
    val minLatitude = raw.filter(p => p.latitude > 0).takeOrdered(1)(Ordering[Double].on(p=>p.latitude))(0).latitude
    val maxLatitude = raw.takeOrdered(1)(Ordering[Double].reverse.on(x=>x.latitude))(0).latitude
    val minLongitude = raw.filter(p => p.longitude > 0).takeOrdered(1)(Ordering[Double].on(p=>p.longitude))(0).longitude
    val maxLongitude = raw.takeOrdered(1)(Ordering[Double].reverse.on(x=>x.longitude))(0).longitude
 
    
    val initialMeans = {
      val meansArray = Array.ofDim[(Double, Double)](kmeansKernels)
      for (i <- 0 to kmeansKernels-1) {
        val random1 = scala.util.Random.nextDouble()
        val random2 = scala.util.Random.nextDouble()
        val randomLatitude = minLatitude + random1*(maxLatitude - minLatitude)
        val randomLongitude = minLongitude + random2*(maxLongitude - minLongitude)
        val latLonPair = (randomLatitude, randomLongitude)
        meansArray(i) = latLonPair
      }
      println(meansArray.length)
      meansArray
    }

    val means   = kmeans(initialMeans, raw)
    println("Final means:")
    var i : Int = 0
    means.foreach(f => {i = i + 1;println(i + " "+f._1 + "," + f._2)})
    val fw = new PrintWriter(new File("data_stream.csv"))
    val textOutput = {
      var text = ""
      means.foreach(d => text = text.concat(d._1 + "," + d._2 + "\n"))
      text = text.substring(0, text.length()-2)
      text
    }
    new PrintWriter("data_stream.csv") {write(textOutput); close }
    //Files.write(Paths.get("data_stream.csv"), textOutput, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    //means.foreach(d => Files.write(Paths.get("data_stream.csv"), (d._1 + "," + d._2 + "\n").getBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND))
  }
}


class Flickr extends Serializable {
  
/** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D
  
  /** K-means parameter: Number of clusters */
  def kmeansKernels = 8
  
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
  def averageVectors(ps: Iterable[Photo]): (Double, Double) = {
    val latitudeSum = ps.map(_.latitude).sum
    val avgLatitude = latitudeSum/ps.size
    
    val longitudeSum = ps.map(_.longitude).sum
    val avgLongitude = longitudeSum/ps.size    
    
    (avgLatitude, avgLongitude)
  }
  
  def rawPhotos(lines: RDD[String]) : RDD[Photo] = {    
    def parseDouble(s: String) = try { s.toDouble } catch { case _ => 0 }
    def isRightFormat(l: String) = {
        val a = l.split(",")
        
        if (a.length == 4 && parseDouble(a(1)) != 0 && parseDouble(a(2)) != 0){
          true
        }
        else
          false
        
    }
    
    // Ignores empty lines:
    val lines_without_empties = lines.filter(!_.isEmpty())
    
    // Clean lines which are not in right format:
    val cleaned_lines = lines_without_empties.filter( f => isRightFormat(f) )
    
    println(cleaned_lines.count)

    cleaned_lines.map(l => {val a = l.split(","); (Photo(a(0), parseDouble(a(1)), parseDouble(a(2))))})
  }
  def classify(photos: RDD[Photo], means: Array[(Double, Double)]): RDD[(Int, Iterable[Photo])] = {
    photos.map(p => {val a = p; (findClosest((a.latitude, a.longitude), means), a)}).groupByKey()   
  }
  
  def refineMeans(classification: RDD[(Int, Iterable[Photo])], currentMeans: Array[(Double, Double)]) : Array[(Double, Double)] =  {
    classification.map{(f => averageVectors(f._2))}.collect()
  }
  
  
  def converged(kmeansEta: Double)(oldMeans: Array[(Double,Double)], newMeans: Array[(Double, Double)]): Boolean = {
    oldMeans.zip(newMeans).forall{case (om, nm) => distanceInMeters(om, nm) <= kmeansEta}
  }
  
  def textOutput(classification : RDD[(Int, Iterable[Photo])]) {
    val csvClassification =
      classification.flatMap { case (key, coords) => coords.map { case (photo) =>  val lat = photo.latitude;
                                                                                   val lon = photo.longitude;
                                                                                   s"$key,$lat,$lon" }}
    val random = scala.util.Random
    random.nextInt(100000)   
    csvClassification.saveAsTextFile("TestOutput" + random.nextInt(100000))
  }
    
  @tailrec final def kmeans(means: Array[(Double, Double)], vectors: RDD[Photo], iter: Int = 1): Array[(Double, Double)] = {
    //println(iter)
    val classification : RDD[(Int, Iterable[Photo])] = classify(vectors, means)
    val newMeans = refineMeans(classification, means)
    if (converged(kmeansEta)(means.sorted, newMeans.sorted)) { 
      //textOutput(classification)
      newMeans
    }
    else if (iter >= kmeansMaxIterations) {
      //textOutput(classification)
      newMeans 
    }
    else kmeans(newMeans, vectors, iter+1)
  }

}
