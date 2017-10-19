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
import java.time
import Math._


import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import java.util.Random
import java.io._
import java.nio.file._

case class Photo(id: String,
                 latitude: Double,
                 longitude: Double,
                 datetime: Date)

                 
object Flickr extends Flickr {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("NYPD")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  
  
  /** Control log level */
  sc.setLogLevel("ERROR")
  
  /** Main function */
  def main(args: Array[String]): Unit = {
    //val lines   = sc.textFile("src/main/resources/photos/dataForBasicSolution.csv")
    //val lines   = sc.textFile("src/main/resources/photos/flickrDirtySimple.csv")
    //val lines   = sc.textFile("src/main/resources/photos/flickrDirtySimple_SYLK.csv")
    //val lines   = sc.textFile("src/main/resources/photos/DirtyTest.csv")
    //val lines   = sc.textFile("src/main/resources/photos/elbow.csv")
    //val lines   = sc.textFile("src/main/resources/photos/smallDataset.csv")
    //val lines   = sc.textFile("src/main/resources/photos/flickr3D2.csv")
    val lines = sc.textFile("src/main/resources/photos/flickr3D.csv")
    
    
    
    val lines_without1 = lines.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)   
    val raw = rawPhotos(lines_without1)
    
    val latMinMax = (raw.takeOrdered(1)(Ordering[Double].reverse.on(p=>p.latitude))(0).latitude, 
                       raw.takeOrdered(1)(Ordering[Double].on(p=>p.latitude))(0).latitude)
    val lonMinMax = (raw.takeOrdered(1)(Ordering[Double].reverse.on(p=>p.longitude))(0).longitude, 
                       raw.takeOrdered(1)(Ordering[Double].on(p=>p.longitude))(0).longitude)
    
    val raw3d = getScaledRDD(raw)
    

    
    def parseDouble(s: String) = try { s.toDouble } catch { case _ => 0 }
    
    
    var textOutput = ""
    
    if (!enable3d) {
      var initialMeans : Array[(Double, Double)] = raw.takeSample(false, kmeansKernels).map{p => (p.latitude, p.longitude)}
      while(initialMeans.distinct.size != initialMeans.size) {
        initialMeans = raw.takeSample(false, kmeansKernels).map{p => (p.latitude, p.longitude)}
      }
      println("Initial means:")
      var j : Int = 0
      initialMeans.foreach(f => {j = j + 1;println(j + " "+f._1 + "," + f._2)})
      val means   = kmeans(initialMeans, raw)
      println("Final means:")
      var i : Int = 0
      means.foreach(f => {i = i + 1;println(i + " "+f._1 + "," + f._2)})
      textOutput = {
        var text = ""
        means.foreach(d => text = text.concat(d._1 + "," + d._2 + "\n"))
        text.substring(0, text.length()-2)
      }
      means
    }
    else {
      var initialCoords = raw3d.takeSample(false, kmeansKernels).map(p => (p._1, p._2))
      var initialTimes = raw3d.takeSample(false, kmeansKernels).map(p => (p._3))
      while(initialCoords.distinct.size != initialCoords.size) {
        
        /** Test print */
        var j : Int = 0
        initialCoords.foreach(f => {j = j + 1;println(j + " "+f._1 + "," + f._2)})
        /**************/
        
        initialCoords = raw3d.takeSample(false, kmeansKernels).map(p => (p._1, p._2))       
      }
      var initialMeans3d = initialCoords.zip(initialTimes).map(f => (f._1._1, f._1._2, f._2))

      println("Initial means:")
      var j : Int = 0
      
      initialMeans3d.foreach(f => {j = j + 1;println(j + " "+f._1 + "," + f._2 + "," + f._3)})
      val means = kmeans3d(initialMeans3d, raw3d)
      println("Final means:")
      textOutput = {
        var text : String = ""
        means.foreach(d => text = text.concat(scaleCoordinatesFrom100(d._1, latMinMax._2, latMinMax._1) + ","
            + scaleCoordinatesFrom100(d._2, lonMinMax._2, lonMinMax._1) + ","
            + scaleDateFrom100(d._3) + "\n"))
        println(text)
        text.substring(0, text.length()-1)
        
      }
      
    }
    
    val fw = new PrintWriter(new File("data_stream.csv"))
    new PrintWriter("data_stream.csv") {write(textOutput); close }
  }
}


class Flickr extends Serializable {
  
  def sumOfSquares(clusters: RDD[(Int, Iterable[Photo])]) {
    var sumLat : Double = 0.0
    var sumLon : Double = 0.0
    var partitionSize : Int = 0
    var meanLatLon : (Double, Double) = (0.0, 0.0)
    
    //clusters.foreach(f => println(":D"))

    //clusters.foreachPartition(partition => partition.foreach(f => {println("size:"+f._2.size); f._2.foreach(p => 
      //{sumLat = sumLat + p.latitude; sumLon = sumLon + p.longitude}); println(f._1); meanLatLon = (sumLat/f._2.size, sumLon/f._2.size); println(meanLatLon);  sumLat = 0.0; sumLon = 0.0}))   
  }
  /** Choose to enable 3d or not */
  def enable3d : Boolean = true;
  
/** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D
  
  /** K-means parameter: Number of clusters */
  def kmeansKernels = 8
  
  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 20
  
  /** Date format to be used */
  def dateFormat : SimpleDateFormat = new java.text.SimpleDateFormat("yyyy:MM:dd HH:mm:ss")
  
  /** Maximum and minimum dates for the purpose of scaling dates for seasons */
  def maxDate : Date = dateFormat.parse("2016:12:31 23:59:59")
  def minDate : Date = dateFormat.parse("2016:01:01 00:00:00")
  
  def scaleTo100(value : Double, maxVal : Double, minVal : Double) : Double = {
    (value - minVal)/(maxVal - minVal)*100
  }
  def scaleCoordinatesFrom100(value : Double, maxVal : Double, minVal : Double) : Double = {
    value*(maxVal-minVal)/100 + minVal
  }
  def scaleDateFrom100(value : Double) : String = {
    val maxDateLong = maxDate.getTime
    val minDateLong = minDate.getTime
    val yearLength = maxDateLong-minDateLong
    
    val timeOfYear : Long = (value/100*yearLength).toLong
    
    val locale = new java.util.Locale("English", "UK")
    val timezone = java.util.TimeZone.getTimeZone("UTC")
    val wantedDateFormat = new java.text.SimpleDateFormat("d MMMM HH:mm:ss", locale)
    wantedDateFormat.setTimeZone(timezone)
       
    wantedDateFormat.format(timeOfYear)
  }
  
  def sinceStartOfYear(date : Date) : Long = {
    val year = date.getYear()+1900
    val dateAtStartOfYear = dateFormat.parse(year.toString()+":01:01 00:00:00")
    date.getTime - dateAtStartOfYear.getTime
  } 
  
  def getScaledRDD(raw : RDD[Photo]) : RDD[(Double, Double, Double)] = {
      val latMinMax = (raw.takeOrdered(1)(Ordering[Double].reverse.on(p=>p.latitude))(0).latitude, 
                       raw.takeOrdered(1)(Ordering[Double].on(p=>p.latitude))(0).latitude)
      val lonMinMax = (raw.takeOrdered(1)(Ordering[Double].reverse.on(p=>p.longitude))(0).longitude, 
                       raw.takeOrdered(1)(Ordering[Double].on(p=>p.longitude))(0).longitude)                                  
      
      val scaledRDD = raw.map(p => {(
        (scaleTo100(p.latitude, latMinMax._2, latMinMax._1)),
        (scaleTo100(p.longitude, lonMinMax._2, lonMinMax._1)),
        (scaleTo100(sinceStartOfYear(p.datetime).toDouble,
            sinceStartOfYear(maxDate).toDouble,
            sinceStartOfYear(minDate).toDouble))
        )})
      //scaledRDD.foreach(f => println(f._1 + "," + f._2 + "," + f._3))
      scaledRDD    
  }
  

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
  
  def distanceIn3d(c1: (Double, Double, Double), c2: (Double, Double, Double)) = {
    val distLat = c2._1 - c1._1
    val distLon = c2._2 - c2._2
    var distTime = c2._3 - c2._3    
    
    // distance from a datepoint to another always has to be less or equal to 50 on 0-100 scale.
    if(Math.abs(distTime) > 50) distTime = 100-Math.abs(distTime)
    
    Math.sqrt(distLat*distLat + distLon*distLon + distTime*distTime)
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
  
  /** Return the index of the closest mean 3d */
  def findClosest3d(p: (Double, Double, Double), centers: Array[(Double, Double, Double)]) : Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = distanceIn3d(p, centers(i))
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
  
  /** Average the vectors 3d*/
  def averageVectors3d(ps: Iterable[(Double, Double, Double)]): (Double, Double, Double) = {
    var sumLat = 0.0
    var sumLon = 0.0
    var sumTime = 0.0
    val count = ps.size
        
    ps.foreach(f => {
      sumLat += f._1
      sumLon += f._2
      sumTime += f._3
    })
    (sumLat/count, sumLon/count, sumTime/count)
    
  }
  
  def rawPhotos(lines: RDD[String]) : RDD[Photo] = {    
    def parseDouble(s: String) = try { s.toDouble } catch { case _ => 0 }
    def isRightFormat(l: String) = {
        val a = l.split(",")
        val dateFormat = new java.text.SimpleDateFormat("yyyy:MM:dd kk:mm:ss")
        
        if (a.length == 4 && parseDouble(a(1)) != 0 && parseDouble(a(2)) != 0){
          try {
            dateFormat.parse(a(3)) 
            true
          }
          catch {
            case _: Throwable => {}
            false
          }
          
        }
        else
          false
        
    }
    
    // Ignores empty lines:
    val lines_without_empties = lines.filter(!_.isEmpty())
    
    // Clean lines which are not in right format:
    val cleaned_lines = lines_without_empties.filter( f => isRightFormat(f) )
    
    //println(cleaned_lines.count)
    val dateFormat = new java.text.SimpleDateFormat("yyyy:MM:dd kk:mm:ss")
    cleaned_lines.map(l => {val a = l.split(","); (Photo(a(0), parseDouble(a(1)), parseDouble(a(2)), dateFormat.parse(a(3))))})
  }
  /** Classify photos by their cluster index */
  def classify(photos: RDD[Photo], means: Array[(Double, Double)]): RDD[(Int, Iterable[Photo])] = {
    val classification = photos.map(p => {val a = p; (findClosest((a.latitude, a.longitude), means), a)}).groupByKey()
    sumOfSquares(classification)
    classification
  }
  
  /** Classify datapoints by their cluster index in 3d */
  def classify3d(vectors: RDD[(Double,Double,Double)], means: Array[(Double, Double, Double)]): RDD[(Int, Iterable[(Double, Double, Double)])] = {
    val classification = vectors.map(p => {val a = p; (findClosest3d((a._1,a._2,a._3), means), a)}).groupByKey()
    classification
  }
  
  /** Recalculate cluster means */
  def refineMeans(classification: RDD[(Int, Iterable[Photo])], currentMeans: Array[(Double, Double)]) : Array[(Double, Double)] =  {
    var i = 1
    classification.map{(f => {i=i+1 ;averageVectors(f._2)})}.collect()
  }
  /** Recalculate cluster means 3d */
  def refineMeans3d(classification: RDD[(Int, Iterable[(Double, Double, Double)])], currentMeans: Array[(Double,Double,Double)]) : Array[(Double,Double,Double)] = {
    var i = 1
    classification.map{(f => {i=i+1 ;averageVectors3d(f._2)})}.collect()
  }
  /** Check if points are close enough to one another */
  def converged(kmeansEta: Double)(oldMeans: Array[(Double,Double)], newMeans: Array[(Double, Double)]): Boolean = {
    oldMeans.zip(newMeans).forall{case (om, nm) => distanceInMeters(om, nm) <= kmeansEta}
  }
  
  /** Check if points are close enough to one another 3d */
  def converged3d(kmeansEta: Double)(oldMeans: Array[(Double, Double, Double)], newMeans: Array[(Double, Double, Double)]) : Boolean = {
    oldMeans.zip(newMeans).forall{case (om, nm) => distanceIn3d(om, nm) <= kmeansEta}
  }
  
  /** Recursive function to calculate k-means */
  @tailrec final def kmeans(means: Array[(Double, Double)], vectors: RDD[Photo], iter: Int = 1): Array[(Double, Double)] = {
    //println(iter)
    val classification : RDD[(Int, Iterable[Photo])] = classify(vectors, means)
    val newMeans = refineMeans(classification, means)
    if (converged(kmeansEta)(means.sorted, newMeans.sorted)) { 
      newMeans
    }
    else if (iter >= kmeansMaxIterations) {
      newMeans 
    }
    else kmeans(newMeans, vectors, iter+1)
  }
  
  /** Recursive function to calculate k-means 3d */
  @tailrec final def kmeans3d(means: Array[(Double, Double, Double)], vectors: RDD[(Double,Double,Double)], iter: Int = 1): Array[(Double, Double, Double)] = {
    val classification : RDD[(Int, Iterable[(Double, Double, Double)])] = classify3d(vectors, means)
    val newMeans = refineMeans3d(classification, means)
    if (converged3d(kmeansEta)(means.sorted, newMeans.sorted)) {
      newMeans
    }
    else if (iter >= kmeansMaxIterations) {
      newMeans
    }
    else kmeans3d(means, vectors, iter+1)
  }
}