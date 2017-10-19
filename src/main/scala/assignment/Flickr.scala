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
    
    /** Options for test files */
    //val lines = sc.textFile("src/main/resources/photos/dataForBasicSolution.csv")
    val lines   = sc.textFile("src/main/resources/photos/flickrDirtySimple.csv")
    //val lines = sc.textFile("src/main/resources/photos/flickrDirtySimple_SYLK.csv")
    //val lines = sc.textFile("src/main/resources/photos/DirtyTest.csv")
    //val lines = sc.textFile("src/main/resources/photos/elbow.csv")
    //val lines = sc.textFile("src/main/resources/photos/smallDataset.csv")
    //val lines = sc.textFile("src/main/resources/photos/flickr3D2.csv")
    //val lines = sc.textFile("src/main/resources/photos/flickr3D.csv")
    
    val lines_without1 = lines.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)   
    val raw = rawPhotos(lines_without1)
    val raw3d = getScaledRDD(raw)
    // println(raw.count)
    
    val latMinMax = getLatMinMax(raw)
    val lonMinMax = getLonMinMax(raw)
     
    var textOutput = ""
    
    /** 2D branch */
    if (!enable3d) {
      val initialMeans : Array[(Double, Double)] = initializeMeans(raw)

      println("Initial means:")
      var j : Int = 0
      initialMeans.foreach(f => {j = j + 1;println(j + " "+f._1 + "," + f._2)})
      val means   = kmeans(initialMeans, raw)
      
      println("Final means:")
      textOutput = {
        var text = ""
        means.foreach(d => text = text.concat(d._1 + "," + d._2 + "\n"))
        text.substring(0, text.length()-2)
      }
      println(textOutput)
    }
    
    /** 3D branch */
    else {
      val initialMeans3d = initializeMeans3d(raw3d)

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
    
    /** Writing results into data_stream.csv */
    val fw = new PrintWriter(new File("data_stream.csv"))
    new PrintWriter("data_stream.csv") {write(textOutput); close }
  }
}


class Flickr extends Serializable {
  
  /** Choose to enable 3d or not */
  def enable3d : Boolean = true;
  
  /** K-means parameter: Convergence criteria. K-means 3d uses kmeansEta/20 convergence criteria. */
  def kmeansEta: Double = 20.0D
  
  /** K-means parameter: Number of clusters */
  def kmeansKernels = 16
  
  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 20
  
  /** Date format to be used */
  def dateFormat : SimpleDateFormat = new java.text.SimpleDateFormat("yyyy:MM:dd HH:mm:ss")
  
  /** Maximum and minimum dates for the purpose of scaling dates for seasons */
  def maxDate : Date = dateFormat.parse("2016:12:31 23:59:59")
  def minDate : Date = dateFormat.parse("2016:01:01 00:00:00")
  
  /** Max and minimum latitudes and longitudes from data, for scaling and initializing */
  def getLatMinMax(raw : RDD[Photo]) : (Double, Double) = {
    (raw.takeOrdered(1)(Ordering[Double].reverse.on(p=>p.latitude))(0).latitude,
    (raw.takeOrdered(1)(Ordering[Double].on(p=>p.latitude))(0).latitude))
  }
  def getLonMinMax(raw : RDD[Photo]) : (Double, Double) = {
    (raw.takeOrdered(1)(Ordering[Double].reverse.on(p=>p.longitude))(0).longitude,
    (raw.takeOrdered(1)(Ordering[Double].on(p=>p.longitude))(0).longitude))
  }
                   
  
  /** Scale coordinates and dates (in double) to 0-100 scale */
  def scaleTo100(value : Double, maxVal : Double, minVal : Double) : Double = {
    (value - minVal)/(maxVal - minVal)*100
  }
  
  /** Scale coordinates back to original form */
  def scaleCoordinatesFrom100(value : Double, maxVal : Double, minVal : Double) : Double = {
    value*(maxVal-minVal)/100 + minVal
  }
  
  /** Scale dates back to date form */
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
  
  /** Calculate long that represents the time from start of the year. To use seasons. */
  def sinceStartOfYear(date : Date) : Long = {
    // java date.getYear gives currentYear-1900
    val year = date.getYear()+1900
    val dateAtStartOfYear = dateFormat.parse(year.toString()+":01:01 00:00:00")
    date.getTime - dateAtStartOfYear.getTime
  } 
  /** Initialization means for k-means */
  def initializeMeans(raw : RDD[Photo]) : Array[(Double, Double)] = {
    var initialMeans : Array[(Double, Double)] = raw.takeSample(false, kmeansKernels).map{p => (p.latitude, p.longitude)} 
    while(initialMeans.distinct.size != initialMeans.size) {
        initialMeans = raw.takeSample(false, kmeansKernels).map{p => (p.latitude, p.longitude)}
    }
    initialMeans
  }
  
  /** Initialization means for k-means 3d */
  def initializeMeans3d(raw3d : RDD[(Double, Double, Double)]) : Array[(Double, Double, Double)] = {
    var initialCoords = raw3d.takeSample(false, kmeansKernels).map(p => (p._1, p._2))
    var initialTimes = raw3d.takeSample(false, kmeansKernels).map(p => (p._3))
    
    /* Avoiding having same initial coordinates */
    while(initialCoords.distinct.size != initialCoords.size) {        
      initialCoords = raw3d.takeSample(false, kmeansKernels).map(p => (p._1, p._2))       
    }
    var initialMeans3d = initialCoords.zip(initialTimes).map(f => (f._1._1, f._1._2, f._2))
    initialMeans3d
  }
  
  /** Getting 0-100 scaled RDD for 3d */
  def getScaledRDD(raw : RDD[Photo]) : RDD[(Double, Double, Double)] = {
      val latMinMax = getLatMinMax(raw)
      val lonMinMax = getLonMinMax(raw)                                 
      
      val scaledRDD = raw.map(p => {(
        (scaleTo100(p.latitude, latMinMax._2, latMinMax._1)),
        (scaleTo100(p.longitude, lonMinMax._2, lonMinMax._1)),
        (scaleTo100(sinceStartOfYear(p.datetime).toDouble,
            sinceStartOfYear(maxDate).toDouble,
            sinceStartOfYear(minDate).toDouble))
        )})
      scaledRDD    
  }
  

  /** Calculating the distance in meters */
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
  
  /** Calculating the distance in 3d */
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
  
  /** Average the vectors 3d */
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
    
    /** Tries to convert String to Double. If not possible, 0. */
    def parseDouble(s: String) = try { s.toDouble } catch { case _ => 0 }
    
    /** Checks whether the format of line (string) is in right format. 
     *  Returns true if ok, in other cases false. For 2D purposes. */
    def isRightFormat(l: String) = {
        val a = l.split(",")
        if ((a.length == 4 || a.length ==3 ) && parseDouble(a(1)) != 0 && parseDouble(a(2)) != 0){
          true
        }
        else {
          false
        }
    }
    /** Checks whether the format of line (string) is in right format. 
     *  Returns true if ok, in other cases false For 3D purposes. */
    def isRightFormat3D(l: String) = {
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
    
    /** Ignores empty lines: */
    val lines_without_empties = lines.filter(!_.isEmpty())
    
    /** Clean lines which are not in right format. Processing depends on whether the 3rd dimension is 
     *  activated or not. */
    val dateFormat = new java.text.SimpleDateFormat("yyyy:MM:dd kk:mm:ss")
    if (enable3d) {
        val cleaned_lines = lines_without_empties.filter( f => isRightFormat3D(f) )
        cleaned_lines.map(l => {val a = l.split(","); (Photo(a(0), parseDouble(a(1)), parseDouble(a(2)), dateFormat.parse(a(3))))})
    }
    else {
        val cleaned_lines = lines_without_empties.filter( f => isRightFormat(f) )        
        cleaned_lines.map(l => {val a = l.split(","); (Photo(a(0), parseDouble(a(1)), parseDouble(a(2)), dateFormat.parse("2000:01:01 00:00:00")))})
    }
  }
  
  /** Classify photos by their cluster index */
  def classify(photos: RDD[Photo], means: Array[(Double, Double)]): RDD[(Int, Iterable[Photo])] = {
    val classification = photos.map(p => {val a = p; (findClosest((a.latitude, a.longitude), means), a)}).groupByKey()
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
  
  /** Check if points are close enough to one another 3d. KmeansEta has been divided by 20, 
   *  since the scale in 3d is a lot smaller, 1 to 100 */
  def converged3d(kmeansEta: Double)(oldMeans: Array[(Double, Double, Double)], newMeans: Array[(Double, Double, Double)]) : Boolean = {
    oldMeans.zip(newMeans).forall{case (oldmeans, newmeans) => distanceIn3d(oldmeans, newmeans) <= kmeansEta/20}
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