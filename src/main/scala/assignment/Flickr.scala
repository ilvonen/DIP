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

    val lines   = sc.textFile("src/main/resources/photos/dataForBasicSolution.csv")
    val raw     = rawPhotos(lines)
    
    
    //val tupleRdd = lines.map(l => {val a = l.split(","); (a(0), a(1), a(2), a(3))})
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
    /*
    for ( a <- tupleRdd ){
      val b = a._2
      val c = parseDouble(b)
      
      val d = 25.650157
      
      if (c > d) {
        //println(c)
      }
    }
    * 
    */
    val count = raw.count()
    
    println(count)
    
    val k = kmeansKernels
    val minLatitude = raw.filter(p => p.latitude > 0).takeOrdered(1)(Ordering[Double].on(p=>p.latitude))(0).latitude
    val maxLatitude = raw.takeOrdered(1)(Ordering[Double].reverse.on(x=>x.latitude))(0).latitude
    val minLongitude = raw.filter(p => p.longitude > 0).takeOrdered(1)(Ordering[Double].on(p=>p.longitude))(0).longitude
    val maxLongitude = raw.takeOrdered(1)(Ordering[Double].reverse.on(x=>x.longitude))(0).longitude
    
    /*
    println("maxLatitude: " + maxLatitude)
    println("minLatitude: " + minLatitude)
    println("maxLongitude: " + maxLongitude)
    println("minLongitude: " + minLongitude)
    */
 
    
    val initialMeans = {
      //var meansArray : Array[(Double, Double)] = Array((5.5,2.6))
      //val meansArray = scala.collection.mutable.ArrayBuffer[(Double,Double)]()
      val meansArray = Array.ofDim[(Double, Double)](k)
      
      
      for (i <- 0 to k-1) {
        val random1 = scala.util.Random.nextDouble()
        val random2 = scala.util.Random.nextDouble()
        
        val randomLatitude = minLatitude + random1*(maxLatitude - minLatitude)
        val randomLongitude = minLongitude + random2*(maxLongitude - minLongitude)
        
        //println("lat:" + randomLatitude +" ; " + "lon:" + randomLongitude)
        
        val latLonPair = (randomLatitude, randomLongitude)
        
        
        
        
        //println(new Random(i).nextInt(maxLatitude-minLatitude))
        //meansArray :+ latLonPair
        //meansArray+=(latLonPair)
        meansArray(i) = latLonPair
        //println(meansArray(0)._1)
      }
      meansArray
    }
    //println("Contents of initialMeans: Array(Double,Double):")
    for (i <- 0 to k-1) {
      //println("lat:" + initialMeans(i)._1 +" ; " + "lon:" + initialMeans(i)._2)
    }
    
    //println(initialMeans.length)
    //classify(raw, initialMeans)
    //val initialMeans2: Array[(Double, Double)] = Array((59.7, 21.2),(63.4, 30.1), (67.0,  28.4), (64.5, 23.7))
    val means   = kmeans(initialMeans, raw)
    println(means.foreach(f => println(f._1 + "," + f._2)))
    val fw = new PrintWriter(new File("data_stream.csv"))
    means.foreach(d => Files.write(Paths.get("data_stream.csv"), (d._1 + "," + d._2 + "\n").getBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND))
    
    
    
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
    println("rawPhotos")
    def parseDouble(s: String) = try { s.toDouble } catch { case _ => 0 }
    val photos = lines.map(l => {val a = l.split(","); (Photo(a(0), parseDouble(a(1)), parseDouble(a(2))))})
    //photos.collect.foreach(println)
    photos
  }
  def classify(photos: RDD[Photo], means: Array[(Double, Double)]): RDD[(Int, Iterable[Photo])] = {
    //means.map{(_, photos)}.toMap ++ photos.map.groupBy(findClosest(, means))
    //val classification = photos.map(p => {val a = p; (a, findClosest((a.latitude, a.longitude), means))})
    
    //val classification = photos.map(p => {val a = p; (findClosest((a.latitude, a.longitude), means), a)}).groupByKey()
    val classification = photos.map(p => {val a = p; (findClosest((a.latitude, a.longitude), means), a)}).groupByKey()
    
    //photos.map(p => {val a = p; (findClosest((a.latitude, a.longitude), means),a)})
    
    //classification.saveAsTextFile("C:/Users/Tuomas/Documents/GitHub/DIP/TestOutput")
    
    //val textOutputRDD = classification.map(c => {val a = c; (a._1.latitude.toString(),a._1.longitude,a._2) })
    //val textOutputRDD = classification.map(c => c._1.latitude.toString() + "," + c._1.longitude + "," + c._2)
    //val textOutputRDD2 = textOutputRDD.map(f => f._1+"\t"+f._2)
    //val textOutputRDD = classification.map(c => c._1.toString() + "," + c._1.latitude + "," + c._2.longitude)
    
    //textOutputRDD.coalesce(1).saveAsTextFile("TestOutput1")
    
    classification
  }
  
  def refineMeans(classification: RDD[(Int, Iterable[Photo])], currentMeans: Array[(Double, Double)]) : Array[(Double, Double)] =  {
    //val newMeans = Array.ofDim[(Double, Double)](kmeansKernels)
    /*val newMeans : RDD[(Int, (Double,Double))] = {
      classification.map{f => (f._1, averageVectors(f._2))}
    }
    */
    val newMeans = classification.map{(f => averageVectors(f._2))}.collect()
    //var i = 0;
    //classification.groupByKey.foreach(f => { newMeans(f._1) = averageVectors(f._2)})
    //val groupedData = classification.groupByKey()
    
    //groupedData.foreachPartition(f => {newMeans(f._1)
    //classification.groupBy(_._2).foreach(f => { newMeans(i) = averageVectors(f._2) }
    //classification.gr
    
    
    //val newClassification = currentMeans.map(m => find
    newMeans
    
  }
  
  
  def converged(kmeansEta: Double)(oldMeans: Array[(Double,Double)], newMeans: Array[(Double, Double)]): Boolean = {
    oldMeans.zip(newMeans).forall{case (om, nm) => distanceInMeters(om, nm) <= kmeansEta}
  }
  
  def textOutput(classification : RDD[(Int, Iterable[Photo])]) {
    println(Flickr.sc.version)
    println("finished")
    //val textOutputRDD = classification.map(f => {f._1.toString + f._2.flatten(p => {p.latitude.toString() + p.longitude  })
    
    
    
    
    //val rdd = Flickr.sc.parallelize(Seq(1 -> Seq((4.1, 3.4), (5.6, 6.7), (3.4, 9.0)), 2 -> Seq((0.4, -4.1), (-3.4, 6.7), (7.0, 8.9))))
    
    val csvLike =
      //for((key, coords) <- rdd; (lat, lon) <- coords) yield s"$key,$lat,lon"
      //rdd.flatMap { case (key, coords) => coords.map { case (lat, lon) => s"$key,$lat,$lon" } }
      classification.flatMap { case (key, coords) => coords.map { case (photo) =>  val lat = photo.latitude;
                                                                                   val lon = photo.longitude;
                                                                                   s"$key,$lat,$lon" }}
    //for (row <- csvLike) println(row)
    
    
    
    
    // this works, need the index in front though
    val textOutputRDD = classification.flatMap(f => f._2.map(p => p.latitude.toString() + "," + p.longitude.toString()))
    
    val output = classification.flatMap(s=>{
      var list=List[String]()
      
      val b = new StringBuilder
      for (latlon <- s._2) {
        //println(s._1.toString() + "," + latlon.latitude + "," + latlon.longitude)
        
        //list :+ s._1.toString() + "," + latlon.latitude.toString() + "," + latlon.longitude.toString()'
        
        
      }
      println(list)
      list
    })
    
    /*
    val file = "whatever.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- output) {
      writer.write(x + "\n")
    }
    writer.close()
    */
    
   
    //val textOutputRDD = classification.map(f => {f._1.toString() + f._2.iterator + f._2.iterator.next().toString()})
    //val photos = lines.map(l => {val a = l.split(","); (Photo(a(0), parseDouble(a(1)), parseDouble(a(2))))})
    val random = scala.util.Random
    random.nextInt(100000)
    
    csvLike.saveAsTextFile("TestOutput" + random.nextInt(100000))
    
    //textOutputRDD2.saveAsTextFile("TestOutput" + random.nextInt(100000))
  }
    
  @tailrec final def kmeans(means: Array[(Double, Double)], vectors: RDD[Photo], iter: Int = 1): Array[(Double, Double)] = {
    println(iter)
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
