package poc

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object streamingPoc {
  
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf,seconds(1))
  
  val lines = ssc.SocketTextStream("localhost",9999)
  
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word,1))
  val wordCounts = pairs.reduceBykey(_+_)
  wordCounts.print()
  
}