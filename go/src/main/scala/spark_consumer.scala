
import java.util.Properties
import org.apache.spark.SparkContext._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object spark_consumer {
  
    def main(args: Array[String]): Unit = {
    
 
  
          val conf = new SparkConf().setAppName("Streaming")
        	val sc = new SparkContext(conf)
          
          val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
          val ssc = new StreamingContext(sc, Seconds(10))
          //val kafkaStream = KafkaUtils.createStream(ssc, "172.31.6.163:2181","spark-streaming-consumer-group", Map("go_topic" -> 5)).map(_._2)
          val kafkaStream = KafkaUtils.createStream(ssc, "172.31.15.81:2181","spark-streaming-consumer-group", Map("test-mysql-t1" -> 5)).map(_._2)
          kafkaStream.print()
          ssc.start()
          ssc.awaitTermination()  
  
}
}