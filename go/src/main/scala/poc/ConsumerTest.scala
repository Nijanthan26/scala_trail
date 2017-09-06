package poc

import java.util.Properties

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.types.{ StructType, StructField, StringType }
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.log4j.{ Level, Logger }

//import io.confluent.connect.avro.AvroConverter
import io.confluent.kafka.serializers.KafkaAvroDecoder

import org.apache.spark.streaming.kafka010.{ CanCommitOffsets, HasOffsetRanges }
import org.apache.spark.TaskContext

object ConsumerTest extends App {

  val conf = new SparkConf().setAppName("kafka-consumer")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(20))
  val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092", "schema.registry.url" -> "http://localhost:8081", "auto.offset.reset" -> "largest", "enable.auto.commit" -> "false", "group.id" -> "group1")
  val topics = "test_mysql_test_table"
  val topicSet = Set(topics)
  val messages = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, topicSet).map(_._2)
  val lines = messages.map(data => data.toString)

  messages.foreachRDD(rdds => {
    print("Hello How R u before offsetRanges")
    val offsetRanges = rdds.asInstanceOf[HasOffsetRanges].offsetRanges
    print("hello how r u  after offsetRanges")
    messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  })
  
  
  
//  messages.foreachRDD(rdds => {
//    val offsetRanges = rdds.asInstanceOf[HasOffsetRanges].offsetRanges
//    lines.foreachRDD(rdd => {
//      if (rdd.count != 0) {
//        val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
//        val data = hiveContext.read.json(rdd)
//        data.printSchema()
//        data.show()
//        data.registerTempTable("sourceData")
//        hiveContext.sql("INSERT INTO TABLE default.kafka_demo_1 SELECT * FROM sourceData")
//        messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//        println("RDD has data")
//      } else {
//        println("Inside else block")
//        println("RDD is Empty")
//      }
//    })
// })

  
//  lines.foreachRDD(rdd => {
//    if (rdd.count != 0) {
//      messages.foreachRDD { rdds =>
//        val offsetRanges = rdds.asInstanceOf[HasOffsetRanges].offsetRanges
//        val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
//        val data = hiveContext.read.json(rdd)
//        data.printSchema()
//        data.show()
//        data.registerTempTable("sourceData")
//        hiveContext.sql("INSERT INTO TABLE default.kafka_demo_1 SELECT * FROM sourceData")
//        messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//      }
//
//      println("RDD has data")
//
//    } else {
//      println("RDD is Empty")
//    }
//
//  })

  ssc.start()
  ssc.awaitTermination()

}