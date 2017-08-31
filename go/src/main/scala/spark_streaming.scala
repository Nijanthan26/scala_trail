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
import io.confluent.kafka.serializers.KafkaAvroDecoder

object spark_streaming extends App {

  val conf = new SparkConf().setAppName("kafka-consumer")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(20))

  //  import hiveContext.implicits._
  //  import hiveContext.sql

  //    val kafkaStream = KafkaUtils.createStream(ssc, "172.31.6.163:2181", "spark-streaming-consumer-group", Map("go_topic" -> 5)).map(_._2)
  //    println("kafkaStream.print()")
  //    kafkaStream.print()
  

  val kafkaParams = Map[String, String]("metadata.broker.list" -> "172.31.15.81:9092", "schema.registry.url" -> "http://localhost:8081", "auto.offset.reset" -> "smallest")
  val topics = "go_mysql_test"
  val topicSet = Set(topics)
  val messages = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, topicSet).map(_._2)
  messages.print()

  val lines = messages.map(data => data.toString)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  lines.foreachRDD(rdd => {
    //  val sqlContext = SQLContextSingleton.getInstance(jsonRDD.sparkContext)

    if (rdd.count != 0) {
      val data = hiveContext.read.json(rdd)
      data.printSchema()
      data.show()
      data.registerTempTable("sourceData")
      hiveContext.sql("INSERT INTO TABLE test.avro_test SELECT * FROM sourceData")
    } else {
      println("RDD is Empty")
    }

  })

  ssc.start()
  ssc.awaitTermination()

}