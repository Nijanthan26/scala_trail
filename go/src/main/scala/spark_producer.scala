
import java.util.Properties
import org.apache.kafka.clients.producer._

object spark_producer {
  
  def main(args: Array[String]): Unit = {
    
 
 val  props = new Properties()
props.put("bootstrap.servers", "172.31.6.163:9092")
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
val producer = new KafkaProducer[String, String](props)
val TOPIC="go_topic"
val record = new ProducerRecord(TOPIC, "1", "hello 1")
producer.send(record)
producer.close()
  
}
}