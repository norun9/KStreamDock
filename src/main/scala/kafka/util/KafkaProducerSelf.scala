package kafka.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

trait KafkaProducerSelf extends LazyLogging {
  val broker: String

  private lazy val producerProps = new Properties()
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  private val producer = new KafkaProducer[String, String](producerProps)

  // Currying
  def producer(topic: String)(message: String): Unit = {
    val record = new ProducerRecord[String, String](topic, null, message)
    producer.send(record)
    logger.info(s"Kafka Producer: $topic - $message")
  }
}
