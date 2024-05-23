package kafka

import logger.Logger
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecords, KafkaConsumer }
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters.*
import java.util.Properties

trait KafkaConsumerSelf extends Logger {
  val broker: String

  protected lazy val consumerProps = new Properties()
  protected lazy val groupId = "sample-group"
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  protected lazy val consumer = new KafkaConsumer[String, String](consumerProps)

  def subscribe(topics: List[String]): Unit = {
    consumer.subscribe(topics.asJava)
  }

  def consumerClose(): Unit = {
    consumer.close()
  }

  def listConsumerRecords(): ConsumerRecords[String, String] = {
    consumer.poll(java.time.Duration.ofMillis(100))
  }
}
