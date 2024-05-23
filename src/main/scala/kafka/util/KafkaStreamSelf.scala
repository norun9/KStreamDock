package kafka.util

import logger.Logger
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{ StreamsBuilder, StreamsConfig, KafkaStreams }

import java.util.Properties

trait KafkaStreamSelf extends Logger {
  val broker: String
  val groupId: String

  private val streamProps = new Properties()
  streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId)
  streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
  streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[StringDeserializer])
  streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[StringDeserializer])

  private val builder = new StreamsBuilder()

  def stream(topic: String): KStream[String, String] = {
    builder.stream[String, String](topic)
  }

  def produce(kv: KStream[String, String], topic: String): Unit = {
    kv.to(topic)
  }

  def streamStart(): Unit = {
    val streams = new KafkaStreams(builder.build(), streamProps)
    streams.start()
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }
}
