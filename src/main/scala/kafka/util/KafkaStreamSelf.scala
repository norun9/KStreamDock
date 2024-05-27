package kafka.util

import logger.Logger
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{ StreamsBuilder, StreamsConfig, KafkaStreams }
import org.apache.kafka.common.serialization.Serdes
import java.util.UUID
import java.util.Properties

trait KafkaStreamSelf extends Logger {
  val broker: String
  val groupId: String = "kafka-streams"
  private val streamProps = new Properties()
  streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId)
  streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
  streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
  streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass.getName)

  private val builder = new StreamsBuilder()

  def stream(topic: String): KStream[Array[Byte], String] = {
    builder.stream[Array[Byte], String](topic)
  }

  def produce(kv: KStream[Array[Byte], String], topic: String): Unit = {
    kv.to(topic)
  }

  def streamStart(): KafkaStreams = {
    val streams = new KafkaStreams(builder.build(), streamProps)
    streams.start()
    streams
  }
}
