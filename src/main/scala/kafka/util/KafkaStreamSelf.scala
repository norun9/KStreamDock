package kafka.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.kstream.{Windowed, WindowedSerdes}
import org.apache.kafka.streams.scala.kstream.{KStream, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, kstream}

import java.util.Properties
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde

import java.time.Duration
import scala.sys.ShutdownHookThread
import java.util.UUID

trait KafkaStreamSelf extends LazyLogging {
  val broker: String
  private val groupId: String = s"kafka-streams-${UUID.randomUUID()}"
  private val streamProps = new Properties()
  // NOTE: The consumer group.id is not accidentally (or intentionally) the same as the group.id of the producers.
  // https://forum.confluent.io/t/inconsistent-group-protocol-exception-why/8387
  streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId)
  streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
  streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass.getName)
  streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass.getName)

  private val builder = new StreamsBuilder()

  implicit protected val produced: Produced[Windowed[String], String] =
    Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), stringSerde)

  protected def stream(topic: String): kstream.KStream[String, String] = {
    builder.stream[String, String](topic)
  }

  protected def produce(kv: KStream[Windowed[String], String], topic: String): Unit = {
    kv.to(topic)
    logger.info(s"Kafka Streams: $topic")
  }

  protected def streamStart(): ShutdownHookThread = {
    val streams = new KafkaStreams(builder.build(), streamProps)
    streams.start()
    streams
  }
}
