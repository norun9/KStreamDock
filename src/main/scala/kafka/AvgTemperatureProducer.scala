package kafka

import logger.Logger
import org.apache.kafka.clients.consumer.{ ConsumerRecord, ConsumerRecords, KafkaConsumer, ConsumerConfig }
import org.apache.kafka.streams.{ StreamsBuilder, StreamsConfig, KafkaStreams }
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.common.serialization.StringDeserializer
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.Exception.ultimately
import java.util.Properties

class AvgTemperatureProducer(conf: KafkaConfig) extends Logger {
  private val props = new Properties()
  private val broker = s"${ conf.server.address }:${ conf.server.port }"
  private val groupId = "sample-group"

  props.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId)
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[StringDeserializer])

  private val builder = new StreamsBuilder()

  def producer(): Unit = { ??? }

  def process(): Unit = {
    val ppmThreshold = 700
    Try {
      {
        while (true) {
//          TODO: ここを修正、windowsとかを利用する
//          val textLines: KStream[String, String] = builder.stream[String, String](conf.topic.temperature)
//          val wordCounts: Nothing = textLines.flatMapValues((textLine) => Arrays.asList(textLine.toLowerCase.split("\\W+"))).groupBy((key, word) =>
//            word
//          ).count(Materialized.as[String, Long, Nothing]("counts-store"))
//          wordCounts.toStream.to("i483-s2410014-BMP180_avg-temperature", Produced.`with`(Serdes.String, Serdes.Long))
//          val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
//          streams.start()
//          sys.ShutdownHookThread {
//            streams.close(10, TimeUnit.SECONDS)
//          }
        }
      }
    }
  }
}
