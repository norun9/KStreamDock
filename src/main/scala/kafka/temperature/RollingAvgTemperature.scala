package kafka.temperature

import kafka.Executable
import kafka.util.KafkaConfig
import logger.Logger
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer }
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{ KafkaStreams, StreamsBuilder, StreamsConfig }

import java.util.Properties
import scala.jdk.CollectionConverters.*
import scala.util.{ Try, Failure }
import scala.util.control.Exception.ultimately
import kafka.util.KafkaStreamSelf

class RollingAvgTemperature(
    val broker: String,
    val groupId: String = "sample-group"
) extends Executable with Logger with KafkaStreamSelf {
  private val consumerTopic = "i483-sensors-s2410014-BMP180-temperature"
  private val producerTopic = "i483-s2410014-BMP180_avg-temperature"

  override def exec(): Unit = {
    val ppmThreshold = 700
    Try {
      val temperatures: KStream[String, String] = stream(consumerTopic)
      val temperatureValues: KStream[String, Double] = temperatures.mapValues(_.toDouble)

      val windowedAverages: KStream[String, String] = temperatureValues
        .groupByKey
        .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(30)))
        .aggregate(0.0)((key, newValue, aggValue) => aggValue + newValue)((window, aggValue) => aggValue / window.count())(
          Materialized.as("average-store")(Serdes.String, Serdes.Double)
        )
        .toStream
        .map((windowedKey, avgTemperature) => (windowedKey.toString, avgTemperature.toString))

      produce(windowedAverages, producerTopic)

      streamStart()
    } match {
      case Failure(ex) =>
        logger.error(ex.getMessage)
      case _ =>
      // do nothing
    }
  }
}
