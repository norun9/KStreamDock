package kafka.temperature

import kafka.Executable
import kafka.util.KafkaConfig
import logger.Logger
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer }
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.kstream.{ KStream, TimeWindows }
import org.apache.kafka.streams.{ KafkaStreams, StreamsBuilder, StreamsConfig }

import java.util.Properties
import scala.jdk.CollectionConverters.*
import scala.util.{ Try, Failure }
import scala.util.control.Exception.ultimately
import kafka.util.KafkaStreamSelf

class RollingAvgTemperature(
    val broker: String
) extends Executable with Logger with KafkaStreamSelf {
  private val consumerTopic = "i483-sensors-s2410014-BMP180-temperature"
  private val producerTopic = "i483-s2410014-BMP180_avg-temperature"

  override def exec(): Unit = {
    Try {
      val temperatures: KStream[String, String] = stream[String, String](consumerTopic)

//      val avgTemperature = safeTemperatureValues
//        .flatMapValues { value =>
//          Try(value.toDouble).toOption
//        }
//        .groupByKey
//        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(30)))
//        .aggregate((0.0, 0))((_: String, newValue: Double, aggValue: (Double, Int)) => {
//          val updatedValue = (newValue + aggValue._1, aggValue._2 + 1)
//          println(s"Aggregating - New Value: $newValue, Aggregated Value: $aggValue, Updated Value: $updatedValue")
//          updatedValue
//        })
//        .mapValues((_, result) => {
//          val average = result._1 / result._2
//          println(s"Calculating average - Sum: ${result._1} Count: ${result._2}, Average: $average")
//          average
//        })

      val temperatureValues: KStream[String, Double] = temperatures
        .flatMapValues(value => Try(value.toDouble).toOption)

      val windowedAverages: KStream[String, String] = temperatureValues
        .groupByKey
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(30)))
        .aggregate(0.0, ((key, newValue, aggValue) => aggValue + newValue)((window, aggValue) => aggValue / window.count()))
        .toStream
        .map((windowedKey, avgTemperature) => (windowedKey.toString, avgTemperature.toString))

      produce(windowedAverages, producerTopic)

      val streams: KafkaStreams = streamStart()
      sys.ShutdownHookThread {
        streams.close(Duration.ofSeconds(10))
      }
    } match {
      case Failure(ex) =>
        logger.error(ex.getMessage)
      case _ =>
      // do nothing
    }
  }
}
