package kafka.temperature

import kafka.util.{Executable, KafkaStreamSelf}
import kafka.util.serializer.TupleSerde
import org.apache.kafka.streams.kstream.{Materialized, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes.{doubleSerde, stringSerde}
import org.apache.kafka.common.serialization.Serde

import java.time.{Duration, Instant}
import scala.util.Try
import com.typesafe.scalalogging.LazyLogging

import java.time.format.DateTimeFormatter

class RollingAvgTemperature(
    val broker: String
) extends Executable
    with KafkaStreamSelf
    with LazyLogging {
  private val consumerTopic = "i483-sensors-s2410014-BMP180-temperature"
  private val producerTopic = "i483-s2410014-BMP180_avg-temperature"

  override def exec(): Unit = {
    val temperatureStreams: KStream[String, String] = stream(consumerTopic)
    implicit val grouped: Grouped[String, Double] = Grouped.`with`(stringSerde, doubleSerde)
    implicit val tupleSerde: Serde[(Double, Int)] = new TupleSerde
    implicit val materialized: Materialized[String, (Double, Int), ByteArrayWindowStore] = {
      Materialized.as[String, (Double, Int), ByteArrayWindowStore]("avg-temperature-window-store")
        .withKeySerde(stringSerde)
        .withValueSerde(tupleSerde)
    }

    // Step 1: Select Key
    val keyedTemperatureValues: KStream[String, String] = temperatureStreams
      .selectKey((key, _) => if (key == null) consumerTopic else key)

    // Step 2: Convert to Double if possible
    val validTemperatureValues: KStream[String, Double] = keyedTemperatureValues
      .flatMapValues(value => Try(value.toDouble).toOption)

    // Step 3: Group by key
    val groupedTemperatureValues: KGroupedStream[String, Double] = validTemperatureValues
      .groupBy((key, _) => key)

    // Step 4: Window and Aggregate
    val tumblingWindow: TimeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(30))
    val aggregatedTemperatureValues: KTable[Windowed[String], (Double, Int)] = groupedTemperatureValues
      .windowedBy(tumblingWindow)
      .aggregate((0.0, 0))((_: String, newValue: Double, aggValue: (Double, Int)) => {
        val aggTemperature = newValue + aggValue._1
        val aggCount = aggValue._2 + 1
        (aggTemperature, aggCount)
      })

    // Step 5: Calculate average if threshold met
    // The total number of counts in one window reaches 20 at exactly 30 seconds after the previous measurement.
    val temperatureMeasurementThreshold = 20
    val averageTemperatureValues: KTable[Windowed[String], Option[Double]] = aggregatedTemperatureValues
      .mapValues((result: (Double, Int)) => {
        val sumOfTemperature = result._1
        val windowRecordCount = result._2
        // If the number of elements is less than 20 in a 5-minute window, it is not measured
        if (windowRecordCount >= temperatureMeasurementThreshold)
          Some(sumOfTemperature / windowRecordCount.toDouble)
        else
          None
      })

    // Step 6: Filter and process results
    val resultStream: KStream[Windowed[String], String] = averageTemperatureValues.toStream
      .filter((windowedKey, value) => {
        if (value.isDefined) {
          // debugging
          val windowStart = windowedKey.window().start()
          val windowEnd = windowedKey.window().end()
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
          val startTime = Instant.ofEpochMilli(windowStart).atZone(java.time.ZoneId.systemDefault()).format(formatter)
          val endTime = Instant.ofEpochMilli(windowEnd).atZone(java.time.ZoneId.systemDefault()).format(formatter)
          logger.info(s"$startTime - $endTime")
        }
        value.isDefined
      })
      .mapValues(value => {
        val result = BigDecimal(value.get).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble.toString
        logger.info(s"Average Temperature for Five Minutes: $result C")
        result
      })

    // Step 7: produce processed stream data to specified topic
    produce(resultStream, producerTopic)

    streamStart()
  }
}
