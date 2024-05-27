package kafka.temperature

import kafka.Executable
import logger.Logger
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}

import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.streams.kstream.{Materialized, TimeWindows, Windowed, WindowedSerdes}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes.{doubleSerde, longSerde, stringSerde}
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.common.serialization.Serde

import java.lang
import java.time.Duration
import scala.util.Try
import java.nio.ByteBuffer

class TupleSerializer extends Serializer[(Double, Int)] {
  override def serialize(topic: String, data: (Double, Int)): Array[Byte] = {
    if (data == null) return null
    val byteBuffer = ByteBuffer.allocate(12)
    byteBuffer.putDouble(data._1)
    byteBuffer.putInt(data._2)
    byteBuffer.array()
  }
}

class TupleDeserializer extends Deserializer[(Double, Int)] {
  override def deserialize(topic: String, data: Array[Byte]): (Double, Int) = {
    if (data == null) return null
    val byteBuffer = ByteBuffer.wrap(data)
    val first = byteBuffer.getDouble()
    val second = byteBuffer.getInt()
    (first, second)
  }
}

class TupleSerde extends Serde[(Double, Int)] {
  override def serializer(): Serializer[(Double, Int)] = new TupleSerializer
  override def deserializer(): Deserializer[(Double, Int)] = new TupleDeserializer
}

class RollingAvgTemperature(
    val broker: String
) extends Executable {
  private val consumerTopic = "i483-sensors-s2410014-BMP180-temperature"
  private val producerTopic = "i483-s2410014-BMP180_avg-temperature"

  private val streamProps = new Properties()
  // NOTE: The consumer group.id is not accidentally (or intentionally) the same as the group.id of the producers.
  // https://forum.confluent.io/t/inconsistent-group-protocol-exception-why/8387
  streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams")
  streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
  streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass.getName)
  streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass.getName)
  streamProps.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, "latest")

  val builder: StreamsBuilder = new StreamsBuilder

  override def exec(): Unit = {
    val temperatureValues: KStream[String, String] = builder.stream[String, String](consumerTopic)

    implicit val grouped: Grouped[String, Double] = Grouped.`with`(stringSerde, doubleSerde)
    implicit val produced: Produced[Windowed[String], String] = Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), stringSerde)
    implicit val tupleSerde: Serde[(Double, Int)] = new TupleSerde
    implicit val materialized: Materialized[String, (Double, Int), ByteArrayWindowStore] = {
      Materialized.as[String, (Double, Int), ByteArrayWindowStore]("avg-temperature-window-store")
        .withKeySerde(stringSerde)
        .withValueSerde(tupleSerde)
    }

    // Step 1: Select Key
    val keyedTemperatureValues: KStream[String, String] = temperatureValues
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
        (newValue + aggValue._1, aggValue._2 + 1)
      })

    // Step 5: Calculate average if threshold met
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
      .filter((_, value) => value.isDefined)
      .mapValues(value => {
        val result = BigDecimal(value.get).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble.toString
        println(s"平均気温:$result")
        result
      })

    // Step 7: produce processed stream data to specified topic
    resultStream.to(producerTopic)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamProps)
    streams.start()
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }
}
