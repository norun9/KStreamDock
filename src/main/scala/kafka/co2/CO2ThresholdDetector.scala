package kafka.co2

import kafka.util.{ KafkaConsumerSelf, KafkaProducerSelf }
import logger.Logger
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer }
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.{ StreamsBuilder, StreamsConfig }
import kafka.Executable
import java.util.Properties
import scala.jdk.CollectionConverters.*
import scala.util.{ Try, Failure }
import scala.util.control.Exception.ultimately

/** Publish message according to CO2 threshold value
  * @param broker
  * @param groupId
  */
class CO2ThresholdDetector(
    val broker: String,
    val groupId: String = "sample-group"
) extends Executable with Logger with KafkaConsumerSelf with KafkaProducerSelf {
  private val consumerTopic = "i483-sensors-s2410014-SCD41-co2"
  private val producerTopic = "i483-s2410014-co2_threshold-crossed"

  subscribe(List(consumerTopic))

  override def exec(): Unit = {
    val ppmThreshold: Int = 700
    Try {
      ultimately {
        // equal to finally
        consumerClose()
      }
      {
        val sendToTopic = producer(producerTopic)
        while (true) {
          val records: ConsumerRecords[String, String] = listConsumerRecords()
          for (record <- records.asScala) {
            val ppm: Int = record.value.toInt
            logger.info(s"CO2 $ppm ppm")
            if (ppm > ppmThreshold) {
              sendToTopic("yes")
            } else {
              sendToTopic("no")
            }
          }
        }
      }
    } match {
      case Failure(ex) =>
        logger.error(ex.getMessage)
      case _ =>
      // do nothing
    }
  }
}
