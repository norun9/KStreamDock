package kafka.co2

import kafka.{KafkaConsumerSelf, KafkaProducerSelf}
import logger.Logger
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig}

import java.util.Properties
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.control.Exception.ultimately

class WarnCO2Producer(val broker: String) extends Logger with KafkaConsumerSelf with KafkaProducerSelf {
  private val consumerTopic = "i483-sensors-s2410014-SCD41-co2"
  private val producerTopic = "i483-s2410014-co2_threshold-crossed"
  subscribe(List(consumerTopic))

  def exec(): Unit = {
    val ppmThreshold = 700
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
    }
  }
}
