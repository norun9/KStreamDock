package kafka.co2

import kafka.util.{Executable, KafkaConsumerSelf, KafkaProducerSelf}
import org.apache.kafka.clients.consumer.ConsumerRecords
import scala.jdk.CollectionConverters._
import scala.util.{Try, Failure}
import scala.util.control.Exception.ultimately
import com.typesafe.scalalogging.LazyLogging

/** Publish message according to CO2 threshold value
  * @param broker
  * @param groupId
  */
class CO2ThresholdDetector(
    val broker: String
) extends Executable
    with KafkaConsumerSelf
    with KafkaProducerSelf
    with LazyLogging {
  private val consumerTopic = "i483-sensors-s2410014-SCD41-co2"
  private val producerTopic = "i483-s2410014-co2_threshold-crossed"

  subscribe(List(consumerTopic))

  override def exec(): Unit = {
    val sendToTopic = producer(producerTopic) _
    val ppmThreshold: Int = 700
    Try {
      ultimately {
        // equal to finally
        consumerClose()
      } {
        while (true) {
          val records: ConsumerRecords[String, String] = listConsumerRecords()
          for (record <- records.asScala) {
            val ppmOpt: Option[Int] = Try(record.value.toInt).toOption
            ppmOpt.foreach(ppm => {
              logger.info(s"Measurement CO2: $ppm ppm")
              if (ppm > ppmThreshold) {
                sendToTopic("yes")
              } else {
                sendToTopic("no")
              }
            })
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
