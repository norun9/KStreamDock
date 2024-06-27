package kafka.co2

import kafka.util.{Executable, KafkaConsumerSelf, KafkaProducerSelf, TopicInfo}
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}
import scala.util.control.Exception.ultimately
import com.typesafe.scalalogging.LazyLogging

/** Publish message according to CO2 threshold value
  * @param broker
  * @param groupId
  */
class CO2ThresholdDetector(
    val broker: String,
    topic: TopicInfo
) extends Executable
    with KafkaConsumerSelf
    with KafkaProducerSelf
    with LazyLogging {
  private val consumerTopic = topic.consumerTopic
  private val producerTopic = topic.producerTopic

  subscribe(List(consumerTopic))

  override def exec(): Unit = {
    val sendToTopic = producer(producerTopic) _
    val ppmThreshold: Int = 700
    Try {
      ultimately {
        // equal to finally
        consumerClose()
      } {
        // Store variable here to manage previous state
        var lessThanThreshold: Option[Boolean] = None
        while (true) {
          val records: ConsumerRecords[String, String] = listConsumerRecords()
          for (record <- records.asScala) {
            val ppmOpt: Option[Int] = Try(record.value.toInt).toOption
            ppmOpt.foreach(ppm => {
              // Update initial state
              if (lessThanThreshold.isEmpty) {
                if (ppm < ppmThreshold) {
                  sendToTopic("no")
                  lessThanThreshold = Some(true)
                } else {
                  sendToTopic("yes")
                  lessThanThreshold = Some(false)
                }
              } else {
                // If the previous value was above the threshold and the current value is below, send "no" message
                if (ppm < ppmThreshold && !lessThanThreshold.get) {
                  sendToTopic("no")
                  lessThanThreshold = Some(true)
                } else if (ppm >= ppmThreshold && lessThanThreshold.get) {
                  // If the previous value was below the threshold and the current value is above, send "yes" message
                  sendToTopic("yes")
                  lessThanThreshold = Some(false)
                }
                logger.info(s"Measurement CO2: $ppm ppm")
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
