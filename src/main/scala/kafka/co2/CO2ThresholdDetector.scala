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
  private val producerTopic = "i483-sensors-s2410014-co2_threshold-crossed"

  subscribe(List(consumerTopic))

  override def exec(): Unit = {
    val sendToTopic = producer(producerTopic) _
    val ppmThreshold: Int = 700
    Try {
      ultimately {
        // equal to finally
        consumerClose()
      } {
        // ここに変数を格納する(過去のステートを管理する)
        var lessThanThreshold: Option[Boolean] = None
        while (true) {
          val records: ConsumerRecords[String, String] = listConsumerRecords()
          for (record <- records.asScala) {
            val ppmOpt: Option[Int] = Try(record.value.toInt).toOption
            ppmOpt.foreach(ppm => {
              // 初期状態を更新
              if (lessThanThreshold.isEmpty) {
                if (ppm < ppmThreshold) {
                  sendToTopic("no")
                  lessThanThreshold = Some(true)
                } else {
                  sendToTopic("yes")
                  lessThanThreshold = Some(false)
                }
              } else {
                // 直前の処理で閾値を超えている場合かつ現在の値が閾値以下であれば、閾値未満であるメッセージ(no)を送信
                if (ppm < ppmThreshold && !lessThanThreshold.get) {
                  sendToTopic("no")
                  lessThanThreshold = Some(false)
                } else if (ppm >= ppmThreshold && lessThanThreshold.get) {
                  // 直前の処理で閾値を超えていない場合かつ現在の値が閾値以上であれば、閾値を超えているメッセージ(yes)を送信
                  sendToTopic("yes")
                  lessThanThreshold = Some(true)
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
