package kafka

import kafka.co2.CO2ThresholdDetector
import kafka.util.KafkaConfig

object Main extends App {
  val conf: KafkaConfig = KafkaConfig.load()
  val broker = s"${ conf.bootstrap.address }:${ conf.bootstrap.port }"
  private val co2ThresholdDetector = new CO2ThresholdDetector(broker)
  co2ThresholdDetector.exec()
}
