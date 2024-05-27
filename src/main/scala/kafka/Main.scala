package kafka

import kafka.co2.CO2ThresholdDetector
import kafka.temperature.RollingAvgTemperature
import kafka.util.KafkaConfig
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {
  val conf: KafkaConfig = KafkaConfig.load()
  val broker = s"${conf.bootstrap.address}:${conf.bootstrap.port}"
  private val co2ThresholdDetector = new CO2ThresholdDetector(broker)
  private val rollingAvgTemperature = new RollingAvgTemperature(broker)
  Future {
    co2ThresholdDetector.exec()
  }
  Future {
    rollingAvgTemperature.exec()
  }

  while (true) {
    Thread.sleep(1000)
  }
}
