package kafka

object Main extends App {
  val conf: KafkaConfig = KafkaConfig.load()
  val broker = s"${ conf.server.address }:${ conf.server.port }"
  private val warnCO2Producer = new WarnCO2Producer(broker)
  warnCO2Producer.exec()
}
