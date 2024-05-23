package kafka.util

import com.typesafe.config.ConfigFactory

case class KafkaBootstrap(
    address: String,
    port: Int
)

case class KafkaConfig(
    bootstrap: KafkaBootstrap
)

object KafkaConfig {
  def load(): KafkaConfig = {
    val conf = ConfigFactory.load()
    val kafkaConf = conf.getConfig("kafka")
    KafkaConfig(
      bootstrap = KafkaBootstrap(
        kafkaConf.getString("bootstrap.address"),
        kafkaConf.getInt("bootstrap.port")
      )
    )
  }
}
