package kafka

import com.typesafe.config.ConfigFactory

case class KafkaBootstrap(
    address: String,
    port: Int
)

case class KafkaConfig(
    server: KafkaBootstrap
)

object KafkaConfig {
  def load(): KafkaConfig = {
    val conf = ConfigFactory.load()
    val kafkaConf = conf.getConfig("kafka")
    KafkaConfig(
      server = KafkaBootstrap(
        kafkaConf.getString("bootstrap.address"),
        kafkaConf.getInt("bootstrap.port")
      )
    )
  }
}
