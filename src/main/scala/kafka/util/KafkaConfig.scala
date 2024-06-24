package kafka.util

import com.typesafe.config.ConfigFactory

case class KafkaBootstrap(
    address: String,
    port: Int
)

case class TopicInfo(
    consumerTopic: String,
    producerTopic: String
)

case class Topics(
  co2ThresholdDetector: TopicInfo,
  rollingAvgTemperature: TopicInfo
)

case class KafkaConfig(
    bootstrap: KafkaBootstrap,
    topics: Topics
)

object KafkaConfig {
  def load(): KafkaConfig = {
    val conf = ConfigFactory.load()
    val kafkaConf = conf.getConfig("kafka")
    KafkaConfig(
      bootstrap = KafkaBootstrap(
        kafkaConf.getString("bootstrap.address"),
        kafkaConf.getInt("bootstrap.port")
      ),
      topics = Topics(
        co2ThresholdDetector = TopicInfo(
          kafkaConf.getString("topics.co2ThresholdDetector.consumerTopic"),
          kafkaConf.getString("topics.co2ThresholdDetector.producerTopic")
        ),
        rollingAvgTemperature = TopicInfo(
          kafkaConf.getString("topics.rollingAvgTemperature.consumerTopic"),
          kafkaConf.getString("topics.rollingAvgTemperature.producerTopic")
        )
      )
    )
  }
}
