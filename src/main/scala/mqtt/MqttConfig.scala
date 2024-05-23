package mqtt

//import com.typesafe.config.ConfigFactory

//case class MqttConfig(
//    protocol: String,
//    address: String,
//    port: Int
//)
//
//object MqttConfig {
//  def load(): MqttConfig = {
//    val config = ConfigFactory.load()
//    val mqtt = config.getConfig("mqtt")
//    MqttConfig(
//      protocol = mqtt.getString("protocol"),
//      address = mqtt.getString("address"),
//      port = mqtt.getInt("port")
//    )
//  }
//}
