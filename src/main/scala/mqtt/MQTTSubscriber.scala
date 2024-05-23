package mqtt

//import org.eclipse.paho.mqttv5.client._
//import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence
//import org.eclipse.paho.mqttv5.common.{ MqttException, MqttMessage }
//import org.eclipse.paho.mqttv5.common.packet.MqttProperties
//import com.typesafe.config.ConfigFactory
//import java.util.concurrent.{ ArrayBlockingQueue, BlockingQueue }
//import scala.util.{ Failure, Success, Try }
//import logger.Logger

// NOTE: Experimental: This is a simple MQTT subscriber that subscribes to a topic and forwards the messages to another system.
//object MQTTSubscriber extends Logger {
//  private val mqttConfig = MqttConfig.load()
//  private val clientId = "sample"
//  private val brokerUrl = s"${ mqttConfig.protocol }://${ mqttConfig.address }:${ mqttConfig.port }"
//  private val persistence = new MemoryPersistence()
//  @volatile private var keepGoing = true
//  private val queue: BlockingQueue[(String, String)] = new ArrayBlockingQueue(1024)
//
//  def main(args: Array[String]): Unit = {
//    val mainApp = new Main
//    mainApp.start()
//  }
//
//  private class Main extends App with Logger with MqttCallback {
//
//    /** implement MqttCallback abstract methods */
//    override def messageArrived(topic: String, message: MqttMessage): Unit = {
//      logger.info("messageArrived called")
//      val payload = new String(message.getPayload)
//      logger.info(s"Message arrived. Topic: $topic, Message: $payload")
//      queue.put((topic, payload))
//      logger.info("Message added to queue")
//    }
//
//    override def authPacketArrived(reasonCode: Int, properties: MqttProperties): Unit = {
//      logger.info("Auth Packet Arrived")
//    }
//
//    override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
//      logger.info(s"Connect Complete: $serverURI")
//      // Reset keepGoing to true in case it was set to false
//      keepGoing = true
//    }
//
//    override def disconnected(response: MqttDisconnectResponse): Unit = {
//      logger.info("Disconnected")
//      keepGoing = false
//    }
//
//    override def deliveryComplete(token: IMqttToken): Unit = {
//      logger.info("Delivery Complete")
//    }
//
//    override def mqttErrorOccurred(exception: MqttException): Unit = {
//      logger.error(s"Error Occurred: ${ exception.getMessage }")
//    }
//
//    private class MessageProcessor extends Runnable {
//      def run(): Unit = {
//        while (keepGoing) {
//          try {
//            logger.info("Waiting for message in queue...")
//            val message = queue.take()
//            logger.info("Message taken from queue")
//            forwardMessage(message)
//          } catch {
//            case e: InterruptedException =>
//              logger.error(s"Interrupted: ${ e.getMessage }")
//              keepGoing = false
//          }
//        }
//      }
//
//      private def forwardMessage(message: (String, String)): Unit = {
//        logger.info(s"Forwarding message: Topic: ${ message._1 }, Message: ${ message._2 }")
//        // TODO: Here you would typically forward the message to Kafka or another system (ex. Produce Kafka Topic)
//      }
//    }
//
//    def start(): Unit = {
//      Try {
//        val client: MqttClient = new MqttClient(brokerUrl, clientId, persistence)
//        val connOpt: MqttConnectionOptions = new MqttConnectionOptions()
//        connOpt.setCleanStart(true)
//        connOpt.setAutomaticReconnect(true) // Enable automatic reconnect
//        logger.info("Connecting to broker: " + brokerUrl)
//        client.connect(connOpt)
//        client.setCallback(this)
//        logger.info("Subscribing to topic: i483/sensors/s2410014/# with QoS 1")
//        client.subscribe("i483/sensors/s2410014/#", 1) // Use QoS 1
//        logger.info("Connected and subscribed to topic with QoS 1")
//      } match {
//        case Success(_) =>
//          logger.info("Success")
//          val processor = new MessageProcessor()
//          val thread = new Thread(processor)
//          thread.start()
//          while (keepGoing) {
//            logger.info("Thread is alive")
//            try {
//              Thread.sleep(1000) // Wait for 1 second before checking again
//            } catch {
//              case ex: InterruptedException =>
//                logger.error(s"Failure: ${ ex.getMessage }")
//            }
//          }
//          logger.info("Shutting down")
//        case Failure(e) => logger.error(s"Failure: ${ e.getMessage }")
//      }
//    }
//  }
//}
