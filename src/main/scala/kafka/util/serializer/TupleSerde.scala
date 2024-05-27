package kafka.util.serializer

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import java.nio.ByteBuffer

// TODO: Generics
class TupleSerializer extends Serializer[(Double, Int)] {
  override def serialize(topic: String, data: (Double, Int)): Array[Byte] = {
    if (data == null) return null
    val byteBuffer = ByteBuffer.allocate(12)
    byteBuffer.putDouble(data._1)
    byteBuffer.putInt(data._2)
    byteBuffer.array()
  }
}

class TupleDeserializer extends Deserializer[(Double, Int)] {
  override def deserialize(topic: String, data: Array[Byte]): (Double, Int) = {
    if (data == null) return null
    val byteBuffer = ByteBuffer.wrap(data)
    val first = byteBuffer.getDouble()
    val second = byteBuffer.getInt()
    (first, second)
  }
}

class TupleSerde extends Serde[(Double, Int)] {
  override def serializer(): Serializer[(Double, Int)] = new TupleSerializer
  override def deserializer(): Deserializer[(Double, Int)] = new TupleDeserializer
}
