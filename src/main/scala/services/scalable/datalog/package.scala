package services.scalable

import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import services.scalable.datalog.grpc.Datom
import services.scalable.index.{Bytes, Serializer}
import services.scalable.index.impl.GrpcByteSerializer

package object datalog {

  object DefaultDatalogSerializers {

    import services.scalable.index.DefaultSerializers._

    implicit val serializer = new Serializer[Datom] {
      override def serialize(t: Datom): Bytes = Any.pack(t).toByteArray
      override def deserialize(b: Bytes): Datom = Any.parseFrom(b).unpack(Datom)
    }

    implicit val grpcBlockSerializer = new GrpcByteSerializer[Datom, Bytes]()

  }

  object Helper {

    def readString(b: Array[Byte]): String = new String(b, Charsets.UTF_8)
    def readInt(b: Array[Byte]): Int = java.nio.ByteBuffer.allocate(4).put(b).flip().getInt()
    def readLong(b: Array[Byte]): Long = java.nio.ByteBuffer.allocate(8).put(b).flip().getLong()
    def readShort(b: Array[Byte]): Short = java.nio.ByteBuffer.allocate(2).put(b).flip().getShort()
    def readDouble(b: Array[Byte]): Double = java.nio.ByteBuffer.allocate(8).put(b).flip().getDouble()
    def readFloat(b: Array[Byte]): Float = java.nio.ByteBuffer.allocate(4).put(b).flip().getFloat()
    def readByte(b: Array[Byte]): Byte = java.nio.ByteBuffer.allocate(1).put(b).flip().get()
    def readChar(b: Array[Byte]): Char = java.nio.ByteBuffer.allocate(1).put(b).flip().getChar()

    def write(v: String): Array[Byte] = v.getBytes(Charsets.UTF_8)
    def write(v: Int): Array[Byte] = java.nio.ByteBuffer.allocate(4).putInt(v).flip().array()
    def write(v: Long): Array[Byte] = java.nio.ByteBuffer.allocate(8).putLong(v).flip().array()
    def write(v: Double): Array[Byte] = java.nio.ByteBuffer.allocate(8).putDouble(v).flip().array()
    def write(v: Float): Array[Byte] = java.nio.ByteBuffer.allocate(4).putFloat(v).flip().array()
    def write(v: Short): Array[Byte] = java.nio.ByteBuffer.allocate(2).putShort(v).flip().array()
    def write(v: Byte): Array[Byte] = java.nio.ByteBuffer.allocate(1).putShort(v).flip().array()
    def write(v: Char): Array[Byte] = java.nio.ByteBuffer.allocate(1).putChar(v).flip().array()

  }

  object Implicits {
    implicit def byteToByteString(b: Array[Byte]): ByteString = ByteString.copyFrom(b)
  }

}
