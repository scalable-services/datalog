package services.scalable

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

}
