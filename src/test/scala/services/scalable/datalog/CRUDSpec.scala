package services.scalable.datalog

import com.google.protobuf.any.Any
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.datalog.grpc.{Datom, FileDB}
import services.scalable.index.DefaultSerializers._
import services.scalable.index.impl.{DefaultCache, GrpcByteSerializer}
import services.scalable.index.{Bytes, Serializer}

import java.io.FileInputStream
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class CRUDSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  def printDatom(d: Datom, p: String): String = {
    p match {
      case "users/:color" => s"${Console.GREEN_B}[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]${Console.RESET}"
      case _ => ""
    }
  }

  "index data " must "be equal to test data" in {

    val NUM_LEAF_ENTRIES = 64
    val NUM_META_ENTRIES = 64

    val EMPTY_ARRAY = Array.empty[Byte]

    implicit val serializer = new Serializer[Datom] {
      override def serialize(t: Datom): Bytes = Any.pack(t).toByteArray
      override def deserialize(b: Bytes): Datom = Any.parseFrom(b).unpack(Datom)
    }

    implicit val grpcBlockSerializer = new GrpcByteSerializer[Datom, Bytes]()

    implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new CQLStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    val db = new DatomDatabase("crud-db", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(global, grpcBlockSerializer, cache, storage)

    var result = Await.result(db.loadOrCreate(), Duration.Inf)

    logger.debug(s"load or create: ${result} ${db.getContexts()}")

    /*result = Await.result(db.insert(datoms.map(_ -> EMPTY_ARRAY)), Duration.Inf)
    result = Await.result(db.save(), Duration.Inf)

    logger.debug(s"insertion: ${result}")*/

    val idata = Await.result(TestHelper.all(db.aevtIndex.inOrder()(db.aevtOrdering)), Duration.Inf)
      .map{case (d, _) => printDatom(d, d.getA)}

    logger.debug(s"${Console.MAGENTA_B}idata: ${idata}${Console.RESET}\n")
  }

}
