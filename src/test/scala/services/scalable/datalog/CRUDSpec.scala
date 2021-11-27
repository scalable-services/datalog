package services.scalable.datalog

import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.datalog.grpc.{Datom, FileDB}
import services.scalable.index.DefaultComparators.ord
import services.scalable.index.DefaultSerializers._
import services.scalable.index.impl.{DefaultCache, GrpcByteSerializer}
import services.scalable.index.{Bytes, Serializer}

import java.io.FileInputStream
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class CRUDSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  def printDatom(d: Datom, p: String): String = {
    p match {
      case "users/:color" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t},${d.op}]"
      case "users/:movie" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t},${d.op}]"
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

    var datoms = Seq.empty[(Datom, Bytes)]

    val id = UUID.randomUUID().toString

    var now = System.currentTimeMillis()

    datoms :++= Seq(
      Datom(
        e = Some(id),
        a = Some("users/:color"),
        v = Some(ByteString.copyFrom("blue".getBytes())),
        t = Some(now),
        op = Some(true)
      ) -> Array.empty[Byte],

      Datom(
        e = Some(id),
        a = Some("users/:movie"),
        v = Some(ByteString.copyFrom("Titanic".getBytes())),
        t = Some(now),
        op = Some(true)
      ) -> Array.empty[Byte]
    )

    /*result = Await.result(db.insert(datoms), Duration.Inf)
    result = Await.result(db.save(), Duration.Inf)

    logger.debug(s"insertion: ${result}")*/

    val termOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        var r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

        if(r != 0) return r

        ord.compare(x.getA.getBytes(), y.getA.getBytes())
      }
    }

    def find(a: String, id: String): Option[Datom] = {
      val it = db.eavtIndex.find(Datom(e = Some(id), a = Some(a)), false, termOrd)
      Await.result(TestHelper.one(it), Duration.Inf).map(_._1)
    }

    val idata = Await.result(TestHelper.all(db.eavtIndex.inOrder()(db.eavtOrdering)), Duration.Inf)
      .map{case (d, _) => printDatom(d, d.getA)}

    logger.debug(s"${Console.MAGENTA_B}idata: ${idata}${Console.RESET}\n")

    var one: Option[Datom] = find("users/:color", "b4e5c122-2dd1-4cae-a329-22f4786174ca")

    logger.debug(s"${Console.BLUE_B}one: ${one.map{d => printDatom(d, d.getA)}}${Console.RESET}")

    /*if(one.isDefined){
      result = Await.result(db.remove(Seq(one.get)), Duration.Inf)
      logger.debug(s"${Console.MAGENTA_B}deletion: ${result}${Console.RESET}\n")

      datoms = Seq(
        Datom(
          e = Some(one.get.getE),
          a = Some("users/:color"),
          v = Some(ByteString.copyFrom("red".getBytes())),
          t = Some(System.currentTimeMillis()),
          op = Some(true)
        ) -> Array.empty[Byte]
      )

      Await.result(db.insert(datoms), Duration.Inf)

      Await.result(db.save(), Duration.Inf)
    }*/

    Await.result(db.update(Seq(
      Datom(
        e = Some("c500032d-32c4-49e3-b759-cf345e9625b0"),
        a = Some("users/:color"),
        v = Some(ByteString.copyFrom("red".getBytes())),
        t = Some(System.currentTimeMillis()),
        op = Some(true)
      ) -> Array.empty[Byte]
    )), Duration.Inf)

    Await.result(db.save(), Duration.Inf)

    val data = Await.result(TestHelper.all(db.vaetIndex.inOrder()(db.vaetOrdering)), Duration.Inf)
    logger.debug(s"\n${Console.GREEN_B}data: ${data.map{case (k, v) => printDatom(k, k.getA)}}${Console.RESET}\n")

  }

}
