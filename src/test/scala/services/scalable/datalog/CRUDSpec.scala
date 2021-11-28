package services.scalable.datalog

import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.datalog.grpc.{Datom, FileDB}
import services.scalable.index.DefaultComparators.ord
import services.scalable.index.DefaultSerializers._
import services.scalable.datalog.DefaultDatalogSerializers._
import services.scalable.index.impl.{DefaultCache, GrpcByteSerializer}
import services.scalable.index.{Bytes, Serializer}

import java.io.FileInputStream
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class CRUDSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  def printDatom(d: Datom, p: String): String = {
    p match {
      case "users/:color" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t},${d.op}]"
      case "users/:movie" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t},${d.op}]"
      case "users/:balance" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t},${d.op}]"
      case _ => ""
    }
  }

  "index data " must "be equal to test data" in {

    val rand = ThreadLocalRandom.current()

    val NUM_LEAF_ENTRIES = 64
    val NUM_META_ENTRIES = 64

    val EMPTY_ARRAY = Array.empty[Byte]

    implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new CQLStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    val db = new DatomDatabase("crud-db", "indexes", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(global, grpcBlockSerializer, cache, storage)

    var result = Await.result(db.loadOrCreate(), Duration.Inf)

    logger.debug(s"load or create: ${result} ${db.getContexts()}")

    var datoms = Seq.empty[(Datom, Bytes)]

    val id = UUID.randomUUID().toString

    var now = System.currentTimeMillis()
    val balance = java.nio.ByteBuffer.allocate(4).putInt(rand.nextInt(0, 1000)).flip().array()

    datoms :++= Seq(
      Datom(
        e = Some(id),
        a = Some("users/:color"),
        v = Some(ByteString.copyFrom("blue".getBytes())),
        t = Some(now),
        op = Some(true)
      ) -> EMPTY_ARRAY,

      Datom(
        e = Some(id),
        a = Some("users/:movie"),
        v = Some(ByteString.copyFrom("Titanic".getBytes())),
        t = Some(now),
        op = Some(true)
      ) -> EMPTY_ARRAY,

      Datom(
        e = Some(id),
        a = Some("users/:balance"),
        v = Some(ByteString.copyFrom(balance)),
        t = Some(now),
        op = Some(true)
      ) -> EMPTY_ARRAY
    )

    /*datoms = Seq(
      Datom(
        e = Some("b4e5c122-2dd1-4cae-a329-22f4786174ca"),
        a = Some("users/:balance"),
        v = Some(ByteString.copyFrom(balance)),
        t = Some(now),
        op = Some(true)
      ) -> Array.empty[Byte]
    )*/

    /*result = Await.result(db.insert(datoms), Duration.Inf)
    result = Await.result(db.save(), Duration.Inf)*/

    logger.debug(s"insertion: ${result}")

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

    /*Await.result(db.update(Seq(
      Datom(
        e = Some("c500032d-32c4-49e3-b759-cf345e9625b0"),
        a = Some("users/:movie"),
        v = Some(ByteString.copyFrom("Jurassic Park".getBytes())),
        t = Some(System.currentTimeMillis()),
        op = Some(true)
      ) -> Array.empty[Byte]
    )), Duration.Inf)

    Await.result(db.save(), Duration.Inf)*/

    one = find("users/:balance", "b4e5c122-2dd1-4cae-a329-22f4786174ca")

    println(s"${Console.BLUE_B}balance: ${one.map{d => printDatom(d, d.getA)}}${Console.RESET}")

    val data = Await.result(TestHelper.all(db.eavtIndex.inOrder()(db.eavtOrdering)), Duration.Inf)
    logger.debug(s"\n${Console.GREEN_B}data: ${data.map{case (k, v) => printDatom(k, k.getA)}}${Console.RESET}\n")

  }

}
