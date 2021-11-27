package services.scalable.datalog

import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.datalog.grpc.Datom
import services.scalable.index.{Bytes, QueryableIndex}
import services.scalable.index.DefaultComparators.ord
import services.scalable.index.impl.{DefaultCache, DefaultContext, MemoryStorage}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class CRUDSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  "" should "" in {

    def printDatom(d: Datom, p: String): String = {
      p match {
        case "users/:color" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t},${d.op}]"
        case "users/:movie" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t},${d.op}]"
        case _ => ""
      }
    }

    implicit def xf(k: Bytes): String = new String(k)

    implicit val eavtOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        var r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

        if(r != 0) return r

        r = ord.compare(x.getA.getBytes(), y.getA.getBytes())

        if(r != 0) return r

        r = ord.compare(x.getV.toByteArray, y.getV.toByteArray)

        if(r != 0) return r

        r = x.getT.compareTo(y.getT)

        if(r != 0) return r

        x.getOp.compareTo(y.getOp)
      }
    }

    val NUM_LEAF_ENTRIES = 5
    val NUM_META_ENTRIES = 5

    val indexId = "test_index"

    implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new CassandraStorage[Bytes, Bytes](TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)
    implicit val storage = new MemoryStorage[Datom, Bytes](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    implicit val ctx = new DefaultContext[Datom, Bytes](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    val prefixOrd = new Ordering[Datom] {
      override def compare(k: Datom, prefix: Datom): Int = {
        ord.compare(k.getA.getBytes(), prefix.getA.getBytes())
      }
    }

    val termOrd = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        var r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

        if(r != 0) return r

        r = ord.compare(x.getA.getBytes(), y.getA.getBytes())

        /*if(r != 0) return r

        ord.compare(x.getV.toByteArray, y.getV.toByteArray)*/

        /*if(r != 0) return r

        r = x.getT.compareTo(y.getT)

        if(r != 0) return r

        x.getOp.compareTo(y.getOp)*/

        r
      }
    }

    val index = new QueryableIndex[Datom, Bytes]()

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

    Await.result(index.insert(datoms), Duration.Inf)
    Await.result(ctx.save(), Duration.Inf)

    var idata = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"\n${Console.GREEN_B}data: ${idata.map{case (k, v) => printDatom(k, k.getA)}}${Console.RESET}\n")

    def find(a: String, now: Long): Option[Datom] = {
      var it = index.find(Datom(e = Some(id), a = Some(a), op = Some(true), t = Some(now)), false, termOrd)
      Await.result(TestHelper.one(it), Duration.Inf).map(_._1)
    }

    // Updating
    var one: Option[Datom] = find("users/:color", now)

    Await.result(index.remove(Seq(one.get)), Duration.Inf)
    Await.result(ctx.save(), Duration.Inf)

    now = System.currentTimeMillis()

    datoms = Seq(
      Datom(
        e = Some(id),
        a = Some("users/:color"),
        v = Some(ByteString.copyFrom("red".getBytes())),
        t = Some(System.currentTimeMillis()),
        op = Some(true)
      ) -> Array.empty[Byte]
    )

    Await.result(index.insert(datoms), Duration.Inf)
    Await.result(ctx.save(), Duration.Inf)

    idata = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"\n${Console.GREEN_B}data: ${idata.map{case (k, v) => printDatom(k, k.getA)}}${Console.RESET}\n")

    one = find("users/:color", now)
    logger.debug(s"${Console.MAGENTA_B}color: ${one.map(d => printDatom(d, d.getA))}${Console.RESET}")

    one = find("users/:movie", now)
    logger.debug(s"${Console.BLUE_B}movie: ${one.map(d => printDatom(d, d.getA))}${Console.RESET}")

  }

}
