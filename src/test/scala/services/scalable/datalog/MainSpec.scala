package services.scalable.datalog

import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, FileInputStream}
import java.util.concurrent.ThreadLocalRandom
import scala.language.postfixOps
import com.google.protobuf.any.Any
import services.scalable.datalog.grpc.{Datom, FileDB}
import services.scalable.index.{Bytes, RichAsyncIterator}
import services.scalable.index.DefaultComparators.ord

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class MainSpec extends AnyFlatSpec with Repeatable {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times: Int = 1

  val rand = ThreadLocalRandom.current()

  "index data " must "be equal to test data" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    def printDatom(d: Datom, p: String): String = {
      p match {
        case "users/:tweet" => s"${Console.GREEN_B}[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]${Console.RESET}"
        case "users/:tweetedBy" => s"${Console.RED_B}[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]${Console.RESET}"
        case "users/:username" => s"${Console.MAGENTA_B}[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]${Console.RESET}"
        case "users/:email" => s"[${Console.CYAN_B}${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]${Console.RESET}"
        case "users/:likes" => s"${Console.YELLOW_B}[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]${Console.RESET}"
        case "users/:follows" => s"${Console.BLUE_B}[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]${Console.RESET}"
        case "users/:age" => s"${Console.CYAN_B}[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]${Console.RESET}"
        case _ => ""
      }
    }

    val bytes = new FileInputStream("twitter.db").readAllBytes()
    val datoms = Any.parseFrom(bytes).unpack(FileDB).datoms

    val db = new DatomDatabase("twitter-db", 64, 64)

    //logger.debug(s"idata: ${datoms.map{d => printDatom(d, d.getA)}}\n")

    val EMPTY_ARRAY = Array.empty[Byte]
    Await.result(db.insert(datoms.map{_ -> EMPTY_ARRAY}), Duration.Inf)

    val list = Await.result(TestHelper.all(db.avetIndex.inOrder()(db.avetOrdering)), Duration.Inf)
    logger.debug(s"avet: ${list.map{case (k, v) => printDatom(k, k.getA)}}")

    val prefixOrd = new Ordering[Datom] {
      override def compare(k: Datom, prefix: Datom): Int = {
        ord.compare(k.getA.getBytes(), prefix.getA.getBytes())
      }
    }

    val aevtOrdering = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        val r = ord.compare(x.getA.getBytes(), y.getA.getBytes())

        if(r != 0) return r

        ord.compare(x.getE.getBytes(), y.getE.getBytes())
      }
    }

    val avetOrdering = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        val r = ord.compare(x.getA.getBytes(), y.getA.getBytes())

        if(r != 0) return r

        ord.compare(x.getV.toByteArray, y.getV.toByteArray)
      }
    }

    val eavtOrdering = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {
        var r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

        if(r != 0) return r

        ord.compare(x.getA.getBytes(), y.getA.getBytes())
      }
    }

    var it: RichAsyncIterator[Datom, Bytes] = null

    def find30(followeeId: String): Future[Option[(String, Int)]] = {
      TestHelper.one(db.eavtIndex.find(Datom(e = Some(followeeId), a = Some("users/:age")), false, eavtOrdering))
        .map {
          case None => None
          case Some((d, _)) =>

            val age = java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()

            logger.info(s"age: ${age} followee: ${followeeId}")

            if(age >= 30){
              Some(followeeId -> age)
            } else {
              None
            }
        }
    }

    def findFollowersAgeGteq30(it: RichAsyncIterator[Datom, Bytes]): Future[Seq[(String, Int)]] = {
      it.hasNext().flatMap {
        case true => it.next().flatMap { list =>
          Future.sequence(list.map{ case (f, _) =>
            find30(new String(f.getV.toByteArray))
          }).map(_.filter(_.isDefined).map(_.get))
        }
        case false => Future.successful(Seq.empty[(String, Int)])
      }
    }

    val userId = Await.result(TestHelper.one(db.avetIndex.find(Datom(a = Some("users/:username"),
      v = Some(ByteString.copyFrom("user-87".getBytes()))), false, avetOrdering)), Duration.Inf).map(_._1.getE).get

    /*val followers = Await.result(TestHelper.all(db.aevtIndex.find(Datom(a = Some("users/:follows"), e = Some(userId)),
      false, aevtOrdering)), Duration.Inf)*/

    val f = findFollowersAgeGteq30(db.aevtIndex.find(Datom(a = Some("users/:follows"), e = Some(userId)),
      false, aevtOrdering))

    val result = Await.result(f, Duration.Inf)

    logger.debug(s"\n\nfollowers: ${result}${Console.RESET}\n\n")

    //logger.debug(s"\n\nfollowers: ${followers.map{case (d, _) => printDatom(d, d.getA)}}${Console.RESET}\n\n")
}

}