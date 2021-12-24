package services.scalable.datalog

import com.datastax.oss.driver.api.core.CqlSession
import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.datalog.DefaultDatalogSerializers.grpcBlockSerializer
import services.scalable.datalog.grpc.Datom
import services.scalable.index.{Bytes, QueryableIndex, RichAsyncIterator, loader}
import services.scalable.index.impl.{DefaultCache, MemoryStorage}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import services.scalable.datalog.Helper
import services.scalable.datalog.Implicits._
import services.scalable.index.DefaultComparators.ord

import scala.concurrent.duration.Duration

class QueryEngineSpec extends AnyFlatSpec {

  def printd(d: Datom, p: String): String = {
    p match {
      case "movies/:title" => s"[${d.a},${Helper.readString(d.getV.toByteArray)},${d.e},${d.tx},${d.tmp}]"
      case "movies/:year" => s"[${d.a},${Helper.readInt(d.getV.toByteArray)},${d.e},${d.tx},${d.tmp}]"
      case "movies/:genre" => s"[${d.a},${Helper.readString(d.getV.toByteArray)},${d.e},${d.tx},${d.tmp}]"

      case "actors/:name" => s"[${d.a},${Helper.readString(d.getV.toByteArray)},${d.e},${d.tx},${d.tmp}]"
      case "actors/:birth" => s"[${d.a},${Helper.readInt(d.getV.toByteArray)},${d.e},${d.tx},${d.tmp}]"
      case "actors/:gender" => s"[${d.a},${Helper.readString(d.getV.toByteArray)},${d.e},${d.tx},${d.tmp}]"

      case "actors/:played" => s"[${d.a},${Helper.readString(d.getV.toByteArray)},${d.e},${d.tx},${d.tmp}]"

      case _ => ""
    }
  }

  case class Movie(title: String, year: Int, genre: String) {
    val id = UUID.randomUUID().toString

    def toDatom(tx: String): Seq[Datom] = {

      val now = System.currentTimeMillis()

      Seq(
        Datom(
          a = Some("movies/:title"),
          v = Some(Helper.write(title)),
          e = Some(id),
          tx = Some(tx),
          tmp = Some(now)
        ),

        Datom(
          a = Some("movies/:year"),
          v = Some(Helper.write(year)),
          e = Some(id),
          tx = Some(tx),
          tmp = Some(now)
        ),

        Datom(
          a = Some("movies/:genre"),
          v = Some(Helper.write(genre)),
          e = Some(id),
          tx = Some(tx),
          tmp = Some(now)
        )
      )
    }
  }

  case class Actor(name: String, yearOfBirth: Int, gender: String) {
    val id = UUID.randomUUID().toString

    def toDatom(tx: String): Seq[Datom] = {

      val now = System.currentTimeMillis()

      Seq(
        Datom(
          a = Some("actors/:name"),
          v = Some(Helper.write(name)),
          e = Some(id),
          tx = Some(tx),
          tmp = Some(now)
        ),

        Datom(
          a = Some("actors/:birth"),
          v = Some(Helper.write(yearOfBirth)),
          e = Some(id),
          tx = Some(tx),
          tmp = Some(now)
        ),

        Datom(
          a = Some("actors/:gender"),
          v = Some(Helper.write(gender)),
          e = Some(id),
          tx = Some(tx),
          tmp = Some(now)
        )
      )
    }
  }

  case class PlayedAt(actorId: String, movieId: String) {
    def toDatom(tx: String): Seq[Datom] = {
      val now = System.currentTimeMillis()

      Seq(
        Datom(
          a = Some("actors/:played"),
          v = Some(Helper.write(movieId)),
          e = Some(actorId),
          tx = Some(tx),
          tmp = Some(now)
        )
      )
    }
  }

  trait Position
  case class Var(name: String) extends Position
  case class Val(v: ByteString) extends Position

  case class Where(clauses: c*)
  case class c(e: Option[Position] = None, a: Option[Position] = None, v: Option[Position] = None)

  case class Query(find: Seq[Var], where: Where)

  object Implicits {

    //implicit def bytesToByteString(b: Array[Byte]): ByteString = ByteString.copyFrom(b)

    implicit def write(v: String): ByteString = v.getBytes(Charsets.UTF_8)
    implicit def write(v: Int): ByteString = java.nio.ByteBuffer.allocate(4).putInt(v).flip().array()
    implicit def write(v: Long): ByteString = java.nio.ByteBuffer.allocate(8).putLong(v).flip().array()
    implicit def write(v: Double): ByteString = java.nio.ByteBuffer.allocate(8).putDouble(v).flip().array()
    implicit def write(v: Float): ByteString = java.nio.ByteBuffer.allocate(4).putFloat(v).flip().array()
    implicit def write(v: Short): ByteString = java.nio.ByteBuffer.allocate(2).putShort(v).flip().array()
    implicit def write(v: Byte): ByteString = java.nio.ByteBuffer.allocate(1).putShort(v).flip().array()
    implicit def write(v: Char): ByteString = java.nio.ByteBuffer.allocate(1).putChar(v).flip().array()

    /*implicit def varToOptionString(v: Var): Option[String] = Some(v.name)
    implicit def varToOptionByteString(v :Var): Option[ByteString] = Some(write(v.name))*/

    def ?(name: String) = Var(name)

    implicit def askToSomeVar(v: Var): Option[Position] = Some(v)
    implicit def askToSomeVal(v: Val): Option[Position] = Some(v)

    implicit def someToVal(s: String): Val = Val(s)
    implicit def someToVal(s: Int): Val = Val(s)
  }

  val avetTermFinder = new Ordering[Datom] {
    override def compare(x: Datom, y: Datom): Int = {
      val r = ord.compare(x.getA.getBytes(Charsets.UTF_8), y.getA.getBytes(Charsets.UTF_8))

      if(r != 0) return r

      ord.compare(x.getV.toByteArray, y.getV.toByteArray)
    }
  }

  class QueryEngine(val q: Query, db: DatomDatabase) {

    val vars = TrieMap.empty[String, Var]

    def selectIndex(cl: c): String = {
      val n = Seq(cl.e.isDefined && cl.e.get.isInstanceOf[Val], cl.a.isDefined && cl.a.get.isInstanceOf[Val],
        cl.v.isDefined && cl.v.get.isInstanceOf[Val]).count(_ == true)

      if(n == 0 || n == 3){
        return "eavt"
      }

      if(n == 1){

        if(cl.e.isDefined && cl.e.get.isInstanceOf[Val]){
          return "eavt"
        }

        if(cl.a.isDefined && cl.a.get.isInstanceOf[Val]){
          return "aevt"
        }

        return "vaet"
      }

      logger.info(s"n: ${n} ${cl.e} ${cl.a} ${cl.v}")

      if(cl.e.isDefined && cl.e.get.isInstanceOf[Val] && cl.a.isDefined && cl.a.get.isInstanceOf[Val]){
        return "eavt"
      }

      if(cl.a.isDefined && cl.a.get.isInstanceOf[Val] && cl.v.isDefined && cl.v.get.isInstanceOf[Val]){
        return "avet"
      }

      // VE ? NO SUCH CASE... AVE
      "aevt"
    }

    q.where.clauses.foreach { c =>
      val idx = selectIndex(c)

      logger.info(s"\nindex: ${idx} \n")

      idx match {
        case "aevt" =>
        case "eavt" =>
        case "avet" =>

          val data = Await.result(TestHelper.all(db.avetIndex.find(Datom(
            a = Some(c.a.get.asInstanceOf[Val].v.toStringUtf8),
            v = Some(c.v.get.asInstanceOf[Val].v)), false, avetTermFinder)), Duration.Inf).map(_._1)

          logger.debug(s"\n${Console.GREEN_B}data: ${data.map{k => printd(k, k.getA)}}${Console.RESET}\n")

        case "vaet" =>
        case _ => throw new RuntimeException("no index!")
      }

      //logger.info(s"c: $c index: ${idx}\n")

    }

  }

  val logger = LoggerFactory.getLogger(this.getClass)

  "" should "" in {

    import Implicits._

    val session = CqlSession
      .builder()
      .withConfigLoader(loader)
      .withKeyspace("movies")
      .build()

    val NUM_LEAF_ENTRIES = 64
    val NUM_META_ENTRIES = 64

    val EMPTY_ARRAY = Array.empty[Byte]

    implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new CQLStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES, session)
    val db = new DatomDatabase("movie-db", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(global, session, grpcBlockSerializer, cache, storage)

    var result = Await.result(db.loadOrCreate(), Duration.Inf)

    logger.debug(s"load or create: ${result} ${db.getContexts()}")

   /* val data = Await.result(TestHelper.all(db.eavtIndex.inOrder()(db.eavtOrdering)), Duration.Inf)
    logger.debug(s"\n${Console.GREEN_B}data: ${data.map{case (k, v) => printd(k, k.getA)}}${Console.RESET}\n")*/

   /* // Get all actors who played Titanic...
    val where = Query(
      Seq(?("actor")),
      Where(
        c(e = ?("m"), a = Some(s"movies/:title"), v = Some("Titanic")),
        c(e = ?("a"), a = Some(s"actors/:played"), v = ?("m")),
        c(e = ?("a"), a = Some(s"actors/:name"), v = ?("actor"))
      )
    )

    val engine = new QueryEngine(where, db)

    logger.info(s"where: ${where}")*/

    def getAllActorsOlderThan(age: Int): Future[Seq[String]] = {

      def getActorName(id: String): Future[String] = {
        db.findOne(Datom(e = Some(id), a = Some("actors/:name"))).map(_.head.getV.toStringUtf8)
      }

      def next(it: RichAsyncIterator[Datom, Bytes]): Future[Seq[String]] = {
        it.hasNext().flatMap {
          case false => Future.successful(Seq.empty[String])
          case true => it.next().flatMap { list =>
            Future.sequence(list.map{case (d, _) => getActorName(d.getE)}).flatMap { names =>
              next(it).map{names ++ _}
            }
          }
        }
      }

      val it = db.lt(Datom(a = Some("actors/:birth"), v = Some(1997 - age)), true, false)

      next(it)
    }

    def getTitanicActors(): Future[Seq[String]] = {

      def getActorName(id: String): Future[String] = {
        db.findOne(Datom(e = Some(id), a = Some("actors/:name"))).map(_.head.getV.toStringUtf8)
      }

      def next(it: RichAsyncIterator[Datom, Bytes]): Future[Seq[String]] = {
        it.hasNext().flatMap {
          case false => Future.successful(Seq.empty[String])
          case true => it.next().flatMap { list =>
            Future.sequence(list.map{case (d, _) => getActorName(d.getE)}).flatMap { names =>
              next(it).map{names ++ _}
            }
          }
        }
      }

      db.findOne(Datom(a = Some("movies/:title"), v = Some("Titanic"))).flatMap {
        case None => Future.successful(Seq.empty[String])
        case Some(m) => next(db.findMany(Datom(a = Some("actors/:played"), v = Some(m.getE))))
      }
    }

    val data = Await.result(getAllActorsOlderThan(30), Duration.Inf)
    logger.debug(s"\n${Console.GREEN_B}data: ${data}${Console.RESET}\n")

  }

}
