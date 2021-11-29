package services.scalable.datalog

import com.datastax.oss.driver.api.core.CqlSession
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.datalog.DefaultDatalogSerializers.grpcBlockSerializer
import services.scalable.datalog.grpc.Datom
import services.scalable.index.impl.DefaultCache
import services.scalable.index.{Bytes, loader}

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import services.scalable.datalog.Helper
import services.scalable.datalog.Implicits._

import java.util.UUID

class MovieDatabaseSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

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

  "it" should "insert and query successfully" in {

    val session = CqlSession
      .builder()
      .withConfigLoader(loader)
      .withKeyspace("movies")
      .build()

    val rand = ThreadLocalRandom.current()

    val NUM_LEAF_ENTRIES = 64
    val NUM_META_ENTRIES = 64

    val EMPTY_ARRAY = Array.empty[Byte]

    implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new CQLStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    val db = new DatomDatabase("movie-db", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(global, session, grpcBlockSerializer, cache, storage)

    var result = Await.result(db.loadOrCreate(), Duration.Inf)

    logger.debug(s"load or create: ${result} ${db.getContexts()}")

    var movieIds = Map.empty[String, String]
    var actorIds = Map.empty[String, String]
    var directorIds = Map.empty[String, String]

    def insertMovies(tx: String): Future[Boolean] = {
      val movies = Seq(
        Movie("Titanic", 1997, "Drama"),
        Movie("Jurassic Park", 1993, "Fiction"),
        Movie("Alien", 1979, "Fiction"),
        Movie("Inception", 2010, "Action")
      )

      movies.foreach { m =>
        movieIds = movieIds + (m.title -> m.id)
      }

      db.insert(movies.map(_.toDatom(tx)).flatten.map(_ -> EMPTY_ARRAY))
    }

    def insertActors(tx: String): Future[Boolean] = {
      val actors = Seq(
        Actor("Leonardo DiCaprio", 1974, "male"),
        Actor("Kate Winslet", 1975, "female"),

        Actor("Richard Attenborough", 1923, "male"),
        Actor("Laura Dern", 1967, "female"),
        Actor("Sam Neill", 1947, "male"),

        Actor("Sigourney Weaver", 1949, "female"),
        Actor("Tom Skerritt", 1933, "male"),
        Actor("John Hurt", 1940, "male"),

        Actor("Joseph Gordon-Levitt", 1981, "male"),
        Actor("Elliot Page", 1987, "female")
      )

      actors.foreach { a =>
        actorIds = actorIds + (a.name -> a.id)
      }

      db.insert(actors.map(_.toDatom(tx)).flatten.map(_ -> EMPTY_ARRAY))
    }

    def insertPlays(tx: String): Future[Boolean] = {
      val plays = Seq(
        PlayedAt(actorIds("Leonardo DiCaprio"), movieIds("Titanic")),
        PlayedAt(actorIds("Kate Winslet"), movieIds("Titanic")),

        PlayedAt(actorIds("Richard Attenborough"), movieIds("Jurassic Park")),
        PlayedAt(actorIds("Laura Dern"), movieIds("Jurassic Park")),
        PlayedAt(actorIds("Sam Neill"), movieIds("Jurassic Park")),

        PlayedAt(actorIds("Sigourney Weaver"), movieIds("Alien")),
        PlayedAt(actorIds("Tom Skerritt"), movieIds("Alien")),
        PlayedAt(actorIds("John Hurt"), movieIds("Alien")),

        PlayedAt(actorIds("Leonardo DiCaprio"), movieIds("Inception")),
        PlayedAt(actorIds("Joseph Gordon-Levitt"), movieIds("Inception")),
        PlayedAt(actorIds("Elliot Page"), movieIds("Inception"))
      )

      db.insert(plays.map(_.toDatom(tx)).flatten.map(_ -> EMPTY_ARRAY))
    }

    val tx = UUID.randomUUID().toString

    /*val task = for {
      ok1 <- insertMovies(tx)
      /*ok2 <- insertActors(tx)
      ok3 <- insertPlays(tx)*/
    } yield {
      db.save()
    }

    val op = Await.result(task, Duration.Inf)

    logger.debug(s"\n${Console.MAGENTA_B}insertion: ${op}${Console.RESET}\n")*/

    val data = Await.result(TestHelper.all(db.eavtIndex.inOrder()(db.eavtOrdering)), Duration.Inf)
    logger.debug(s"\n${Console.GREEN_B}data: ${data.map{case (k, v) => printd(k, k.getA)}}${Console.RESET}\n")

    session.close()
  }
}
