package services.scalable.datalog

import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.datalog.grpc.{Datom, FileDB}
import services.scalable.index.Bytes
import com.google.protobuf.any.Any

import java.io.{BufferedOutputStream, FileOutputStream}
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.io.Source

class CreateFileDatabase extends AnyFlatSpec {

  "it " should "" in {

    def printDatom(d: Datom, p: String): String = {
      p match {
        case "users/:tweet" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
        case "users/:tweetedBy" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
        case "users/:username" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
        case "users/:likes" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
        case "users/:follows" => s"[${d.a},${new String(d.getV.toByteArray)},${d.e},${d.t}]"
        case "users/:age" => s"[${d.a},${java.nio.ByteBuffer.allocate(4).put(d.getV.toByteArray).flip().getInt()},${d.e},${d.t}]"
        case _ => ""
      }
    }

    val logger = LoggerFactory.getLogger(this.getClass)

    //val db = new DatomDatabase("twitter-db", 64, 64)
    val rand = ThreadLocalRandom.current()

    val n = 100

    var datoms = Seq.empty[Datom]
    var users = Seq.empty[(String, String)]

    // Inserting some users...
    for(i<-0 until n){

      val id = UUID.randomUUID().toString
      val username = s"user-$i"
      val age = rand.nextInt(18, 100)
      val now = System.currentTimeMillis()

      users :+= username -> id

      datoms ++= Seq(
        Datom(
          a = Some("users/:username"),
          v = Some(ByteString.copyFrom(username.getBytes())),
          e = Some(id),
          t = Some(now),
          op = Some(true)
        ),

        Datom(
          a = Some("users/:age"),
          v = Some(ByteString.copyFrom(java.nio.ByteBuffer.allocate(4).putInt(age).flip().array())),
          e = Some(id),
          t = Some(now),
          op = Some(true)
        )
      )
    }

    val followers = TrieMap.empty[String, Seq[String]]

    // Insert some followers
    for(i<-0 until 1000){
      val (follower, _) = users(rand.nextInt(0, users.length))
      val (following, _) = users(rand.nextInt(0, users.length))
      val now = System.currentTimeMillis()

      if(follower.compareTo(following) != 0){
        if(!followers.isDefinedAt(follower)){

          followers.put(follower, Seq(following))

          datoms = datoms :+ Datom(
            a = Some("users/:follows"),
            v = Some(ByteString.copyFrom(follower.getBytes())),
            e = Some(following),
            t = Some(now),
            op = Some(true)
          )

        } else if(!followers(follower).exists{x => x.compareTo(following) == 0}){
          followers.update(follower, followers(follower) :+ following)

          datoms = datoms :+ Datom(
            a = Some("users/:follows"),
            v = Some(ByteString.copyFrom(follower.getBytes())),
            e = Some(following),
            t = Some(now),
            op = Some(true)
          )

        }
      }
    }

    val tweets = TrieMap.empty[String, String]
    val likes = TrieMap.empty[String, Seq[String]]

    val verbs = Seq("like", "don't like", "love", "hate")
    val foods = Seq("tacos", "banana", "apples", "oranges", "blueberry", "watermelon", "strawberry", "avocado", "pear", "lemons",
      "pasta", "rice", "beans")
    val drinks = Seq("shot", "juice", "water", "vitamin", "rum", "vodka", "champagne")

    // Inserting some tweets...
    for(i<-0 until 100){
      val (user, id) = users(rand.nextInt(0, users.length))
      val now = System.currentTimeMillis()
      val tweet = s"I ${verbs(rand.nextInt(0, verbs.length))} to eat ${foods(rand.nextInt(0, foods.length))} and to drink ${drinks(rand.nextInt(0, drinks.length))}"
      val tweetId = UUID.randomUUID().toString

      tweets.put(tweetId, tweet)

      datoms ++= Seq(
        Datom(
          a = Some("users/:tweet"),
          v = Some(ByteString.copyFrom(tweet.getBytes())),
          e = Some(tweetId),
          t = Some(now),
          op = Some(true)
        ),
        Datom(
          a = Some("users/:tweetedBy"),
          v = Some(ByteString.copyFrom(user.getBytes())),
          e = Some(tweetId),
          t = Some(now),
          op = Some(true)
        )
      )

      // Inserting some tweet likes...
      for(j<-0 until rand.nextInt(1, 10)){
        val (liker, likerId) = users(rand.nextInt(0, users.length))

        val liked = likes.get(tweetId)

        if(liked.isEmpty || !liked.get.exists{l => l.compareTo(liker) == 0}){

          likes.update(tweetId, if(liked.isEmpty) Seq(liker) else liked.get :+ liker)

          datoms ++= Seq(
            Datom(
              a = Some("users/:likes"),
              v = Some(ByteString.copyFrom(tweetId.getBytes())),
              e = Some(liker),
              t = Some(now),
              op = Some(true)
            )
          )
        }
      }

    }

    val fileDB = FileDB(datoms)
    val bytes = Any.pack(fileDB).toByteArray

    val bos = new BufferedOutputStream(new FileOutputStream("twitter.db"))

    bos.write(bytes)
    bos.close()

    //logger.debug(s"${Console.GREEN_B}idata: ${datoms.map{d => printDatom(d, d.getA)}}${Console.RESET}\n")

    //Await.result(db.insert(datoms), Duration.Inf)

    /*val list = Await.result(TestHelper.all(db.avetIndex.inOrder()(db.avetOrdering)), Duration.Inf)
    logger.debug(s"idata: ${list.map{case (k, v) => printDatom(k, k.getA)}}")*/

    /*logger.info(s"${Console.GREEN_B}users: ${users}${Console.RESET}\n")
    logger.info(s"${Console.MAGENTA_B}tweets: ${tweets}${Console.RESET}\n")
    logger.info(s"${Console.YELLOW_B}likes: ${likes}${Console.RESET}\n")
    logger.info(s"${Console.RED_B}followers: ${followers}${Console.RESET}\n")*/
  }

}
