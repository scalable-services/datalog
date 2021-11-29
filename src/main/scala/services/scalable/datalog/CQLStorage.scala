package services.scalable.datalog

import com.datastax.oss.driver.api.core.CqlSession
import org.slf4j.LoggerFactory
import services.scalable.datalog.grpc.Datom
import services.scalable.index.{Block, Bytes, Context, Serializer, Storage, loader}

import scala.jdk.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}

class CQLStorage(val NUM_LEAF_ENTRIES: Int, val NUM_META_ENTRIES: Int)
                (implicit val ec: ExecutionContext,
                 val session: CqlSession,
                 val serializer: Serializer[Block[Datom, Bytes]]
                ) extends Storage[Datom, Bytes]{

  val logger = LoggerFactory.getLogger(this.getClass)

  val SELECT = session.prepare("select * from blocks where id=?;")

  override def get(unique_id: String)(implicit ctx: Context[Datom, Bytes]): Future[Block[Datom, Bytes]] = {
    session.executeAsync(SELECT.bind().setString(0, unique_id)).asScala.map { rs =>
      val one = rs.one()
      val buf = one.getByteBuffer("bin")
      serializer.deserialize(buf.array())
    }
  }

  override def save(ctx: Context[Datom, Bytes]): Future[Boolean] = ???

  override def createIndex(indexId: String): Future[Context[Datom, Bytes]] = ???

  override def loadOrCreate(indexId: String): Future[Context[Datom, Bytes]] = ???

  override def load(indexId: String): Future[Context[Datom, Bytes]] = ???

  override def close(): Future[Unit] = ???
}
