package services.scalable.datalog

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType}
import services.scalable.datalog.grpc.{DBMeta, Datom, IndexMeta}
import services.scalable.index.{Block, Bytes, Cache, Context, Leaf, QueryableIndex, Serializer, Storage, loader}
import services.scalable.index.DefaultComparators.ord
import services.scalable.index.impl.{DefaultCache, DefaultContext, MemoryStorage}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._
import com.google.protobuf.any.Any
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer

class DatomDatabase(val name: String, val KEYSPACE: String, val NUM_LEAF_ENTRIES: Int, val NUM_META_ENTRIES: Int)
                   (implicit val ec: ExecutionContext,
                    val serializer: Serializer[Block[Datom, Bytes]],
                    val cache: Cache[Datom, Bytes],
                    val storage: Storage[Datom, Bytes]) {

  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val eavtOrdering = new Ordering[Datom] {
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

  val avetOrdering = new Ordering[Datom] {
    override def compare(x: Datom, y: Datom): Int = {
      var r = ord.compare(x.getA.getBytes(), y.getA.getBytes())

      if(r != 0) return r

      r = ord.compare(x.getV.toByteArray, y.getV.toByteArray)

      if(r != 0) return r

      r = x.getE.compareTo(y.getE)

      if(r != 0) return r

      r = x.getT.compareTo(y.getT)

      if(r != 0) return r

      x.getOp.compareTo(y.getOp)
    }
  }

  val aevtOrdering = new Ordering[Datom] {
    override def compare(x: Datom, y: Datom): Int = {
      var r = ord.compare(x.getA.getBytes(), y.getA.getBytes())

      if(r != 0) return r

      r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

      if(r != 0) return r

      r = ord.compare(x.getV.toByteArray, y.getV.toByteArray)

      if(r != 0) return r

      r = x.getT.compareTo(y.getT)

      if(r != 0) return r

      x.getOp.compareTo(y.getOp)
    }
  }

  val vaetOrdering = new Ordering[Datom] {
    override def compare(x: Datom, y: Datom): Int = {
      var r = ord.compare(x.getV.toByteArray, y.getV.toByteArray)

      if(r != 0) return r

      r = ord.compare(x.getA.getBytes(), y.getA.getBytes())

      if(r != 0) return r

      r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

      if(r != 0) return r

      r = x.getT.compareTo(y.getT)

      if(r != 0) return r

      x.getOp.compareTo(y.getOp)
    }
  }

  var eavtIndex: QueryableIndex[Datom, Bytes] = null
  var eavtCtx: DefaultContext[Datom, Bytes] = null

  var aevtIndex: QueryableIndex[Datom, Bytes] = null
  var aevtCtx: DefaultContext[Datom, Bytes] = null

  var avetIndex: QueryableIndex[Datom, Bytes] = null
  var avetCtx: DefaultContext[Datom, Bytes] = null

  var vaetIndex: QueryableIndex[Datom, Bytes] = null
  var vaetCtx: DefaultContext[Datom, Bytes] = null

  val session = CqlSession
    .builder()
    .withConfigLoader(loader)
    .withKeyspace(KEYSPACE)
    .build()

  val INSERT = session.prepare("insert into meta(name, num_leaf_entries, num_meta_entries, roots) values (:name, :le, :me, :roots);")
  val INSERT_BLOCK = session.prepare("insert into blocks(id, bin, leaf, size) values (:id, :bin, :leaf, :size);")
  val READ_ROOTS = session.prepare("select * from meta where name = :name;")
  val UPDATE_ROOTS = session.prepare("update meta set roots = :roots where name = :name;")

  def create(): Future[Boolean] = {
    val roots = DBMeta(name, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    eavtCtx = new DefaultContext[Datom, Bytes](s"$name-eavt", None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, eavtOrdering)
    eavtIndex = new QueryableIndex[Datom, Bytes]()(ec, eavtCtx, eavtOrdering)

    aevtCtx = new DefaultContext[Datom, Bytes](s"$name-aevt", None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, aevtOrdering)
    aevtIndex = new QueryableIndex[Datom, Bytes]()(ec, aevtCtx, aevtOrdering)

    avetCtx = new DefaultContext[Datom, Bytes](s"$name-avet", None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, avetOrdering)
    avetIndex = new QueryableIndex[Datom, Bytes]()(ec, avetCtx, avetOrdering)

    vaetCtx = new DefaultContext[Datom, Bytes](s"$name-vaet", None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, vaetOrdering)
    vaetIndex = new QueryableIndex[Datom, Bytes]()(ec, vaetCtx, vaetOrdering)

    session.executeAsync(
      INSERT
      .bind()
      .setString("name", name)
      .setInt("le", NUM_LEAF_ENTRIES)
      .setInt("me", NUM_META_ENTRIES)
      .setByteBuffer("roots", ByteBuffer.wrap(Any.pack(roots).toByteArray)))
      .asScala.map(_.wasApplied())
  }

  def loadOrCreate(): Future[Boolean] = {
    session.executeAsync(READ_ROOTS.bind()
    .setString("name", name)).asScala.flatMap { rs =>
      val one = rs.one()

      if(one == null){
        create()
      } else {
        val roots = Any.parseFrom(one.getByteBuffer("roots").array()).unpack(DBMeta)

        eavtCtx = new DefaultContext[Datom, Bytes](s"$name-eavt", roots.eavtRoot.map(_.root), NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, eavtOrdering)
        eavtIndex = new QueryableIndex[Datom, Bytes]()(ec, eavtCtx, eavtOrdering)

        aevtCtx = new DefaultContext[Datom, Bytes](s"$name-aevt", roots.aevtRoot.map(_.root), NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, aevtOrdering)
        aevtIndex = new QueryableIndex[Datom, Bytes]()(ec, aevtCtx, aevtOrdering)

        avetCtx = new DefaultContext[Datom, Bytes](s"$name-avet", roots.avetRoot.map(_.root), NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, avetOrdering)
        avetIndex = new QueryableIndex[Datom, Bytes]()(ec, avetCtx, avetOrdering)

        vaetCtx = new DefaultContext[Datom, Bytes](s"$name-vaet", roots.vaetRoot.map(_.root), NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, vaetOrdering)
        vaetIndex = new QueryableIndex[Datom, Bytes]()(ec, vaetCtx, vaetOrdering)

        Future.successful(true)
      }
    }
  }

  def updateMeta(): Future[Boolean] = {
    val roots = DBMeta(
      name,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES,
      Some(IndexMeta(eavtCtx.root.get)),
      Some(IndexMeta(aevtCtx.root.get)),
      Some(IndexMeta(avetCtx.root.get)),
      Some(IndexMeta(vaetCtx.root.get))
    )

    session.executeAsync(UPDATE_ROOTS.bind()
    .setByteBuffer("roots", ByteBuffer.wrap(Any.pack(roots).toByteArray))
      .setString("name", name)).asScala.map(_.wasApplied())
  }

  def getContexts(): Seq[DefaultContext[Datom, Bytes]] = Seq(
    eavtCtx,
    aevtCtx,
    avetCtx,
    vaetCtx
  )

  def save(): Future[Boolean] = {
    val contexts = getContexts()

    contexts.foreach { ctx =>
      ctx.blocks.foreach { case (_, b) =>
        b.root = ctx.root
      }
    }

    val stm = BatchStatement.builder(DefaultBatchType.LOGGED)

    contexts.foreach { ctx =>
      val blocks = ctx.blocks.map(_._2)

      blocks.map { b =>
        val bin = serializer.serialize(b)

        stm.addStatement(INSERT_BLOCK.bind().setString("id", b.unique_id)
          .setByteBuffer("bin", ByteBuffer.wrap(bin))
          .setBoolean("leaf", b.isInstanceOf[Leaf[Datom, Bytes]])
          .setLong("size", bin.length)
        )
      }
    }

    session.executeAsync(stm.build()).asScala.flatMap(ok => if(ok.wasApplied()) updateMeta() else Future.successful(false))
      .map { r =>

        contexts.foreach { ctx =>
          ctx.blocks.clear()
          ctx.parents.clear()
        }

        r
      }
  }

  def insert(data: Seq[Tuple2[Datom, Bytes]]): Future[Boolean] = {
    val inserts = Seq(
      eavtIndex.insert(data)(eavtOrdering),
      aevtIndex.insert(data)(aevtOrdering),
      avetIndex.insert(data)(avetOrdering),
      vaetIndex.insert(data)(vaetOrdering)
    )

    Future.sequence(inserts).map(_ => true)
  }

  val findOrd = new Ordering[Datom] {
    override def compare(x: Datom, y: Datom): Int = {
      val r = ord.compare(x.getE.getBytes(), y.getE.getBytes())

      if(r != 0) return r

      ord.compare(x.getA.getBytes(), y.getA.getBytes())
    }
  }

  protected def find(e: String, a: String): Future[Option[Datom]] = {
    val it = eavtIndex.find(Datom(e = Some(e), a = Some(a)), false, findOrd)
    it.setLimit(1)

    it.hasNext().flatMap {
      case true => it.next().map(_.headOption.map(_._1))
      case false => Future.successful(None)
    }
  }

  /*def update(data: Seq[Tuple2[Datom, Bytes]]): Future[Boolean] = {
    val updates = Seq(
      eavtIndex.update(data)(eavtOrdering),
      aevtIndex.update(data)(aevtOrdering),
      avetIndex.update(data)(avetOrdering),
      vaetIndex.update(data)(vaetOrdering)
    )

    Future.sequence(updates).map(_ => true)
  }*/

  def update(data: Seq[Tuple2[Datom, Bytes]]): Future[Boolean] = {
    val ids = data.map{case (d, _) => d.getE -> d.getA}

    Future.sequence(ids.map{case (e, a) => find(e, a)}).flatMap { datoms =>
      remove(datoms.filter(_.isDefined).map(_.get)).flatMap { ok =>
        insert(data)
      }
    }
  }

  def remove(data: Seq[Datom]): Future[Boolean] = {
    val updates = Seq(
      eavtIndex.remove(data)(eavtOrdering),
      aevtIndex.remove(data)(aevtOrdering),
      avetIndex.remove(data)(avetOrdering),
      vaetIndex.remove(data)(vaetOrdering)
    )

    Future.sequence(updates).map(_ => true)
  }

}
