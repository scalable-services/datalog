package services.scalable.datalog

import services.scalable.datalog.grpc.Datom
import services.scalable.index.{Bytes, QueryableIndex}
import services.scalable.index.DefaultComparators.ord
import services.scalable.index.impl.{DefaultCache, DefaultContext, MemoryStorage}

import scala.concurrent.{ExecutionContext, Future}

class DatomDatabase(val indexId: String, val NUM_LEAF_ENTRIES: Int, val NUM_META_ENTRIES: Int)(implicit val ec: ExecutionContext) {

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

  implicit val cache = new DefaultCache[Datom, Bytes](MAX_PARENT_ENTRIES = 80000)
  implicit val storage = new MemoryStorage[Datom, Bytes](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

  val eavtCtx = new DefaultContext[Datom, Bytes](s"$indexId-eavt", None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, eavtOrdering)
  val eavtIndex = new QueryableIndex[Datom, Bytes]()(ec, eavtCtx, eavtOrdering)

  val aevtCtx = new DefaultContext[Datom, Bytes](s"$indexId-aevt", None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, aevtOrdering)
  val aevtIndex = new QueryableIndex[Datom, Bytes]()(ec, aevtCtx, aevtOrdering)

  val avetCtx = new DefaultContext[Datom, Bytes](s"$indexId-avet", None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, avetOrdering)
  val avetIndex = new QueryableIndex[Datom, Bytes]()(ec, avetCtx, avetOrdering)

  val vaetCtx = new DefaultContext[Datom, Bytes](s"$indexId-vaet", None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(ec, storage, cache, vaetOrdering)
  val vaetIndex = new QueryableIndex[Datom, Bytes]()(ec, vaetCtx, vaetOrdering)

  def insert(data: Seq[Tuple2[Datom, Bytes]]): Future[Boolean] = {
    for {
      r1 <- eavtIndex.insert(data)(eavtOrdering).flatMap(_ => eavtCtx.save())
      r2 <- aevtIndex.insert(data)(aevtOrdering).flatMap(_ => aevtCtx.save())
      r3 <- avetIndex.insert(data)(avetOrdering).flatMap(_ => avetCtx.save())
      r4 <- vaetIndex.insert(data)(vaetOrdering).flatMap(_ => vaetCtx.save())
    } yield {
      true
    }
  }

}
