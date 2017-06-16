package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
 * Single Source Shortest Path algorithm implementation.
 */
class SSSP(engine: SparkEngine, src: VertexId) extends Workload {
  val name = "Single Source Shortest Path"

  /**
   * Runs Single Source Shortest Path.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    for (dst <- graph.vertices.keys.toLocalIterator) {
      bfs(graph, src, dst)
    }
  }

  private def bfs[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], src: VertexId, dst: VertexId): Unit = {
    // if (src == dst) return List(src)
    if (src == dst) return

    // The attribute of each vertex is (dist from src, id of vertex with dist-1)
    var g: Graph[(Int, VertexId), ED] = graph.mapVertices((id, _) => (if (id == src) 0 else Int.MaxValue, 0L)).cache()

    // Traverse forward from src
    var dstAttr = (Int.MaxValue, 0L)
    while (dstAttr._1 == Int.MaxValue) {
      val msgs = g.aggregateMessages[(Int, VertexId)](e => if (e.srcAttr._1 != Int.MaxValue && e.srcAttr._1 + 1 < e.dstAttr._1) {
        e.sendToDst((e.srcAttr._1 + 1, e.srcId))
      }, (a, b) => if (a._1 < b._1) a else b).cache()

      // if (msgs.count == 0) return List.empty
      if (msgs.count == 0) return

      g = g.ops.joinVertices(msgs) { (_, oldAttr, newAttr) =>
        if (newAttr._1 < oldAttr._1) newAttr else oldAttr
      }.cache()

      dstAttr = g.vertices.filter(_._1 == dst).first()._2
    }

    // Traverse backward from dst and collect the path
    var path: List[VertexId] = dstAttr._2 :: dst :: Nil
    while (path.head != src) {
      path = g.vertices.filter(_._1 == path.head).first()._2._2 :: path
    }

    // path
  }
}
