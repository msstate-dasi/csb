package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Closeness Centrality algorithm implementation.
 */
class ClosenessCentrality(engine: SparkEngine, vertex: VertexId) extends Workload {
  val name = "Closeness Centrality"

  /**
   * Runs Closeness Centrality.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    getClosenessOfVert(vertex, graph)
  }

  private class DistanceNodePair(var distance: Long, var totalPairs: Long) extends Comparable[DistanceNodePair] {

    override def compareTo(dp: DistanceNodePair): Int = (this.distance - dp.distance).toInt
  }

  private class NodeVisitCounter extends java.io.Serializable {

    var totalPairs: Long = _

    var levelSize: mutable.HashMap[Long, Long] = _ //first is distance second is pair at that distance
  }

  private def BFSNode[VD: ClassTag, ED: ClassTag](nID: Long, graph: Graph[VD, ED]): NodeVisitCounter = {
    val q = new mutable.Queue[Long]()
    q.enqueue(nID)
    val visited = new mutable.HashSet[VertexId]()
    val levelSize = new mutable.HashMap[Long, Long]()
    visited.add(nID)
    var totalPairs: Long = 0
    val visitCounter = new NodeVisitCounter()
    var level = 0
    while (q.nonEmpty) {
      val size = q.size
      totalPairs += size
      if (level != 0) {
        levelSize.put(level, size)
      }

      val list: Array[Long] = new Array[Long](size)
      for (x <- 0 until size) {
        list(x) = q.dequeue()
      }
      var children: Array[VertexId] = null
      if (list.length > 0) {
        for (x <- list) {
          val node: VertexId = x
          if (graph.collectNeighborIds(EdgeDirection.Out).lookup(node).nonEmpty) {
            children = graph.collectNeighborIds(EdgeDirection.Out).lookup(node).head
            //        children = hashmap.value.get(x).head
            for (c: Long <- children) {
              // val childNode = graph.vertices.lookup(c) //hashmap.value.get(c).head
              if (!visited.contains(c)) {
                q.enqueue(c)
                visited.add(c)
              }
            }
          }
        }
      }
      level += 1
    }
    totalPairs -= 1

    visitCounter.levelSize = levelSize
    visitCounter.totalPairs = totalPairs

    visitCounter
  }

  private def getClosenessOfVert[VD: ClassTag, ED: ClassTag](vertex: VertexId, graph: Graph[VD, ED]): Double = {
    val visitCenter = BFSNode(vertex, graph)

    var denominator: Long = 0L
    for (x <- visitCenter.levelSize.keySet) {
      denominator += visitCenter.levelSize.get(x).head * x
    }
    if (denominator == 0) return -1
    val count = graph.vertices.count().toDouble
    count / denominator
  }
}
