package edu.msstate.dasi.csb

import org.apache.spark.graphx._

import scala.collection.mutable
import scala.collection.mutable.Queue
import scala.reflect.ClassTag

/**
  * Created by justin on 2/17/17.
  */
object ClosenessCentrality {
  private class DistanceNodePair(var distance: Long, var totalPairs: Long) extends Comparable[DistanceNodePair] {

    override def compareTo(dp: DistanceNodePair): Int = (this.distance - dp.distance).toInt
  }

  private class NodeVisitCounter extends java.io.Serializable {

    var totalPairs: Long = _

    var levelSize: mutable.HashMap[Long, Long] = _ //first is distance second is pair at that distance
  }
  private def BFSNode[VD: ClassTag, ED: ClassTag](nID: Long, graph: Graph[VD, ED]): NodeVisitCounter = {


    val q = new Queue[Long]()
    q.enqueue(nID)
    val visited = new mutable.HashSet[VertexId]()
    val levelSize = new mutable.HashMap[Long, Long]()
    visited.add(nID)
    var totalPairs: Long = 0
    val visitCounter = new NodeVisitCounter()
    var level = 0
    while(q.nonEmpty)
    {
      val size = q.size
      totalPairs += size
      if(level != 0)
      {
        levelSize.put(level, size);
      }

      var list: Array[Long] = new Array[Long](size)
      for(x <- 0 until size)
      {
        list(x) = q.dequeue()
      }
      var children: Array[VertexId] = null
      if(list.length > 0)
        {
          for(x <- list)
          {

            var node: VertexId = x
            if(graph.collectNeighborIds(EdgeDirection.Out).lookup(node).length > 0)
              {
                children = graph.collectNeighborIds(EdgeDirection.Out).lookup(node).head
                //        children = hashmap.value.get(x).head
                for(c: Long <- children)
                {
                  val childNode = graph.vertices.lookup(c)//hashmap.value.get(c).head
                  if(!visited.contains(c))
                  {
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

    return visitCounter
  }
  def getClosenessOfVert[VD: ClassTag, ED: ClassTag](vertex: VertexId, graph: Graph[VD, ED]): Double =
  {
    val visitCenter = BFSNode(vertex, graph)

    var denomenator: Long = 0L
    for(x <- visitCenter.levelSize.keySet)
      {
        denomenator += visitCenter.levelSize.get(x).head * x
      }
    if(denomenator == 0) return -1
    val count = graph.vertices.count().toDouble
    return count / denomenator
  }


}
