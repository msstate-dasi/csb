package edu.msstate.dasi.csb

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.storage.StorageLevel

import scala.io.Source
import scala.util.Random

class ParallelKroSynth(partitions: Int, mtxFile: String, iterations: Int) extends GraphSynth {

  private def loadMtx(mtxFile: String): Array[(VertexId, VertexId, Double)] = {
    val lines = Source.fromFile(mtxFile).getLines().toArray

    lines.zipWithIndex
      .map{ case (line, rowIndex) => (rowIndex, line.split(" ")) }
      .flatMap{ case (rowIndex, values) => values.map( (rowIndex, _) ).zipWithIndex }
      .map{ case ( (rowIndex, value), columnIndex) => (rowIndex.toLong, columnIndex.toLong, value.toDouble) }
  }

  /**
   * Generates a small probability matrix from a graph.
   *
   * The KronFit algorithm is a gradient descent based algorithm which ensures that the probability of generating the
   * original graph from the small probability matrix after performing Kronecker multiplications is very high.
   */
  private def kronFit(seed: Graph[VertexData, EdgeData]): Array[(Long, Long, Double)] = {
    loadMtx(mtxFile)
  }

  /**
   * Generates a graph using a parallel implementation of the stochastic Kronecker algorithm.
   */
  private def stochasticKro(seedMtx: Array[(VertexId, VertexId, Double)], seedDists: DataDistributions): Graph[VertexData,EdgeData] = {
    val seedSize = seedMtx.length

    var rawEdges = sc.parallelize(
      seedMtx.flatMap{ case (src, dst, prob) =>
        if (Random.nextDouble() < prob) {
          Array((src, dst))
        } else {
          None
        }
      }
    )

    for (_ <- 1 until iterations) {
      rawEdges = rawEdges.flatMap{ case (srcId, dstId) =>
        var edges = Array.empty[(VertexId, VertexId)]

        for ( (seedEdgeSrc, seedEdgeDst, seedEdgeProb) <- seedMtx ) {
          if (Random.nextDouble() < seedEdgeProb) {
            edges :+= (srcId * seedSize + seedEdgeSrc, dstId * seedSize + seedEdgeDst)
          }
        }
        edges
      }.repartition(partitions)
    }

    val seedDistsBroadcast = sc.broadcast(seedDists)

    val edges = rawEdges.flatMap{ case (srcId, dstId) =>
      var multiEdges = Array.empty[Edge[EdgeData]]

      val outEdgesNum = seedDistsBroadcast.value.getOutEdgeSample

      for ( _ <- 1L to outEdgesNum ) multiEdges :+= Edge[EdgeData](srcId, dstId)

      multiEdges
    }

    Graph.fromEdges(
      edges,
      null.asInstanceOf[VertexData],
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK
    )
  }

  /**
   * Generates a synthetic graph with no properties starting from a seed graph.
   */
  protected def genGraph(seed: Graph[VertexData, EdgeData], seedDists: DataDistributions): Graph[VertexData, EdgeData] = {
    println()
    println(s"Running parallel Kronecker with $iterations iterations.")
    println()

    stochasticKro(kronFit(seed), seedDists)
  }
}
