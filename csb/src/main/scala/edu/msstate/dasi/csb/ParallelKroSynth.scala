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
  private def kronFit(seed: Graph[VertexData, EdgeData]): Array[(VertexId, VertexId, Double)] = {
    loadMtx(mtxFile)
  }

  /**
   * Generates a graph using a parallel implementation of the stochastic Kronecker algorithm.
   */
  private def stochasticKro(seedMtx: Array[(VertexId, VertexId, Double)], seedDists: DataDistributions, iterations: Int): Graph[VertexData,EdgeData] = {
    val seedVerticesNum = seedMtx.length / 2
    val probSum = seedMtx.map{ case (_, _, prob) => prob}.sum

    val expectedVertices = math.pow(seedVerticesNum, iterations).toLong
    val expectedEdges = math.pow(probSum, iterations).toLong

    println(s"Expected # of Vertices: $expectedVertices")
    println(s"Expected # of Edges: $expectedEdges")

    var cumulativeProb = 0.0
    var cumulativeProbMtx = Array.empty[(VertexId, VertexId, Double)]

    for ( (src, dst, prob) <- seedMtx ) {
      cumulativeProb += prob
      cumulativeProbMtx :+= (src, dst, cumulativeProb / probSum)
    }

    var rawEdges = sc.emptyRDD[(VertexId, VertexId)]
    var edgesCount = 0L

    do {
      val edgesPerPartition = (expectedEdges - edgesCount) / partitions
      val remainingEdges = (expectedEdges - edgesCount) % partitions

      val oldEdges = rawEdges

      val emptySeq = sc.parallelize(Array.empty[(VertexId, VertexId)], partitions)

      val newEdges = emptySeq.mapPartitionsWithIndex { (partitionIndex, _) =>
        var count = 0L
        var partitionEdges = Array.empty[(VertexId, VertexId)]

        if (partitionIndex < remainingEdges) count -= 1

        while (count < edgesPerPartition) {
          var range = expectedVertices
          var src = 0L
          var dst = 0L

          for (i <- 0 until iterations) {
            var seedIndex = 0
            val prob = Random.nextDouble()

            while (prob > cumulativeProbMtx(seedIndex)._3) {
              seedIndex += 1
            }

            range = range / seedVerticesNum
            src += cumulativeProbMtx(seedIndex)._1 * range
            dst += cumulativeProbMtx(seedIndex)._2 * range
          }

          partitionEdges :+= (src, dst)
          count += 1
        }

        partitionEdges.iterator
      }

      rawEdges = rawEdges.union(newEdges).coalesce(partitions).distinct().persist(StorageLevel.MEMORY_AND_DISK)

      edgesCount = rawEdges.count()

      oldEdges.unpersist()
    }
    while (edgesCount < expectedEdges)

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

    stochasticKro(kronFit(seed), seedDists, iterations)
  }
}
