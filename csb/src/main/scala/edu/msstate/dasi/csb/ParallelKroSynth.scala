package edu.msstate.dasi.csb

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.storage.StorageLevel

class ParallelKroSynth(partitions: Int, iterations: Int) extends GraphSynth {

  /**
   * Generates a graph using a parallel implementation of the deterministic Kronecker algorithm.
   */
  private def parallelKro(seed: Graph[VertexData, EdgeData], seedDists: DataDistributions): Graph[VertexData,EdgeData] = {
    /*
     * We must ensure that all the existing IDs are all adjacent because the algorithm relies heavily on the size of the
     * adjacency matrix.
     *
     * We use zipWithIndex() instead of zipWithUniqueId() because we want to avoid any hole between indexes.
     */

    // Map each vertex ID to a new ID
    val verticesIdMap = seed.vertices
      .keys // Discard vertices data
      .zipWithIndex() // (id) => (id, newId)
      .persist(StorageLevel.MEMORY_AND_DISK) // The result will be used multiple times

    val verticesCount = verticesIdMap.count() // The size of the adjacency matrix

    // Update vertex IDs in the existing edges according to verticesIdMap
    var rawEdges = seed.edges
      .map( edge => (edge.srcId, edge.dstId) )  // Discard edge data and build a PairRDD[(srcId, dstId)]
      .join(verticesIdMap).values // (srcId, dstId) => ( srcId, (dstId, newSrcId) ) => (dstId, newSrcId)
      .join(verticesIdMap).values // (dstId, newSrcId) => ( dstId, (newSrcId, newDstId) ) => (newSrcId, newDstId)
      .persist(StorageLevel.MEMORY_AND_DISK) // The result will be used multiple times

    val seedEdges = rawEdges

    for (_ <- 1 to iterations) {
      rawEdges = rawEdges.cartesian(seedEdges).coalesce(partitions)
        .map{ case ( (srcId, dstId), (seedSrcId, seedDstId) ) =>
          (srcId * verticesCount + seedSrcId, dstId * verticesCount + seedDstId)
        }
    }

    val seedDistsBroadcast = sc.broadcast(seedDists)

    val edges = rawEdges.flatMap{ case (srcId, dstId) =>
      var multiEdges = Array.empty[Edge[EdgeData]]

      val outEdgesNum = seedDistsBroadcast.value.getOutEdgeSample

      for ( _ <- 1L until outEdgesNum ) multiEdges :+= Edge[EdgeData](srcId, dstId)

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
    parallelKro(seed, seedDists)
  }
}
