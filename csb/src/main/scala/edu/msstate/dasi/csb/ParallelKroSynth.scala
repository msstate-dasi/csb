package edu.msstate.dasi.csb

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.storage.StorageLevel

class ParallelKroSynth(partitions: Int, genIter: Int) extends GraphSynth {

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

    val verticesCount = verticesIdMap.count()

    // Update vertex IDs in the existing edges according to verticesIdMap
    var edges = seed.edges
      .map( edge => (edge.srcId, edge.dstId) )  // Discard edge data and build a PairRDD[(srcId, dstId)]
      .join(verticesIdMap).values // (srcId, dstId) => ( srcId, (dstId, newSrcId) ) => (dstId, newSrcId)
      .join(verticesIdMap).values // (dstId, newSrcId) => ( dstId, (newSrcId, newDstId) ) => (newSrcId, newDstId)
      .map { case (seqSrcId, seqDstId) => Edge[EdgeData](seqSrcId, seqDstId)} // (newSrcId, newDstId) => Edge
    //      .persist(StorageLevel.MEMORY_AND_DISK) // The result will be used multiple times

    val seedEdges = edges.persist(StorageLevel.MEMORY_AND_DISK) // Will be used multiple times

    for (_ <- 1 to genIter) {

    }

    seedEdges.unpersist()
    verticesIdMap.unpersist()

    null
  }

  /**
   * Generates a synthetic graph with no properties starting from a seed graph.
   */
  protected def genGraph(seed: Graph[VertexData, EdgeData], seedDists: DataDistributions): Graph[VertexData, EdgeData] = {
    parallelKro(seed, seedDists)
  }
}
