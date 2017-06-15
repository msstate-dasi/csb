package edu.msstate.dasi.csb.data.synth

import edu.msstate.dasi.csb.data.distributions.DataDistributions
import edu.msstate.dasi.csb.model.{EdgeData, VertexData}
import edu.msstate.dasi.csb.sc
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.storage.StorageLevel

import scala.util.Random

class ParallelBa(partitions: Int, baIter: Long, fractionPerIter: Double) extends GraphSynth {

  /**
   * Generates a graph using a parallel implementation of the Barabási–Albert algorithm.
   */
  private def parallelBa(seed: Graph[VertexData, EdgeData], seedDists: DataDistributions, iter: Long, fractionPerIter: Double): Graph[VertexData,EdgeData] = {
    /*
     * GraphX does not offer a way to generate a unique ID for new vertices on an existing graph, therefore we must
     * ensure that all the existing IDs are ordered and as close as possible between them, so as to be able to pick new
     * vertices IDs starting from the highest unused ID.
     *
     * We use zipWithUniqueId() instead of zipWithIndex() because the former does not trigger a spark job.
     */

    // Map each vertex ID to a new ID
    val verticesIdMap = seed.vertices
      .keys // Discard vertices data
      .zipWithUniqueId() // (id) => (id, newId)
      .persist(StorageLevel.MEMORY_AND_DISK) // The result will be used multiple times

    var nextVertexId = verticesIdMap.values.max()

    // Update vertex IDs in the existing edges according to verticesIdMap
    var edges = seed.edges
      .map( edge => (edge.srcId, edge.dstId) )  // Discard edge data and build a PairRDD[(srcId, dstId)]
      .join(verticesIdMap).values // (srcId, dstId) => ( srcId, (dstId, newSrcId) ) => (dstId, newSrcId)
      .join(verticesIdMap).values // (dstId, newSrcId) => ( dstId, (newSrcId, newDstId) ) => (newSrcId, newDstId)
      .map { case (seqSrcId, seqDstId) => Edge[EdgeData](seqSrcId, seqDstId)} // (newSrcId, newDstId) => Edge
      .persist(StorageLevel.MEMORY_AND_DISK) // The result will be used multiple times

    var oldRawEdges = sc.emptyRDD[(VertexId, VertexId)]
    var oldEdges = sc.emptyRDD[Edge[EdgeData]]

    for (_ <- 1L to iter ) {
      /*
       * The following statement picks random edges and, for each picked edge, creates a new edge which starts from a
       * new vertex and arrives to the destination vertex of the picked edge.
       */
      val newRawEdges = edges.sample(withReplacement = true, fractionPerIter) // Extract a fraction of the edges
        .repartition(partitions) // Balance the RDD among the workers
        .zipWithUniqueId() // Edge => (Edge, baseId)
        .map { case ( edge, id ) =>
          if (Random.nextBoolean) {
            (nextVertexId + id, edge.srcId) // (Edge, baseId) => (newId, srcId)
          } else {
            (nextVertexId + id, edge.dstId) // (Edge, baseId) => (newId, dstId)
          } }
        .persist(StorageLevel.MEMORY_AND_DISK)

      // Keep track of the max vertex ID used
      nextVertexId += newRawEdges.keys.max()

      // Un-persist intermediate raw edges from the previous iteration
      oldRawEdges.unpersist()
      oldRawEdges = newRawEdges

      // Un-persist intermediate edges from the previous iteration
      oldEdges.unpersist()
      oldEdges = edges

      val seedDistsBroadcast = sc.broadcast(seedDists)

      // Create incoming/outgoing edges in place of each new raw edge
      val newEdges = newRawEdges.flatMap { case (newId, dstId) =>
        var multiEdges = Array.empty[Edge[EdgeData]]

        val outEdgesNum = seedDistsBroadcast.value.outDegree.sample
        val inEdgesNum = seedDistsBroadcast.value.inDegree.sample

        for ( _ <- 1 until outEdgesNum ) multiEdges :+= Edge[EdgeData](newId, dstId)
        for ( _ <- 1 until inEdgesNum ) multiEdges :+= Edge[EdgeData](dstId, newId)

        multiEdges
      }

      // Merge new edges with existing ones
      edges = edges.union(newEdges).coalesce(partitions).persist(StorageLevel.MEMORY_AND_DISK)
    }

    verticesIdMap.unpersist()

    Graph.fromEdges(
      edges,
      null.asInstanceOf[VertexData],
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK
    )
  }

  protected def genGraph(seed: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, EdgeData] = {
    parallelBa(seed, seedDists, baIter, fractionPerIter)
  }
}
