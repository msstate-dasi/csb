package edu.msstate.dasi.csb

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

class ParallelBaSynth(partitions: Int, baIter: Long, nodesPerIter: Long, fractionPerIter: Double) extends GraphSynth {

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

        val outEdgesNum = seedDistsBroadcast.value.getOutEdgeSample
        val inEdgesNum = seedDistsBroadcast.value.getInEdgeSample

        for ( _ <- 1L until outEdgesNum ) multiEdges :+= Edge[EdgeData](newId, dstId)
        for ( _ <- 1L until inEdgesNum ) multiEdges :+= Edge[EdgeData](dstId, newId)

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

  /**
    *
    * @param seed Seed graph
    * @param iter Number of iterations to perform BA
    * @return Graph containing vertices + edu.msstate.dasi.VertexData, edges + edu.msstate.dasi.EdgeData
    */
  private def generateBAGraph(seed: Graph[VertexData, EdgeData], seedDists: DataDistributions, iter: Long, nodesPerIter: Long): Graph[VertexData,EdgeData] =
  {
//    var nodeIndices = Array.empty[VertexId]
//    seed.vertices.foreach(record => nodeIndices :+= record._1)
    var totalVertices: Long = seed.vertices.count()
//    val nEdges: Long = seed.edges.count()
//    val localPartitions = math.min(nEdges, partitions).toInt
//    val recordsPerPartition = math.min( (nEdges / localPartitions).toInt, Int.MaxValue )
    var edgeList: RDD[Edge[EdgeData]] = seed.edges

    //var averageNumOfEdges = if (nEdges / totalVertices > 0L) 2L * (nEdges / totalVertices)
    //else { 2L }

//    var edgesToAdd: Array[Edge[EdgeData]] = Array.empty[Edge[EdgeData]]

    var nPI = nodesPerIter

    val iters = if (iter > nodesPerIter) math.ceil(iter.toDouble / nodesPerIter).toInt
    else {
      nPI = iter; 1
    }

    val dataDistBroadcast = sc.broadcast(seedDists)
    //val averageNumOfEdgesBC = sc.broadcast(averageNumOfEdges)

    for (_ <- 1 to iters)
    {
      //println("Entered the loop")
      totalVertices += 1
      val newVertices = totalVertices to (totalVertices + nPI.toInt) toList
      val newVerticesRDD = sc.parallelize(newVertices)
      val oldEdgeList = edgeList
      val someAry:Array[Edge[EdgeData]] = oldEdgeList.collect
      var nEdges = oldEdgeList.count()

      val inEdgesIndexedBroadcast = sc.broadcast(someAry)

      val curEdges: RDD[Edge[EdgeData]] = newVerticesRDD.flatMap{ x =>
        val r = Random
        val r2 = Random
        var attachTo = 0L
        val numOutEdgesToAdd = Math.abs (r2.nextLong () ) % dataDistBroadcast.value.getOutEdgeSample + 1 //averageNumOfEdgesBC.value + 1//

        //Add Out Edges
        var subsetEdges = Array.empty[Edge[EdgeData]]
        for ( _ <- 1L to numOutEdgesToAdd )
        {
          val attachToEdge = Math.abs (r.nextInt () ) % nEdges.toInt
          val edge: Edge[EdgeData] = inEdgesIndexedBroadcast.value(attachToEdge)//inEdgesIndexedRDD.lookup(attachToEdge).last
          attachTo = edge.srcId
          subsetEdges :+= Edge[EdgeData](x, attachTo)
        }

        //Add In Edges
        val numInEdgesToAdd = Math.abs (r2.nextLong () ) % dataDistBroadcast.value.getInEdgeSample + 1 //averageNumOfEdgesBC.value + 1//
        //IN DEGREE
        for ( _ <- 1L to numInEdgesToAdd )
        {
          val attachToEdge = Math.abs (r.nextInt () ) % nEdges.toInt
          val edge: Edge[EdgeData] = inEdgesIndexedBroadcast.value(attachToEdge)
          attachTo = edge.srcId
          subsetEdges :+= Edge[EdgeData](attachTo, x)

        }

        subsetEdges
      }

      edgeList = oldEdgeList.union(curEdges.distinct()).coalesce(partitions).setName("edgeList#" + curEdges).persist(StorageLevel.MEMORY_AND_DISK)
      nEdges = edgeList.count()

      oldEdgeList.unpersist()

      totalVertices += nPI.toInt
    }

    //val newEdges = getMultiEdgesRDD(edgeList, seedDists).setName("newEdges")

    // TODO: finalEdgeList should be un-persisted after the next action (but the action will probably be outside this method)
    //val finalEdgeList = edgeList.union(newEdges)
    //.coalesce(partitions).setName("finalEdgeList").persist(StorageLevel.MEMORY_AND_DISK)
    println("Total # of Edges (including multi edges): " + edgeList.count())

    Graph.fromEdges(
      edgeList,
      null.asInstanceOf[VertexData],
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK
    )
  }

  protected def genGraph(seed: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, EdgeData] = {
    println()
    println("Running BA with " + baIter + " iterations.")
    println()

//    generateBAGraph(seed, seedDists, baIter, nodesPerIter)
    parallelBa(seed, seedDists, baIter, fractionPerIter)
  }
}
