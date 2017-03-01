package edu.msstate.dasi.csb

import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

/**
  * Created by spencer on 11/3/16.
  */
class ParallelBaSynth(partitions: Int, baIter: Long, nodesPerIter: Long) extends GraphSynth {

  /**
    *  Computes the RDD of the additional edges that should be added accordingly to the edge distribution.
    *
    *  @param edgeList The RDD of the edges returned by the Kronecker algorithm.
    *  @return The RDD of the additional edges that should be added to the one returned by Kronecker algorithm.
    */
  private def getMultiEdgesRDD(edgeList: RDD[Edge[EdgeData]], seedDists: DataDistributions): RDD[Edge[EdgeData]] = {
    val dataDistBroadcast = sc.broadcast(seedDists)

    val multiEdgeList = edgeList.flatMap { edge =>
      val multiEdgesNum = dataDistBroadcast.value.getOutEdgeSample
      var multiEdges = Array.empty[Edge[EdgeData]]

      for ( _ <- 1L until multiEdgesNum.toLong ) {
        multiEdges :+= Edge[EdgeData](edge.srcId, edge.dstId)
      }

      multiEdges
    }
    multiEdgeList
  }

  /**
    *
    * @param inVertices RDD of vertices and their edu.msstate.dasi.VertexData
    * @param inEdges RDD of edges and their edu.msstate.dasi.EdgeData
    * @param iter Number of iterations to perform BA
    * @return Graph containing vertices + edu.msstate.dasi.VertexData, edges + edu.msstate.dasi.EdgeData
    */
  private def generateBAGraph(inVertices: RDD[(VertexId, VertexData)], inEdges: RDD[Edge[EdgeData]], seedDists: DataDistributions, iter: Long, nodesPerIter: Long, withProperties: Boolean): Graph[VertexData,EdgeData] =
  {
    // TODO: this method shouldn't have the withProperties parameter, we have to check why it's used in the algorithm

    var nodeIndices = Array.empty[VertexId]
    inVertices.foreach(record => nodeIndices :+= record._1)
    var totalVertices: Long = inVertices.count()
    val nEdges: Long = inEdges.count()
    val localPartitions = math.min(nEdges, partitions).toInt
    val recordsPerPartition = math.min( (nEdges / localPartitions).toInt, Int.MaxValue )
    var edgeList: RDD[Edge[EdgeData]] = inEdges

    //var averageNumOfEdges = if (nEdges / totalVertices > 0L) 2L * (nEdges / totalVertices)
    //else { 2L }

    var edgesToAdd: Array[Edge[EdgeData]] = Array.empty[Edge[EdgeData]]

    var nPI = nodesPerIter

    val iters: Int = if (iter > nodesPerIter) math.ceil(iter.toDouble / nodesPerIter).toInt
    else {
      nPI = iter; 1
    }

    val dataDistBroadcast = sc.broadcast(seedDists)
    //val averageNumOfEdgesBC = sc.broadcast(averageNumOfEdges)

    for (i <- 1 to iters)
    {
      //println("Entered the loop")
      totalVertices += 1
      var newVertices = totalVertices to (totalVertices + nPI.toInt) toList
      var newVerticesRDD = sc.parallelize(newVertices)
      val oldEdgeList = edgeList
      val someAry:Array[Edge[EdgeData]] = oldEdgeList.collect
      var nEdges : Long = oldEdgeList.count()

      val inEdgesIndexedBroadcast = sc.broadcast(someAry)

      val curEdges: RDD[Edge[EdgeData]] = newVerticesRDD.flatMap{ x =>
        val r = Random
        val r2 = Random
        val r3 = Random
        var attachTo: Long = 0L
        val numOutEdgesToAdd = Math.abs (r2.nextLong () ) % dataDistBroadcast.value.getOutEdgeSample + 1 //averageNumOfEdgesBC.value + 1//

        //Add Out Edges
        var subsetEdges = Array.empty[Edge[EdgeData]]
        for ( _ <- 1L to numOutEdgesToAdd )
        {
          val attachToEdge: Int = Math.abs (r.nextInt () ) % nEdges.toInt
          val edge: Edge[EdgeData] = inEdgesIndexedBroadcast.value(attachToEdge)//inEdgesIndexedRDD.lookup(attachToEdge).last
          attachTo = edge.srcId
          subsetEdges :+= Edge[EdgeData](x, attachTo)
        }

        //Add In Edges
        val numInEdgesToAdd = Math.abs (r2.nextLong () ) % dataDistBroadcast.value.getInEdgeSample + 1 //averageNumOfEdgesBC.value + 1//
        //IN DEGREE
        for ( _ <- 1L to numInEdgesToAdd )
        {
          val attachToEdge: Int = Math.abs (r.nextInt () ) % nEdges.toInt
          val edge: Edge[EdgeData] = inEdgesIndexedBroadcast.value(attachToEdge)
          attachTo = edge.srcId
          subsetEdges :+= Edge[EdgeData](attachTo, x)

        }

        subsetEdges
      }



      edgeList = oldEdgeList.union(curEdges).distinct().coalesce(partitions).setName("edgeList#" + curEdges).persist(StorageLevel.MEMORY_AND_DISK)
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

  def genGraph(seed: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, EdgeData] = {
    println()
    println("Running BA with " + baIter + " iterations.")
    println()

    generateBAGraph(seed.vertices, seed.edges, seedDists, baIter.toLong, nodesPerIter, withProperties = true)
  }
}
