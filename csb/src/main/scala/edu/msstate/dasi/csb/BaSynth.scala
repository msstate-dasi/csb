package edu.msstate.dasi.csb

import edu.msstate.dasi.csb.distributions.DataDistributions
import edu.msstate.dasi.csb.model.{EdgeData, VertexData}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * Contains methods relating to generating Barabasi-Albert graphs.
  * @param partitions Number of partitions to use for RDDs inside the method.
  * @param baIter Number of iterations to perform the BA algorithm for.
  * @param nodesPerIter Percentage of nodes to generate per iteration.
  */
class BaSynth(partitions: Int, baIter: Long, nodesPerIter: Long) extends GraphSynth {

  /**
   * Internal method to generate a Barabasi-Albert graph.
   * @param inVertices RDD of vertices and their edu.msstate.dasi.VertexData
   * @param inEdges RDD of edges and their edu.msstate.dasi.EdgeData
   * @param iter Number of iterations to perform BA
   * @return Graph containing vertices + edu.msstate.dasi.VertexData, edges + edu.msstate.dasi.EdgeData
   */
  private def generateBAGraph(inVertices: RDD[(VertexId, VertexData)], inEdges: RDD[Edge[EdgeData]], seedDists: DataDistributions, iter: Long, nodesPerIter: Long, withProperties: Boolean): Graph[VertexData,EdgeData] = {
    // TODO: this method shouldn't have the withProperties parameter, we have to check why it's used in the algorithm
    val r = Random

    var theGraph = Graph(inVertices, inEdges, VertexData())

    var nodeIndices = Array.empty[VertexId]
    var degList: Array[(VertexId, Int)] = theGraph.degrees.sortBy(_._1).collect()

    inVertices.foreach(record => nodeIndices :+= record._1)

    var degSum: Long = degList.map(_._2).sum

    var edgesToAdd: Array[Edge[EdgeData]] = Array.empty[Edge[EdgeData]]
    var vertToAdd: Array[(VertexId, VertexData)] = Array.empty[(VertexId, VertexData)]

    var nPI = nodesPerIter

    val iters: Int = if (iter > nodesPerIter) math.ceil(iter.toDouble / nodesPerIter).toInt
    else {
      nPI = iter; 1
    }

    for (i <- 1 to iters) {
      println(i + "/" + math.ceil(iter.toDouble / partitions).toLong)
      for (_ <- 1 to nPI.toInt) {
        val tempNodeProp = VertexData()
        val srcId: VertexId = degList.last._1.toLong + 1
        var srcIndex = degList.length
        if (degList.head._1 != 0L) {
          srcIndex -= 1
        }


        vertToAdd = vertToAdd :+ (srcId, tempNodeProp)
        degList = degList :+ (srcId, 0) //initial degree of 0

        val numEdgesToAdd = seedDists.outDegree.sample

        for (_ <- 1 to numEdgesToAdd) {
          val attachTo: Long = (Math.abs(r.nextLong()) % (degSum - 1)) + 1

          var dstIndex = 0
          var tempDegSum: Long = 0
          while (tempDegSum < attachTo) {
            tempDegSum += degList(dstIndex)._2
            dstIndex += 1
          }

          dstIndex = dstIndex - 1
          //now we know that the node must attach at index
          val dstId: VertexId = degList(dstIndex)._1

          edgesToAdd = edgesToAdd :+ Edge[EdgeData](srcId, dstId)

          //This doesn't matter, but to be correct, this code updates the degList dstId's degree
          degList(dstIndex) = (degList(dstIndex)._1, degList(dstIndex)._2 + 1)
          degList(srcIndex) = (degList(srcIndex)._1, degList(srcIndex)._2 + 1)

          degSum += 2

        }
      }
      Array(true)
    }

    theGraph = Graph(
      inVertices.union(sc.parallelize(vertToAdd, partitions)),
      inEdges.union(sc.parallelize(edgesToAdd, partitions)),
      VertexData()
    )
    theGraph
  }

  /**
    * Method to generate a Barabasi-Albert graph.
    * @param seed A seed graph consisting of edges and vertices
    * @param seedDists A [[DataDistributions]] object containing the relevant property distributions of the seed graph
    * @return
    */
  protected def genGraph(seed: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, EdgeData] = {
    println()
    println("Running BA with " + baIter + " iterations.")
    println()

    generateBAGraph(seed.vertices, seed.edges, seedDists, baIter.toLong, nodesPerIter, withProperties = true)
  }
}
