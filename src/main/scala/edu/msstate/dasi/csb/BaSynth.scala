package edu.msstate.dasi.csb

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId, Edge}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random

/**
  * Created by spencer on 11/3/16.
  */
class BaSynth(partitions: Int, baIter: Long, nodesPerIter: Long) extends GraphSynth {

  /**
   *
   * @param inVertices RDD of vertices and their edu.msstate.dasi.VertexData
   * @param inEdges RDD of edges and their edu.msstate.dasi.EdgeData
   * @param iter Number of iterations to perform BA
   * @return Graph containing vertices + edu.msstate.dasi.VertexData, edges + edu.msstate.dasi.EdgeData
   */
  private def generateBAGraph(sc: SparkContext, inVertices: RDD[(VertexId, VertexData)], inEdges: RDD[Edge[EdgeData]], seedDists: DataDistributions, iter: Long, nodesPerIter: Long, withProperties: Boolean): Graph[VertexData,EdgeData] = {
    // TODO: this method shouldn't have the withProperties parameter, we have to check why it's used in the algorithm
    val r = Random

    var theGraph = Graph(inVertices, inEdges, VertexData())

    var nodeIndices: mutable.HashMap[String, VertexId] = new mutable.HashMap[String, VertexId]()
    var degList: Array[(VertexId, Int)] = theGraph.degrees.sortBy(_._1).collect()

    inVertices.foreach(record => nodeIndices += record._2.data -> record._1)

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
        //String is IP:Port ex. "192.168.0.1:80"
        val tempNodeProp: VertexData = if (withProperties) VertexData() else {
          val DATA = seedDists.getIpSample
          VertexData(DATA)
        }
        val srcId: VertexId =
          if (nodeIndices.contains(tempNodeProp.data))
            nodeIndices.get(tempNodeProp.data).head
          else
            degList.last._1.toLong + 1
        var srcIndex =
          if (nodeIndices.contains(tempNodeProp.data))
            nodeIndices.get(tempNodeProp.data).head.toLong
          else
            degList.length
        if (degList.head._1 != 0L) {
          srcIndex -= 1
        }


        vertToAdd = vertToAdd :+ (srcId, tempNodeProp)
        degList = degList :+ (srcId, 0) //initial degree of 0

        val numEdgesToAdd = seedDists.getOutEdgeSample

        for (_ <- 1L to numEdgesToAdd.toLong) {
          val attachTo: Long = (Math.abs(r.nextLong()) % (degSum - 1)) + 1

          var dstIndex: Long = 0
          var tempDegSum: Long = 0
          while (tempDegSum < attachTo) {
            tempDegSum += degList(dstIndex.toInt)._2
            dstIndex += 1
          }

          dstIndex = dstIndex - 1
          //now we know that the node must attach at index
          val dstId: VertexId = degList(dstIndex.toInt)._1

          edgesToAdd = edgesToAdd :+ Edge(srcId, dstId, EdgeData())

          //This doesn't matter, but to be correct, this code updates the degList dstId's degree
          degList(dstIndex.toInt) = (degList(dstIndex.toInt)._1, degList(dstIndex.toInt)._2 + 1)
          degList(srcIndex.toInt) = (degList(srcIndex.toInt)._1, degList(srcIndex.toInt)._2 + 1)

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

  protected def genGraph(sc: SparkContext, seed: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, EdgeData] = {
    println()
    println("Running BA with " + baIter + " iterations.")
    println()

    generateBAGraph(sc, seed.vertices, seed.edges, seedDists, baIter.toLong, nodesPerIter, withProperties = true)
  }
}
