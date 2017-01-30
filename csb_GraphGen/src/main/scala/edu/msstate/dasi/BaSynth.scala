package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random

/**
  * Created by spencer on 11/3/16.
  */
class BaSynth(sc: SparkContext, partitions: Int, dataDist: DataDistributions, graphPs: GraphPersistence, baIter: Long, nodesPerIter: Long) extends GraphSynth with DataParser {

  /***
    *
    * @param inVertices RDD of vertices and their edu.msstate.dasi.nodeData
    * @param inEdges RDD of edges and their edu.msstate.dasi.edgeData
    * @param iter Number of iterations to perform BA
    * @return Graph containing vertices + edu.msstate.dasi.nodeData, edges + edu.msstate.dasi.edgeData
    */
  def generateBAGraph(inVertices: RDD[(VertexId, nodeData)], inEdges: RDD[Edge[edgeData]], iter: Long, nodesPerIter: Long, withProperties: Boolean): Graph[nodeData,edgeData] = {
    // TODO: this method shouldn't have the withProperties parameter, we have to check why it's used in the algorithm
    val r = Random

    var theGraph = Graph(inVertices, inEdges, nodeData())

    var nodeIndices: mutable.HashMap[String, VertexId] = new mutable.HashMap[String, VertexId]()
    var degList: Array[(VertexId, Int)] = theGraph.degrees.sortBy(_._1).collect()

    inVertices.foreach(record => nodeIndices += record._2.data -> record._1)

    var degSum: Long = degList.map(_._2).sum

    var edgesToAdd: Array[Edge[edgeData]] = Array.empty[Edge[edgeData]]
    var vertToAdd: Array[(VertexId, nodeData)] = Array.empty[(VertexId, nodeData)]

    var nPI = nodesPerIter

    val iters: Int = if (iter > nodesPerIter) math.ceil(iter.toDouble / nodesPerIter).toInt
    else {
      nPI = iter; 1
    }

    for (i <- 1 to iters) {
      println(i + "/" + math.ceil(iter.toDouble / partitions).toLong)
      for (_ <- 1 to nPI.toInt) {
        //String is IP:Port ex. "192.168.0.1:80"
        val tempNodeProp: nodeData = if (withProperties) nodeData() else {
          val DATA = dataDist.getIpSample
          nodeData(DATA)
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

        val numEdgesToAdd = dataDist.getOutEdgeSample

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

          edgesToAdd = edgesToAdd :+ Edge(srcId, dstId, edgeData())

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
      nodeData()
    )
    theGraph
  }

  protected def genGraph(seed: Graph[nodeData, edgeData], seedDists : DataDistributions): Graph[nodeData, Int] = {
    println()
    println("Running BA with " + baIter + " iterations.")
    println()

    val synth = generateBAGraph(seed.vertices, seed.edges, baIter.toLong, nodesPerIter, withProperties = true)

    // TODO: the following should be removed, generateBAGraph() should return Graph[nodeData, Int]
    val edges = synth.edges.map(record => Edge(record.srcId, record.dstId, 1))
    Graph(synth.vertices, edges, nodeData())
  }

  def run(seedVertFile: String, seedEdgeFile: String, withProperties: Boolean): Boolean = {

    println()
    println("Loading seed graph with vertices file: " + seedVertFile + " and edges file " + seedEdgeFile + " ...")

    var startTime = System.nanoTime()
    //read in and parse vertices and edges
    val (inVertices, inEdges) = readFromSeedGraph(sc, partitions, seedVertFile,seedEdgeFile)
    var timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished loading seed graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println("\tVertices "+inVertices.count())
    println("\tEdges "+inEdges.count())
    println()
    println()
    println("Running BA with " + baIter + " iterations.")
    println()


    //Generate a BA Graph with iterations
    startTime = System.nanoTime()
    var theGraph = generateBAGraph(inVertices, inEdges, baIter.toLong, nodesPerIter, withProperties)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating BA graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    if (withProperties) {
//      theGraph = genProperties(sc, theGraph, dataDist)
    }

    println()
    println("Saving BA Graph and Veracity measurements.....")
    println()

    //Save the ba graph into a format to be read later
    startTime = System.nanoTime()
    graphPs.saveGraph(theGraph, overwrite = true)
    timeSpan = (System.nanoTime() - startTime) / 1e9

    println()
    println("Finished saving BA graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    val seedGraph = Graph(inVertices, inEdges, nodeData())

    val degVeracity = Degree(seedGraph, theGraph)
    val inDegVeracity = InDegree(seedGraph, theGraph)
    val outDegVeracity = OutDegree(seedGraph, theGraph)
    println("Finished calculating degrees veracity.\n\tDegree Veracity:" + degVeracity + "\n\tIn Degree Veracity: " +
      inDegVeracity + "\n\tOut Degree Veracity:" + outDegVeracity)

    true
  }
}
