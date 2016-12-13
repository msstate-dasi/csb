package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random

/**
  * Created by spencer on 11/3/16.
  */
class ba_GraphGen extends base_GraphGen with data_Parser {

  def run(sc: SparkContext, partitions: Int, seedVertFile: String, seedEdgeFile: String, baIter: Int, outputGraphPrefix: String, nodesPerIter: Int, noPropFlag: Boolean, debugFlag: Boolean): Boolean = {
    val dataGen = new data_Generator()

    println()
    println("Loading seed graph with vertices file: " + seedVertFile + " and edges file " + seedEdgeFile + " ...")

    //read in and parse vertices and edges
    var startTime = System.nanoTime()
    val (inVertices, inEdges) = readFromConnFile(sc, seedVertFile,seedEdgeFile)
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
    theGraph = generateBAGraph(sc, partitions, inVertices, inEdges, baIter.toInt, nodesPerIter, noPropFlag, debugFlag)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating BA graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    if(!noPropFlag) {
      println()
      println("Generating Edge and Node properties")
      startTime = System.nanoTime()
      val eRDD: RDD[Edge[edgeData]] = dataGen.generateEdgeProperties(sc, theGraph.edges)
      val vRDD: RDD[(VertexId, nodeData)] = dataGen.generateNodeProperties(sc, theGraph.vertices)
      theGraph = Graph(vRDD, eRDD, nodeData())
      timeSpan = (System.nanoTime() - startTime) / 1e9
      println()
      println("Finished generating Edge and Node Properties.")
      println("\tTotal time elapsed: " + timeSpan.toString)
      println()
    }


    println()
    println("Saving BA Graph and Veracity measurements.....")
    println()

    //Save the ba graph into a format to be read later
    startTime = System.nanoTime()
    saveGraph(sc, outputGraphPrefix + "_ba_" + baIter)
    saveGraphVeracity(sc, outputGraphPrefix + "_ba_" + baIter)
    timeSpan = (System.nanoTime() - startTime) / 1e9

    println()
    println("Finished saving BA graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    true
  }

  /***
    *
    * @param sc Current Sparkcontext
    * @param inVertices RDD of vertices and their edu.msstate.dasi.nodeData
    * @param inEdges RDD of edges and their edu.msstate.dasi.edgeData
    * @param iter Number of iterations to perform BA
    * @return Graph containing vertices + edu.msstate.dasi.nodeData, edges + edu.msstate.dasi.edgeData
    */
  def generateBAGraph(sc: SparkContext, partitions: Int, inVertices: RDD[(VertexId, nodeData)], inEdges: RDD[Edge[edgeData]], iter: Int, nodesPerIter: Int, noPropFlag: Boolean, debugFlag: Boolean): Graph[nodeData,edgeData] = {
    val r = Random
    val dataGen = new data_Generator()

    theGraph = Graph(inVertices, inEdges, nodeData(""))

    var nodeIndices: mutable.HashMap[String, VertexId] = new mutable.HashMap[String, VertexId]()
    var degList: Array[(VertexId,Int)] = theGraph.degrees.sortBy(_._1).collect()

    inVertices.foreach(record => nodeIndices += record._2.data -> record._1)

    var degSum: Long = degList.map(_._2).sum

    var edgesToAdd: Array[Edge[edgeData]] = Array.empty[Edge[edgeData]]
    var vertToAdd: Array[(VertexId, nodeData)] = Array.empty[(VertexId, nodeData)]

    var nPI = nodesPerIter

    val iters: Int = if(iter > nodesPerIter) math.ceil(iter.toDouble / nodesPerIter).toInt else { nPI = iter; 1}

    for(i <- 1 to iters) {
      println(i + "/" + math.ceil(iter.toDouble / partitions).toInt)
      for(n <- 1 to nPI) {
        //String is IP:Port ex. "192.168.0.1:80"
        val tempNodeProp: nodeData = if (noPropFlag) nodeData() else nodeData(dataGen.generateNodeData())
        val srcId: VertexId =
          if (nodeIndices.contains(tempNodeProp.data))
            nodeIndices.get(tempNodeProp.data).head
          else
            degList.last._1.toLong + 1
        var srcIndex =
          if (nodeIndices.contains(tempNodeProp.data))
            nodeIndices.get(tempNodeProp.data).head.toInt
          else
            degList.length
        if (degList.head._1 != 0L) {
          srcIndex -= 1
        }

        vertToAdd = vertToAdd :+ (srcId, tempNodeProp)
        degList = degList :+ (srcId, 0) //initial degree of 0

        val numEdgesToAdd = dataGen.getEdgeCount()

        for (i <- 1 to numEdgesToAdd) {
          val attachTo: Long = (Math.abs(r.nextLong()) % (degSum - 1)) + 1

          var dstIndex: Int = 0
          var tempDegSum: Long = 0
          while (tempDegSum < attachTo) {
            tempDegSum += degList(dstIndex)._2
            dstIndex += 1
          }

          dstIndex = dstIndex - 1
          //now we know that the node must attach at index
          val dstId: VertexId = degList(dstIndex)._1

          /*
        print("degSum = " + degSum.toString + " r = " + attachTo.toString + " degList = ")
        degList.sortBy(_._2).reverse.take(10).foreach(print)
        print(" Adding Edge from " + srcId + " to " + dstId)
        println()
        */

          val tempEdgeProp = if (noPropFlag) {
            edgeData()
          } else {
            val ORIGBYTES = dataGen.getOriginalByteCount()
            val ORIGIPBYTE = dataGen.getOriginalIPByteCount(ORIGBYTES, sc)
            val CONNECTSTATE = dataGen.getConnectState(ORIGBYTES, sc)
            val CONNECTTYPE = dataGen.getConnectType(ORIGBYTES, sc)
            val DURATION = dataGen.getDuration(ORIGBYTES, sc)
            val ORIGPACKCNT = dataGen.getOriginalPackCnt(ORIGBYTES, sc)
            val RESPBYTECNT = dataGen.getRespByteCnt(ORIGBYTES, sc)
            val RESPIPBYTECNT = dataGen.getRespIPByteCnt(ORIGBYTES, sc)
            val RESPPACKCNT = dataGen.getRespPackCnt(ORIGBYTES, sc)
            edgeData("", CONNECTTYPE, DURATION, ORIGBYTES, RESPBYTECNT, CONNECTSTATE, ORIGPACKCNT, ORIGIPBYTE, RESPPACKCNT, RESPBYTECNT, "")
            //val tempEdgeProp: edgeData = edgeData()
          }
          edgesToAdd = edgesToAdd :+ Edge(srcId, dstId, tempEdgeProp)

          //This doesn't matter, but to be correct, this code updates the degList dstId's degree
          degList(dstIndex) = (degList(dstIndex)._1, degList(dstIndex)._2 + 1)
          degList(srcIndex) = (degList(srcIndex)._1, degList(srcIndex)._2 + 1)

          degSum += 2

        }
      }
      Array(true)
    }

    theGraph = Graph(inVertices.union(sc.parallelize(vertToAdd)), inEdges.union(sc.parallelize(edgesToAdd)), nodeData(""))
    theGraph
  }





  /*
  def runGen(sc: SparkContext): Unit = {
    val inVertices: RDD[(VertexId, edu.msstate.dasi.nodeData)] = sc.parallelize(Array((1L, edu.msstate.dasi.nodeData("")), (2L, edu.msstate.dasi.nodeData("")), (3L, edu.msstate.dasi.nodeData(""))))
    val inEdges: RDD[Edge[edu.msstate.dasi.edgeData]] = sc.parallelize(Array(
      Edge(1L, 2L, edu.msstate.dasi.edgeData("","",0,0,"",0,0,0,0,"")),
      Edge(1L, 3L, edu.msstate.dasi.edgeData("","",0,0,"",0,0,0,0,""))
    ))

    generateBAGraph(sc, inVertices, inEdges, 500)
  }
  */


}
