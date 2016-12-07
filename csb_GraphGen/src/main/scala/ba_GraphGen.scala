import java.io.FileWriter
import java.util

import com.google.common.primitives.UnsignedInteger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, VertexRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source
import scala.util.Random

/**
  * Created by spencer on 11/3/16.
  */
class ba_GraphGen extends base_GraphGen {



  /***
    *
    * @param sc Current Sparkcontext
    * @param inVertices RDD of vertices and their nodeData
    * @param inEdges RDD of edges and their edgeData
    * @param iter Number of iterations to perform BA
    * @return Graph containing vertices + nodeData, edges + edgeData
    */
  def generateBAGraph(sc: SparkContext, inVertices: RDD[(VertexId, nodeData)], inEdges: RDD[Edge[edgeData]], iter: Int): Graph[nodeData,edgeData] = {
    val r = Random
    val dataGenerator = DataGenerator

    var theGraph = Graph(inVertices, inEdges, nodeData(""))

    //String is IP:Port ex. "192.168.0.1:80"
    println("test1")
    var nodeIndices: mutable.HashMap[String, VertexId] = new mutable.HashMap[String, VertexId]()
    println("test2")
    var degList: Array[(VertexId,Int)] = theGraph.degrees.sortBy(_._1).collect()

    inVertices.foreach(record => nodeIndices += record._2.data -> record._1)

    var degSum: Long = degList.map(_._2).sum

    var edgesToAdd: Array[Edge[edgeData]] = Array.empty[Edge[edgeData]]
    var vertToAdd: Array[(VertexId, nodeData)] = Array.empty[(VertexId, nodeData)]

    for(i <- 1 to iter) {
      val tempNodeProp: nodeData = nodeData(dataGenerator.generateNodeData())

      val srcId: VertexId =
        if (nodeIndices.get(tempNodeProp.data).isDefined)
          nodeIndices.get(tempNodeProp.data).head
        else
          degList.last._1 + 1

       var srcIndex =
        if (nodeIndices.get(tempNodeProp.data).isDefined)
          nodeIndices.get(tempNodeProp.data).head.toInt
        else
          degList.length

      if(degList.head._1 != 0L) {
        srcIndex -= 1
      }

      vertToAdd = vertToAdd :+ (srcId, tempNodeProp)
      degList = degList :+ (srcId, 0) //initial degree of 0


      val EDGESADD      = dataGenerator.getEdgeCount()
//      println("adding " + EDGESADD + " edges")


      for (i <- 1 to EDGESADD) {


        val ORIGBYTES     = dataGenerator.getOriginalByteCount()
        val ORIGIPBYTE    = dataGenerator.getOriginalIPByteCount(ORIGBYTES, sc)
        val CONNECTSTATE  = dataGenerator.getConnectState(ORIGBYTES, sc)
        val CONNECTTYPE   = dataGenerator.getConnectType(ORIGBYTES, sc)
        val DURATION      = dataGenerator.getDuration(ORIGBYTES, sc)
        val ORIGPACKCNT   = dataGenerator.getOriginalPackCnt(ORIGBYTES, sc)
        val RESPBYTECNT   = dataGenerator.getRespByteCnt(ORIGBYTES, sc)
        val RESPIPBYTECNT = dataGenerator.getRespIPByteCnt(ORIGBYTES, sc)
        val RESPPACKCNT   = dataGenerator.getRespPackCnt(ORIGBYTES, sc)

//        println("Original bytes: " + ORIGBYTES)
//        println("Original IP bytes " + ORIGIPBYTE)
//        println("Connection state: " + CONNECTSTATE)
//        println("protocal: " + CONNECTTYPE)
//        println("Duration: " + DURATION)
//        println("original Packet count: " + ORIGPACKCNT)
//        println("Response Byte count: " + RESPBYTECNT)
//        println("Response IP byte count: " + RESPIPBYTECNT)
//        println("Response Packet Count: " + RESPPACKCNT)
//        println("\n\n")
        val attachTo: Long = (Math.abs(r.nextLong()) % (degSum-1)) + 1

        var dstIndex: Int = 0
        var tempDegSum: Long = 0
        while (tempDegSum < attachTo) {
          tempDegSum += degList(dstIndex)._2
          dstIndex+=1
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

//        val Orig_byte_count = generateRandomFromFile("Original_byte_count")
//        val Orig_IP_byte_count = generateRandomFromFileConditional("Original_IP_byte_count",Orig_byte_count, sc)
//        val connectState = generateRandomFromFileConditionalString("Connection_state", Orig_byte_count, sc)
//        val connectType = generateRandomFromFileConditionalString("Connection_type", Orig_byte_count, sc)
//        val Duration = generateRandomFromFileConditional("Duration_of_connection", Orig_byte_count, sc)
//        val OriginalPackCount = generateRandomFromFileConditional("Original_packet_count", Orig_byte_count, sc)
//        val RespByteCount = generateRandomFromFileConditional("Resp_byte_count", Orig_byte_count, sc)
//        val ResIPByteCount = generateRandomFromFileConditional("Resp_IP_byte_count", Orig_byte_count, sc)
//        val RespPackCount = generateRandomFromFileConditional("Resp_packet_count", Orig_byte_count, sc)
        val tempEdgeProp: edgeData = edgeData("", CONNECTTYPE, DURATION, ORIGBYTES,RESPBYTECNT,CONNECTSTATE,ORIGPACKCNT,ORIGIPBYTE,RESPPACKCNT,RESPBYTECNT,"")
//        val tempEdgeProp = edgeData("", "", 0L, 0L, "", 0L, 0L, 0L, 0L, "")
        edgesToAdd = edgesToAdd :+ Edge(srcId, dstId, tempEdgeProp)

        //This doesn't matter, but to be correct, this code updates the degList dstId's degree
        degList(dstIndex) = (degList(dstIndex)._1, degList(dstIndex)._2+1)
        degList(srcIndex) = (degList(srcIndex)._1, degList(srcIndex)._2+1)

        degSum += 2

      }
    }

    theGraph = Graph(inVertices.union(sc.parallelize(vertToAdd)), inEdges.union(sc.parallelize(edgesToAdd)), nodeData(""))
    theGraph
  }

  /*
  def runGen(sc: SparkContext): Unit = {
    val inVertices: RDD[(VertexId, nodeData)] = sc.parallelize(Array((1L, nodeData("")), (2L, nodeData("")), (3L, nodeData(""))))
    val inEdges: RDD[Edge[edgeData]] = sc.parallelize(Array(
      Edge(1L, 2L, edgeData("","",0,0,"",0,0,0,0,"")),
      Edge(1L, 3L, edgeData("","",0,0,"",0,0,0,0,""))
    ))

    generateBAGraph(sc, inVertices, inEdges, 500)
  }
  */


}
