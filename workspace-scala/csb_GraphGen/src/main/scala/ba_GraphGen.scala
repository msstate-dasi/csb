import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, VertexRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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

    var theGraph = Graph(inVertices, inEdges, nodeData(""))

    var degList: Array[(VertexId,Int)] = theGraph.degrees.sortBy(_._1).collect()
    var degSum: Long = degList.map(_._2).sum

    var edgesToAdd: Array[Edge[edgeData]] = Array.empty[Edge[edgeData]]
    var vertToAdd: Array[(VertexId, nodeData)] = Array.empty[(VertexId, nodeData)]

    for(i <- 1 to iter) {
      val srcId: VertexId = degList.last._1 + 1
      val srcIndex = degList.length

      //TODO: Generate random node properties here
      val tempNodeProp: nodeData = nodeData("")

      vertToAdd = vertToAdd :+ (srcId, tempNodeProp)
      degList = degList :+ (srcId, 0) //initial degree of 0

      //TODO: Generate how many edges to attach from the new node
      val numEdgesToAdd = 1

      for (i <- 1 to numEdgesToAdd) {
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

        //TODO: Generate random edge properties here
        val tempEdgeProp: edgeData = edgeData("","",0,0,"",0,0,0,0,"")
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
