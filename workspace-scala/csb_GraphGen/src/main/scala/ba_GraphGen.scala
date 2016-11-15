import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, VertexRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by spencer on 11/3/16.
  */
class ba_GraphGen extends base_GraphGen {
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.graphx.{Graph, VertexRDD, _}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  import scala.util.Random
  def generateBAGraph(sc: SparkContext, inVertices: RDD[(VertexId, nodeData)], inEdges: RDD[Edge[edgeData]], iter: Int): Graph[nodeData,edgeData] = {
    val r = Random

    var vRDD: RDD[(VertexId, nodeData)] = inVertices
    var eRDD: RDD[Edge[edgeData]] = inEdges

    var theGraph = Graph(vRDD, eRDD, nodeData(""))

    //val (facts: RDD[String], nodeDegs: VertexRDD[Int], degList: RDD[(Int,Int)]) = printGraph(theGraph)

    var length = vRDD.count()

    //saveGraph(sc, theGraph, "BA_Graph.step.0.csv")

    for(i <- 1 to iter) {
      val nodeDegs: VertexRDD[Int] = theGraph.degrees // returns degree for each node

      // reduces the list down to unique degrees, and adds up the number of nodes at that degree
      val degList: RDD[(Int, Int)] = nodeDegs.map(record => (record._2, 1)).reduceByKey(_ + _)

      // Calculates the total degree of the whole graph
      val degSum: Int = degList.map(record => record._1 * record._2).sum().toInt

      //(5L,12)

      //1 to 12, return 5L each time
      //5L 5L 5L ..
      //zipWithIndex
      //5L,1 5L,2, 5L,3 ...
      //map
      //1,5L 2,5L

      val attachList: RDD[(Int, VertexId)] = nodeDegs.flatMap { record =>
        for {
          x <- 1 to record._2
        } yield record._1
      }.zipWithIndex().map(record => (record._2.toInt, record._1))

      //grab a random number from 1 to Kf, degSum

      //TODO: distribution of how many nodes to attach to

      println(degSum)
      val attachTo = r.nextInt(degSum)

      //lookup that index number in the attachList
      val attachNode: Long = attachList.lookup(attachTo).head

      //TODO: distribution of edge properties with randomization

      val tempEdgeData = edgeData("","",0,0,"",0,0,0,0,"")
      val tempNodeData = nodeData("")

      length = length+1
      val nodeID: Long = length.toLong

      //println("Adding Node " + nodeID.toString + " connected to Node" + attachNode.toString + " with P = " + attachTo.toString)

      vRDD = vRDD.union(sc.parallelize(Array((nodeID, tempNodeData))))
      eRDD = eRDD.union(sc.parallelize(Array(Edge(nodeID, attachNode, tempEdgeData))))

    }

    theGraph = Graph(vRDD, eRDD, nodeData(""))

    theGraph
  }

}
