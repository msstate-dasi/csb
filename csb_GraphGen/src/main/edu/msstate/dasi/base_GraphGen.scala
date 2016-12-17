package edu.msstate.dasi

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.rdd.RDD

/** *
  * Created by spencer on 11/3/16.
  *
  * base_GraphGen: Class containing inherited methods that both ba_GraphGen and edu.msstate.dasi.kro_GraphGen use
  */
trait base_GraphGen {

  var theGraph: Graph[nodeData, edgeData] = _

  /** * Print the graph to the terminal
    *
    * @return Tuple containing the Facts, degrees, and degree list of the graph
    */
  def printGraph() = {
    val facts: RDD[String] = theGraph.triplets.map(record =>
      record.srcId + " " + record.dstId + " " + record.attr)
    println("Graph:")
    facts.foreach(println)
    println()

    val nodeDegs: VertexRDD[Int] = theGraph.degrees
    //println("(ID, Degree):")
    //nodeDegs.foreach(println)
    //println()

    val degList: RDD[(Int, Int)] = nodeDegs.map(record => (record._2, 1)).reduceByKey(_ + _)
    //println("(Deg, Number of Nodes with Deg")
    //degList.foreach(println)
    //println()

    (facts, nodeDegs, degList)
  }

  /** * Saves the graph to the specified path
    *
    * @param sc   Current SparkContext
    * @param path The folder name to save the graph files under
    */
  def saveGraph(sc: SparkContext, path: String): Unit = {
    saveGraphEdges(sc, path)
    saveGraphVerts(sc, path + "_vert")
  }

  def saveGraphEdges(sc: SparkContext, path: String): Unit = {
    val edgeFacts: RDD[String] = sc.parallelize(Array("Source,Target,Weight")).union(theGraph.triplets.map(record =>
      record.srcId + "," + record.dstId + "," + record.attr))
    try {
      edgeFacts.repartition(1).saveAsTextFile(path)
    } catch {
      case e: Exception => println("Couldn't save file " + path)
    }
  }

  def saveGraphVerts(sc: SparkContext, path: String): Unit = {
    val vertFacts: RDD[String] = sc.parallelize(Array("ID,Desc")).union(theGraph.vertices.map(record => record._1.toString + "," + record._2.toString))
    try {
      vertFacts.repartition(1).saveAsTextFile(path)
    } catch {
      case e: Exception => println("Couldn't save file " + path)
    }
  }

  /** * Saves the Graph's degree distribution, inDegree distribution, and outDegree distribution to the specified path
    *
    * @param sc   Current SparkContext
    * @param path The folder path that Spark will output the files under
    */
  def saveGraphVeracity(sc: SparkContext, path: String): Unit = {
//    val degDist = sc.parallelize(Array("Degree,NumNodesWithDegree")).union(degreesDist(theGraph).sortBy(_._2, ascending = false).map(record => record._1.toString + ',' + record._2.toString))
//    val inDegDist = sc.parallelize(Array("InDegree,NumNodesWithDegree")).union(inDegreesDist(theGraph).sortBy(_._2, ascending = false).map(record => record._1.toString + ',' + record._2.toString))
//    val outDegDist = sc.parallelize(Array("OutDegree,NumNodesWithDegree")).union(outDegreesDist(theGraph).sortBy(_._2, ascending = false).map(record => record._1.toString + ',' + record._2.toString))


    val degDist = degreesDist(theGraph).sortBy(_._2, ascending = false).map(record => record._1.toString + ',' + record._2.toString)
    val inDegDist = inDegreesDist(theGraph).sortBy(_._2, ascending = false).map(record => record._1.toString + ',' + record._2.toString)
    val outDegDist = outDegreesDist(theGraph).sortBy(_._2, ascending = false).map(record => record._1.toString + ',' + record._2.toString)

    try {
      degDist.coalesce(1).saveAsTextFile(path + "_veracity/degDist")
      inDegDist.coalesce(1).saveAsTextFile(path + "_veracity/inDegDist")
      outDegDist.coalesce(1).saveAsTextFile(path + "_veracity/outDegDist")


//      degDist.repartition(1).saveAsTextFile(path + "_veracity/degDist")
//      inDegDist.repartition(1).saveAsTextFile(path + "_veracity/inDegDist")
//      outDegDist.repartition(1).saveAsTextFile(path + "_veracity/outDegDist")
    } catch {
      case e: Exception => println("Couldn't save Veracity files")
    }
  }

  /** * Function to generate the degree distribution
    *
    * @param theGraph The graph to analyze
    * @return RDD containing degree numbers, and the number of nodes at that specific degree
    */
  def degreesDist(theGraph: Graph[nodeData, edgeData]): RDD[(Int, Int)] = {
    theGraph.degrees.map(record => (record._2, 1)).reduceByKey(_ + _)
  }

  /** * Function to generate the degree distribution
    *
    * @param theGraph The graph to analyze
    * @return RDD containing degree numbers, and the number of nodes at that specific degree
    */
  def inDegreesDist(theGraph: Graph[nodeData, edgeData]): RDD[(Int, Int)] = {
    theGraph.inDegrees.map(record => (record._2, 1)).reduceByKey(_ + _)
  }

  /** * Function to generate outDegree distribution
    *
    * @param theGraph The graph to analyze
    * @return RDD containing degree numbers, and the number of nodes at that specific degree
    */
  def outDegreesDist(theGraph: Graph[nodeData, edgeData]): RDD[(Int, Int)] = {
    theGraph.outDegrees.map(record => (record._2, 1)).reduceByKey(_ + _)
  }

  def saveGraphAsConnFile(sc: SparkContext, theGraph: Graph[nodeData, edgeData], path: String): Unit = {
    val eRDD = theGraph.edges
    val vRDD = theGraph.vertices

    val verticesArr = vRDD.map(record => record._2).collect()

    val connRDD: RDD[String] = eRDD.sortBy(record => record.attr.TS.toDouble).map { record =>


      //THESE TWO LINES CANNOT BE RAN
      //SPARK CANNOT DO TRANSFORMATIONS INSIDE OF TRANSFORMATIONS
      //      val srcNodeData = vRDD.lookup(record.srcId).head
      //      val dstNodeData = vRDD.lookup(record.dstId).head

      //YOU HAVE TO USE AN ARRAY

      val srcNodeData = verticesArr(record.srcId.toInt)
      val dstNodeData = verticesArr(record.dstId.toInt)


      val edgeData = record.attr

      edgeData.TS + "\t" +
        "asdfasdfasdfasdf" + "\t" +
        srcNodeData.data.split(":")(0) + "\t" +
        srcNodeData.data.split(":")(1) + "\t" +
        dstNodeData.data.split(":")(0) + "\t" +
        dstNodeData.data.split(":")(1) + "\t" +
        edgeData.PROTOCOL + "\t" +
        "-" + "\t" +
        edgeData.DURATION + "\t" +
        edgeData.ORIG_BYTES + "\t" +
        edgeData.RESP_BYTES + "\t" +
        edgeData.CONN_STATE + "\t" +
        "-" + "\t" +
        "-" + "\t" +
        "0" + "\t" +
        "F" + "\t" +
        edgeData.ORIG_PKTS + "\t" +
        edgeData.ORIG_IP_BYTES + "\t" +
        edgeData.RESP_PKTS + "\t" +
        edgeData.RESP_IP_BYTES + "\t" +
        "(empty)" + "\t" +
        edgeData.DESC
    }

    try {
      connRDD.repartition(1).saveAsTextFile(path)
    } catch {
      case e: Exception => println("Couldn't save file " + path)
    }
  }

}
