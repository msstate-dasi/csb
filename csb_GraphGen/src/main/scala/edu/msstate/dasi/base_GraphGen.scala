package edu.msstate.dasi

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
  def printGraph(): (RDD[String], VertexRDD[Int], RDD[(Int, Int)]) = {
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

//
//  /** * Saves the Graph's degree distribution, inDegree distribution, and outDegree distribution to the specified path
//   *
//   * @param sc   Current SparkContext
//   * @param path The folder path that Spark will output the files under
//   */
//  def saveGraphVeracity(sc: SparkContext, path: String): Unit = {
//    val degDist = Veracity.degreesDist(theGraph)
//    val inDegDist = Veracity.inDegreesDist(theGraph)
//    val outDegDist = Veracity.outDegreesDist(theGraph)
//
//    try {
//      degDist.coalesce(1).saveAsTextFile(path + "_veracity/degDist")
//      inDegDist.coalesce(1).saveAsTextFile(path + "_veracity/inDegDist")
//      outDegDist.coalesce(1).saveAsTextFile(path + "_veracity/outDegDist")
//    } catch {
//      case _: Exception => println("Couldn't save Veracity files")
//    }
//  }
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
      case _: Exception => println("Couldn't save file " + path)
    }
  }
}
