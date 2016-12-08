package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.rdd.RDD

/***
  * Created by spencer on 11/3/16.
  *
  * base_GraphGen: Class containing inherited methods that both ba_GraphGen and edu.msstate.dasi.kro_GraphGen use
  */
trait base_GraphGen {

  var theGraph: Graph[nodeData, edgeData] = _

  /*** Print the graph to the terminal
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

    val degList: RDD[(Int, Int)] = nodeDegs.map(record => (record._2,1)).reduceByKey(_+_)
    //println("(Deg, Number of Nodes with Deg")
    //degList.foreach(println)
    //println()

    (facts, nodeDegs, degList)
  }

  /*** Saves the graph to the specified path
    *
    * @param sc Current SparkContext
    * @param path The folder name to save the graph files under
    */
  def saveGraph(sc: SparkContext, path: String): Unit = {
    val edgeFacts: RDD[String] = sc.parallelize(Array("Source,Target,Weight")).union(theGraph.triplets.map(record =>
      record.srcId + "," + record.dstId + "," + record.attr))
    val vertFacts: RDD[String] = sc.parallelize(Array("ID,Desc")).union(theGraph.vertices.map(record => record._1.toString + "," + record._2.toString))
    try {
      edgeFacts.repartition(1).saveAsTextFile(path)
      vertFacts.repartition(1).saveAsTextFile(path+"_vert")
    } catch {
      case e: Exception => println("Couldn't save file " + path)
    }
  }

  /*** Saves the Graph's degree distribution, inDegree distribution, and outDegree distribution to the specified path
    *
    * @param sc Current SparkContext
    * @param path The folder path that Spark will output the files under
    */
  def saveGraphVeracity(sc: SparkContext, path: String): Unit = {
    val degDist = sc.parallelize(Array("Degree,NumNodesWithDegree")).union(degreesDist(theGraph).sortBy(_._2, ascending = false).map(record => record._1.toString+','+record._2.toString))
    val inDegDist = sc.parallelize(Array("InDegree,NumNodesWithDegree")).union(inDegreesDist(theGraph).sortBy(_._2, ascending = false).map(record => record._1.toString+','+record._2.toString))
    val outDegDist = sc.parallelize(Array("OutDegree,NumNodesWithDegree")).union(outDegreesDist(theGraph).sortBy(_._2, ascending = false).map(record => record._1.toString+','+record._2.toString))

    try {
      degDist.repartition(1).saveAsTextFile(path+"_veracity/degDist")
      inDegDist.repartition(1).saveAsTextFile(path+"_veracity/inDegDist")
      outDegDist.repartition(1).saveAsTextFile(path+"_veracity/outDegDist")
    } catch {
      case e: Exception => println("Couldn't save Veracity files")
    }
  }

  /*** Function to generate the degree distribution
    *
    * @param theGraph The graph to analyze
    * @return RDD containing degree numbers, and the number of nodes at that specific degree
    */
  def degreesDist(theGraph: Graph[nodeData, edgeData]): RDD[(Int,Int)] = {
    theGraph.degrees.map(record => (record._2,1)).reduceByKey(_+_)
  }

  /*** Function to generate the degree distribution
    *
    * @param theGraph The graph to analyze
    * @return RDD containing degree numbers, and the number of nodes at that specific degree
    */
  def inDegreesDist(theGraph: Graph[nodeData, edgeData]): RDD[(Int,Int)] = {
    theGraph.inDegrees.map(record => (record._2,1)).reduceByKey(_+_)
  }

  /*** Function to generate outDegree distribution
    *
    * @param theGraph The graph to analyze
    * @return RDD containing degree numbers, and the number of nodes at that specific degree
    */
  def outDegreesDist(theGraph: Graph[nodeData, edgeData]): RDD[(Int,Int)] = {
    theGraph.outDegrees.map(record => (record._2,1)).reduceByKey(_+_)
  }
}
