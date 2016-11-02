import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.Random

object csb_GraphGen{

  def main(args: Array[String]) {
    /*
    if (args.length < 3) {
      System.err.println("Usage: csb_GraphGen <input_file> <partitions>")
      System.exit(1)
    }
    */

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("csb_GraphGen")
    val sc = new SparkContext(conf)


    val r = Random

    var vertices = Array((1L,"Node 1"), (2L,"Node 2"), (3L,"Node 3"))
    var edges = Array(Edge(1L,2L,"1"), Edge(1L,3L,"1"))
    var vRDD: RDD[(VertexId, String)] = sc.parallelize(vertices)
    var eRDD: RDD[Edge[String]] = sc.parallelize(edges)

    var theGraph = Graph(vRDD, eRDD, "")

    val (facts: RDD[String], nodeDegs: VertexRDD[Int], degList: RDD[(Int,Int)]) = printGraph(theGraph)

    saveGraph(sc, theGraph, "Graph.step.0.csv")

    for(i <- 1 to 25) {
      val nodeDegs: VertexRDD[Int] = theGraph.degrees
      val degList: RDD[(Int, Int)] = nodeDegs.map(record => (record._2, 1)).reduceByKey(_ + _)
      val degSum: Int = degList.map(record => record._1 * record._2).sum().toInt
      val attachList: RDD[(Int, VertexId)] = nodeDegs.flatMap { record =>
        for {
          x <- 1 to record._2
        } yield record._1
      }.zipWithIndex().map(record => (record._2.toInt, record._1))
      val attachTo = r.nextInt(degSum)

      val tempArray = attachList.lookup(attachTo).toArray

      val attachNode = tempArray(0)
      val nodeID = (vertices.length+1).toLong

      println("Adding Node " + nodeID.toString + " connected to Node" + attachNode.toString + " with P = " + attachTo.toString)

      vertices = vertices :+ (nodeID, "Node " + nodeID.toString)
      edges = edges :+ Edge(nodeID, attachNode, "1")

      vRDD = sc.parallelize(vertices)
      eRDD = sc.parallelize(edges)
      theGraph = Graph(vRDD, eRDD, "")
      printGraph(theGraph)
      saveGraph(sc, theGraph, "Graph.step."+i.toString+".csv")

    }

    println("FINISHED!!!")

    System.exit(0)
  }

  def printGraph(theGraph: Graph[String, String]) = {
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

  def saveGraph(sc: SparkContext, theGraph: Graph[String, String], path: String): Unit = {
    val facts: RDD[String] = sc.parallelize(Array("Source,Target,Weight")).union(theGraph.triplets.map(record =>
      record.srcId + "," + record.dstId + "," + record.attr))
    try {
      facts.repartition(1).saveAsTextFile(path)
    } catch {
      case e: Exception => println("Couldn't save file " + path)
    }
  }
}