import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * Created by spencer on 11/3/16.
  */

object csb_GraphGen{


  /*** Main function of our program, controls graph generation and other pieces.
    *
    * @param args array of command line arguments
    */
  def main(args: Array[String]) {
    //turn off annoying log messages
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //every spark application needs a configuration and sparkcontext
    val conf = new SparkConf().setAppName("csb_GraphGen")
    val sc = new SparkContext(conf)

    // Command line arguments
    if (args.length < 5) {
      System.err.println("Usage: csb_GraphGen <seed_vertices_file> <seed_edges_file> <BA_iterations> <Kro_iterations> [partitions]")
      System.exit(1)
    }


    //Initialize an instance of each type of generator
    val baGenerator = new ba_GraphGen()
    val kroGenerator = new kro_GraphGen()

    //Open the seed files for vertices and edges
    val vFile = sc.textFile(args(0))
    val eFile = sc.textFile(args(1))

    println()
    println("Loading seed graph with vertices file: " + args(0) + " and edges file " + args(1) + " ...")

    //read in and parse vertices and edges
    var startTime = System.nanoTime()
    val inVertices: RDD[(VertexId,nodeData)] = vFile.map(line => line.stripPrefix("(").stripSuffix(")").split(',')).map{record =>
      parseNodeData(record)
    }.filter(_._1 != false).map(record => record._2)

    val inEdges: RDD[Edge[edgeData]] = eFile.map(line => line.stripPrefix("Edge(").stripSuffix(")").split(",",3)).map{record =>
      parseEdgeData(record)
    }.filter(_._1 != false).map(record => record._2)

    var timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished loading seed graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println("\tVertices "+inVertices.count())
    println("\tEdges "+inEdges.count())
    println()

    //val inVertices: RDD[(VertexId, nodeData)] = sc.parallelize(Array((1L, nodeData("Node 1")),(2L, nodeData("Node 2")),(3L, nodeData("Node 3"))))
    //val inEdges: RDD[Edge[edgeData]] = sc.parallelize(Array(Edge(1L,2L,edgeData("","",0,0,"",0,0,0,0,"")),Edge(1L,3L,edgeData("","",0,0,"",0,0,0,0,""))))

    println()
    println("Running BA with " + args(2) + " iterations.")
    println()


    //Generate a BA Graph with iterations
    startTime = System.nanoTime()
    val baGraph = baGenerator.generateBAGraph(sc, inVertices, inEdges, args(2).toInt)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating BA graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    println()
    println("Saving BA Graph.....")
    println()

    //Save the ba graph into a format to be read later
    startTime = System.nanoTime()
    baGenerator.saveGraph(sc, baGraph, "ba_"+args(2))
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished saving BA graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    //Convert edge list to a Zero adjacency matrix
    val n = inVertices.count().toInt
    var adjArr: Array[Array[Int]] = (for (x <- 1 to n) yield (for(y<-1 to n) yield 0).toArray).toArray

    //Replace the spots where a side exists with 1
    inEdges.foreach(record =>
      adjArr(record.srcId.toInt)(record.dstId.toInt) = 0
    )
    //Make it parallel
    val adjMtx: RDD[RDD[Int]] = sc.parallelize(adjArr).map(record => sc.parallelize(record))

    println()
    println("Running Kronecker with " + args(3) + " iterations.")
    println()

    //Run Kronecker with the adjacency matrix
    startTime = System.nanoTime()
    val kroGraph = kroGenerator.generateKroGraph(sc, adjMtx, args(3).toInt)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating Kronecker graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    System.exit(0)
  }

  /*** Parses data for a node out of an array of strings
    *
    * @param inData Array strings to parse as node data. First element is the ID of the node, second element is the description of the node
    * @return Tuple (bool whether the record was successfully parsed, record(VertexID, nodeData))
    */
  def parseNodeData(inData: Array[String]): (Boolean, (Long, nodeData)) = {
    val result: (Boolean, (VertexId, nodeData)) =
    try {
      (true,(inData(0).toLong, nodeData(inData(1).stripPrefix("nodeData(").stripSuffix(")"))))
    } catch {
      case _: Throwable => (false,(0L, nodeData("")))
    }

    result
  }

  /*** Parses Data for an edge out of an array of strings
    *
    * @param inData Array of strings to parse as edge data. First element is source ID, second element is destination ID, third element is the edge data
    * @return (bool whether the record was successfully parsed, record(VertexID, VertexID, edgeData))
    */
  def parseEdgeData(inData: Array[String]): (Boolean, Edge[edgeData]) = {
    val result: (Boolean, Edge[edgeData]) =
    try {
      val srcNode = inData(0).toLong
      val dstNode = inData(1).toLong

      //Just a bunch of string formatting and splitting
      val edgeStrs = inData(2).stripPrefix("edgeData(").stripSuffix(")").split(',')
      println(inData(2).stripPrefix("edgeData(").stripSuffix(")"))

      val TS: String = edgeStrs(0)
      println("TS: \"" + TS + "\"")
      val PROTOCOL: String = edgeStrs(1)
      println("PROTOCOL: \"" + PROTOCOL + "\"")
      val ORIG_BYTES: Int = edgeStrs(2).toInt
      println("ORIG_BYTES: \"" + ORIG_BYTES + "\"")
      val RESP_BYTES: Int = edgeStrs(3).toInt
      println("RESP_BYTES: \"" + RESP_BYTES + "\"")
      val CONN_STATE: String = edgeStrs(4)
      println("CONN_STATE: \"" + CONN_STATE + "\"")
      val ORIG_PKTS: Int = edgeStrs(5).toInt
      println("ORIG_PKTS: \"" + ORIG_PKTS + "\"")
      val ORIG_IP_BYTES: Int = edgeStrs(6).toInt
      println("ORIG_IP_BYTES: \"" + ORIG_IP_BYTES + "\"")
      val RESP_PKTS: Int = edgeStrs(7).toInt
      println("RESP_PKTS: \"" + RESP_PKTS + "\"")
      val RESP_IP_BYTES: Int = edgeStrs(8).toInt
      println("RESP_IP_BYTES: \"" + RESP_IP_BYTES + "\"")

      val DESC: String = if (edgeStrs.length > 8) edgeStrs(0) else ""

      println("DESC: \"" + DESC + "\"")
      println()

      (true, Edge(srcNode, dstNode, edgeData(TS, PROTOCOL, ORIG_BYTES, RESP_BYTES, CONN_STATE, ORIG_PKTS, ORIG_IP_BYTES, RESP_PKTS, RESP_IP_BYTES, DESC)))
    } catch {
      case _: Throwable => (false, Edge(0L, 0L, edgeData("","", 0, 0, "", 0, 0, 0, 0, "")))
    }

    //return
    result
  }

}