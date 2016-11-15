import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.Random



object csb_GraphGen{

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("csb_GraphGen")
    val sc = new SparkContext(conf)

    if (args.length < 3) {
      System.err.println("Usage: csb_GraphGen <seed_vertices_file> <seed_edges_file> [partitions]")
      System.exit(1)
    }

    val baGenerator = new ba_GraphGen()
    val kroGenerator = new kro_GraphGen()

    val vFile = sc.textFile(args(0))
    val eFile = sc.textFile(args(1))

    println()
    println("Loading seed graph with vertices file: " + args(0) + " and edges file " + args(1) + " ...")


    var startTime = System.nanoTime()
    val inVertices: RDD[(VertexId,nodeData)] = vFile.map(line => line.stripPrefix("(").stripSuffix(")").split(',')).map{record =>
      parseNodeData(record)
    }.filter(_._1 != false).map(record => record._2)

    val inEdges: RDD[Edge[edgeData]] = eFile.map(line => line.stripPrefix("Edge(").stripSuffix(")").split(',')).map{record =>
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

    startTime = System.nanoTime()
    val baGraph = baGenerator.generateBAGraph(sc, inVertices, inEdges, 100)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating BA graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    val n = inVertices.count().toInt
    var adjArr: Array[Array[Int]] = (for (x <- 1 to n) yield (for(y<-1 to n) yield 0).toArray).toArray

    inEdges.foreach(record =>
      adjArr(record.srcId.toInt)(record.dstId.toInt) = 0
    )

    val adjMtx: RDD[RDD[Int]] = sc.parallelize(adjArr).map(record => sc.parallelize(record))

    startTime = System.nanoTime()
    val kroGraph = kroGenerator.generateKroGraph(sc, adjMtx, 2)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating Kronecker graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    System.exit(0)
  }

  def parseNodeData(inData: Array[String]): (Boolean, (Long, nodeData)) = {
    val result: (Boolean, (VertexId, nodeData)) =
    try {
      (true,(inData(0).toLong, nodeData(inData(1).stripPrefix("nodeData(").stripSuffix(")"))))
    } catch {
      case _: Throwable => (false,(0L, nodeData("")))
    }

    result
  }

  def parseEdgeData(inData: Array[String]): (Boolean, Edge[edgeData]) = {
    val result: (Boolean, Edge[edgeData]) =
    try {
      val srcNode = inData(0).toLong
      val dstNode = inData(1).toLong

      val edgeStrs = inData(2).stripPrefix("edgeData(").stripSuffix(")").split(',')

      val TS: String = edgeStrs(0)
      val PROTOCOL: String = edgeStrs(1)
      val ORIG_BYTES: Int = edgeStrs(2).toInt
      val RESP_BYTES: Int = edgeStrs(3).toInt
      val CONN_STATE: String = edgeStrs(4)
      val ORIG_PKTS: Int = edgeStrs(5).toInt
      val ORIG_IP_BYTES: Int = edgeStrs(6).toInt
      val RESP_PKTS: Int = edgeStrs(7).toInt
      val RESP_IP_BYTES: Int = edgeStrs(8).toInt
      val DESC: String = edgeStrs(9)

      (true, Edge(srcNode, dstNode, edgeData(TS, PROTOCOL, ORIG_BYTES, RESP_BYTES, CONN_STATE, ORIG_PKTS, ORIG_IP_BYTES, RESP_PKTS, RESP_IP_BYTES, DESC)))
    } catch {
      case _: Throwable => (false, Edge(0L, 0L, edgeData("","", 0, 0, "", 0, 0, 0, 0, "")))
    }

    result
  }

}