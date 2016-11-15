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

    /*
    if (args.length < 3) {
      System.err.println("Usage: csb_GraphGen <input_file> <partitions>")
      System.exit(1)
    }
    */


    val baGenerator = new ba_GraphGen()

    val inVertices: RDD[(VertexId, nodeData)] = sc.parallelize(Array((1L, nodeData("Node 1")),(2L, nodeData("Node 2")),(3L, nodeData("Node 3"))))
    val inEdges: RDD[Edge[edgeData]] = sc.parallelize(Array(Edge(1L,2L,edgeData("","",0,0,"",0,0,0,0,"")),Edge(1L,3L,edgeData("","",0,0,"",0,0,0,0,""))))
    var theGraph = baGenerator.generateBAGraph(sc, inVertices, inEdges, 100000)


    baGenerator.printGraph(theGraph)

    System.exit(0)
  }
}