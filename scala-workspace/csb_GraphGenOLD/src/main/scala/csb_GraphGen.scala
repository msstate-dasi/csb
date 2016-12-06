import java.lang.Character

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

import scala.util.Random
import scala.io.Source



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

    val filename = "conn.log"
    val file = sc.textFile(filename)

    //I get a list of all the lines of the conn.log file in a way for easy parsing
    val lines = file.map(line => line.split("\n")).filter(line => !line(0).contains("#")).map(line => line(0).replaceAll("-","0"))


    //Next I get each line in a list of the dedge that line in conn.log represents and the vertices that make that edge up
    //NOTE: There will be many copies of the vertices which will be reduced later
    var connPLUSnodes = lines.map(line => (new edgeData(line.split("\t")(0), line.split("\t")(6),  line.split("\t")(9).toLong, line.split("\t")(10).toInt,
      line.split("\t")(11), line.split("\t")(12).toInt, line.split("\t")(17).toInt, line.split("\t")(18).toInt, line.split("\t")(19).toInt,
      ""),
      new nodeData(line.split("\t")(2) + ":" + line.split("\t")(3)),
      new nodeData(line.split("\t")(4) + ":" + line.split("\t")(5))))


    //from connPLUSnodes lets grab all the DISTINCT nodes
    var ALLNODES : RDD[nodeData] = connPLUSnodes.map(record => record._2).union(connPLUSnodes.map(record => record._3)).distinct()

    //next lets give them numbers and let that number be the "key"(basically index for my use)
    var vertices: RDD[(VertexId, nodeData)] = ALLNODES.zipWithIndex().map(record => (record._2, record._1))


    //next I make a hashtable of the nodes with it's given index.
    //I have to do this since RDD transformations cannot happen within
    //other RDD's and hashtables have O(1)
    var verticesList = ALLNODES.collect()
    var hashTable = new scala.collection.mutable.HashMap[nodeData, graphx.VertexId]
    for( x<-0 to verticesList.length - 1)
    {
        hashTable.put(verticesList(x), x.toLong)
    }



    //Next I generate the edge list with the vertices represented by indexes(as it wants it)
    var Edges: RDD[Edge[edgeData]] = connPLUSnodes.map(record => Edge[edgeData](hashTable.get(record._2).head, hashTable.get(record._3).head, record._1))


    val baGenerator = new ba_GraphGen()
    baGenerator.generateBAGraph(sc, vertices, Edges, 10)

    System.exit(0)
  }
}