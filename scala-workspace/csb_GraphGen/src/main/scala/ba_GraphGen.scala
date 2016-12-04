import java.io.FileWriter

import scala.io.Source
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, VertexRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by spencer on 11/3/16.
  */
class ba_GraphGen extends base_GraphGen {


  def returnRange(fileIter: Iterator[String]): String =
  {
    val r = new Random
    val numEdgesProb = r.nextFloat()
    var chance = 0.0
    var num = 0
    //    fileIter.next() //we do the next here since the heading is always just text describeing the file


    while(fileIter.hasNext && num == 0)
    {
      val line = fileIter.next()
      val percentage = line.split("\t")(1).toFloat
      chance = chance + percentage
      if(chance > numEdgesProb)
      {
        return line
        if(!line.split("\t")(0).contains("-"))
        {
          num = line.split("\t")(0).split("\\*")(1).toInt //split is a reg expression function so i escape the *
        }
        else
        {
          val firstTabLine = line.split("\t")(0)
          val begin: Int = firstTabLine.split("\\*")(1).split("-")(0).toInt
          val end: Int   = firstTabLine.split("\\*")(1).split("-")(1).toInt
          num = r.nextInt(end - begin) + begin


        }
      }
    }
    return null
  }

  def generateRandomFromFile(filename: String): Int =
  {
    val r = new Random
    val numEdgesProb = r.nextFloat()
    var chance = 0.0
    var num = 0
    var fileIter = Source.fromFile(filename).getLines()
//    fileIter.next() //we do the next here since the heading is always just text describeing the file
    while(fileIter.hasNext && num == 0)
    {
      val line = fileIter.next()
      val percentage = line.split("\t")(1).toFloat
      chance = chance + percentage
      if(chance > numEdgesProb)
      {
        if(!line.split("\t")(0).contains("-"))
          {
            num = line.split("\t")(0).split("\\*")(1).toInt //split is a reg expression function so i escape the *
          }
        else
          {
            val firstTabLine = line.split("\t")(0)
            val begin: Int = firstTabLine.split("\\*")(1).split("-")(0).toInt
            val end: Int   = firstTabLine.split("\\*")(1).split("-")(1).toInt
            num = r.nextInt(end - begin) + begin


          }
      }
    }
    return num;
  }
  def generateRandomFromFileConditional(filename: String, byteNum: Long, sc: SparkContext): Long =
  {
    val r = new Random
    val numEdgesProb = r.nextFloat()
    var chance = 0.0
    var num = 0
    var fileIter = Source.fromFile(filename).getLines()
//    fileIter.next() //we do the next here since the heading is always just text describeing the file


    var file = sc.textFile(filename)
    var text = file.filter(record => record.split("\\*")(0).split("-")(0).toLong <= byteNum && record.split("\\*")(0).split("-")(1).toLong >= byteNum)

    if(text.isEmpty())
      {
        println("bytecount " + byteNum + " produced nothing")
      }

    var allStr = ""

    for (aStr <- text.collect())
      {
        allStr = allStr + aStr + "\n"
      }
//    println(allStr)

    var temp = new FileWriter("temp")

    temp.write(allStr)
    temp.close()

    var line: String = returnRange(Source.fromFile("temp").getLines())

//    val randNum: Long = r.nextInt(text.count().toInt)

    val range: String= line.split("\t").head.split("\\*")(1)

    val begin: Int = range.split("-").head.toInt
    val end  : Int = range.split("-")(1).toInt

    return r.nextInt(end - begin) + begin
  }
  def generateRandomFromFileConditionalString(filename: String, byteNum: Long, sc: SparkContext): String =
  {
    val r = new Random
    val numEdgesProb = r.nextFloat()
    var chance = 0.0
    var num = 0
    var fileIter = Source.fromFile(filename).getLines()
    //    fileIter.next() //we do the next here since the heading is always just text describeing the file


    var file = sc.textFile(filename)
    var text = file.filter(record => record.split("\\*")(0).split("-")(0).toLong <= byteNum && record.split("\\*")(0).split("-")(1).toLong >= byteNum)

    if(text.isEmpty())
  {
    println("bytecount " + byteNum + " produced nothing")
  }


    var allStr = ""

    for (aStr <- text.collect())
    {
      allStr = allStr + aStr + "\n"
    }


    var temp = new FileWriter("temp")

    temp.write(allStr)
    temp.close()

    val line = returnRange(Source.fromFile("temp").getLines())

    return line.split("\t").head.split("\\*")(1)
  }

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
//
//      val numEdgesProb = r.nextFloat()
//      var chance = 0.0
//      var numEdges = 0
//      var fileIter = Source.fromFile("EdgeProbability").getLines()
//      while(fileIter.hasNext && numEdges == 0)
//        {
//          val line = fileIter.next()
//          val percentage = line.split(" ")(1).toFloat
//          chance = chance + percentage
//          if(chance > numEdgesProb)
//            {
//              numEdges = line.split(":")(0).toInt
//            }
//        }
      val numEdges = generateRandomFromFile("Edge_distributions")


      length = length+1
      val nodeID: Long = length.toLong
      println("we are attaching to " + numEdges.toString)
      val tempNodeData = nodeData("")
      vRDD = vRDD.union(sc.parallelize(Array((nodeID, tempNodeData))))
      for(x <- 1 to numEdges)
        {
          val Orig_byte_count = generateRandomFromFile("Original_byte_count")
          val Orig_IP_byte_count = generateRandomFromFileConditional("Original_IP_byte_count",Orig_byte_count, sc)
          val connectState = generateRandomFromFileConditionalString("Connection_state", Orig_byte_count, sc)
          val connectType = generateRandomFromFileConditionalString("Connection_type", Orig_byte_count, sc)
          val Duration = generateRandomFromFileConditional("Duration_of_connection", Orig_byte_count, sc)
          val OriginalPackCount = generateRandomFromFileConditional("Original_packet_count", Orig_byte_count, sc)
          val RespByteCount = generateRandomFromFileConditional("Resp_byte_count", Orig_byte_count, sc)
          val ResIPByteCount = generateRandomFromFileConditional("Resp_IP_byte_count", Orig_byte_count, sc)
          val RespPackCount = generateRandomFromFileConditional("Resp_packet_count", Orig_byte_count, sc)


          println("this line has " + Orig_byte_count.toString + " of data in it")
          println("The IP source packet had " + Orig_IP_byte_count + " of data in it")
          println("The connect state is " + connectState)

//          println("degSum is: " + degSum)
//          println("total num of nodes is " + vRDD.count())
//          println("total num of edges is " + eRDD.count())

          val attachTo = r.nextInt(degSum)

          //lookup that index number in the attachList
          val attachNode: Long = attachList.lookup(attachTo).head

          //TODO: distribution of edge properties with randomization

          val tempEdgeData = edgeData("",connectType,Orig_byte_count,RespByteCount,connectState,OriginalPackCount,Orig_IP_byte_count,RespPackCount,RespByteCount,"")


          println("the data is " + vRDD.lookup(attachNode).head.data)



          println("Adding Node " + nodeID.toString + " connected to Node " + attachNode.toString + " with P = " + attachTo.toString)

          eRDD = eRDD.union(sc.parallelize(Array(Edge(nodeID, attachNode, tempEdgeData))))
        }

//      theGraph = Graph(vRDD, eRDD, nodeData(""))
    }

    theGraph = Graph(vRDD, eRDD, nodeData(""))

    theGraph
  }

}