import java.io.FileWriter
import java.util

import com.google.common.primitives.UnsignedInteger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph, VertexRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.util.Random

/**
  * Created by spencer on 11/3/16.
  */
class ba_GraphGen extends base_GraphGen {

  /***
    *
    * @return
    */
  def generateNodeData(): String = {
    val r = Random

    r.nextInt(255)+"."+r.nextInt(255)+"."+r.nextInt(255)+"."+r.nextInt(255)+":"+r.nextInt(65536)
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
    num
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
        /*
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
        */
      }
    }
    return null
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

    var theGraph = Graph(inVertices, inEdges, nodeData(""))

    //String is IP:Port ex. "192.168.0.1:80"
    var nodeIndices: HashMap[String, VertexId] = HashMap[String, VertexId]()
    var degList: Array[(VertexId,Int)] = theGraph.degrees.sortBy(_._1).collect()

    inVertices.foreach(record => nodeIndices += record._2.data -> record._1)

    var degSum: Long = degList.map(_._2).sum

    var edgesToAdd: Array[Edge[edgeData]] = Array.empty[Edge[edgeData]]
    var vertToAdd: Array[(VertexId, nodeData)] = Array.empty[(VertexId, nodeData)]

    for(i <- 1 to iter) {
      val tempNodeProp: nodeData = nodeData(generateNodeData())

      val srcId: VertexId =
        if (nodeIndices.get(tempNodeProp.data).isDefined)
          nodeIndices.get(tempNodeProp.data).head
        else
          degList.last._1 + 1

       var srcIndex =
        if (nodeIndices.get(tempNodeProp.data).isDefined)
          nodeIndices.get(tempNodeProp.data).head.toInt
        else
          degList.length

      if(degList.head._1 != 0L) {
        srcIndex -= 1
      }

      vertToAdd = vertToAdd :+ (srcId, tempNodeProp)
      degList = degList :+ (srcId, 0) //initial degree of 0

      val numEdgesToAdd = generateRandomFromFile("Edge_distributions")

      for (i <- 1 to numEdgesToAdd) {
        val attachTo: Long = (Math.abs(r.nextLong()) % (degSum-1)) + 1

        var dstIndex: Int = 0
        var tempDegSum: Long = 0
        while (tempDegSum < attachTo) {
          tempDegSum += degList(dstIndex)._2
          dstIndex+=1
        }

        dstIndex = dstIndex - 1
        //now we know that the node must attach at index
        val dstId: VertexId = degList(dstIndex)._1

        /*
        print("degSum = " + degSum.toString + " r = " + attachTo.toString + " degList = ")
        degList.sortBy(_._2).reverse.take(10).foreach(print)
        print(" Adding Edge from " + srcId + " to " + dstId)
        println()
        */

        val Orig_byte_count = generateRandomFromFile("Original_byte_count")
        val Orig_IP_byte_count = generateRandomFromFileConditional("Original_IP_byte_count",Orig_byte_count, sc)
        val connectState = generateRandomFromFileConditionalString("Connection_state", Orig_byte_count, sc)
        val connectType = generateRandomFromFileConditionalString("Connection_type", Orig_byte_count, sc)
        val Duration = generateRandomFromFileConditional("Duration_of_connection", Orig_byte_count, sc)
        val OriginalPackCount = generateRandomFromFileConditional("Original_packet_count", Orig_byte_count, sc)
        val RespByteCount = generateRandomFromFileConditional("Resp_byte_count", Orig_byte_count, sc)
        val ResIPByteCount = generateRandomFromFileConditional("Resp_IP_byte_count", Orig_byte_count, sc)
        val RespPackCount = generateRandomFromFileConditional("Resp_packet_count", Orig_byte_count, sc)
        val tempEdgeProp: edgeData = edgeData("",connectType,Orig_byte_count,RespByteCount,connectState,OriginalPackCount,Orig_IP_byte_count,RespPackCount,RespByteCount,"")
        edgesToAdd = edgesToAdd :+ Edge(srcId, dstId, tempEdgeProp)

        //This doesn't matter, but to be correct, this code updates the degList dstId's degree
        degList(dstIndex) = (degList(dstIndex)._1, degList(dstIndex)._2+1)
        degList(srcIndex) = (degList(srcIndex)._1, degList(srcIndex)._2+1)

        degSum += 2

      }
    }

    theGraph = Graph(inVertices.union(sc.parallelize(vertToAdd)), inEdges.union(sc.parallelize(edgesToAdd)), nodeData(""))
    theGraph
  }

  /*
  def runGen(sc: SparkContext): Unit = {
    val inVertices: RDD[(VertexId, nodeData)] = sc.parallelize(Array((1L, nodeData("")), (2L, nodeData("")), (3L, nodeData(""))))
    val inEdges: RDD[Edge[edgeData]] = sc.parallelize(Array(
      Edge(1L, 2L, edgeData("","",0,0,"",0,0,0,0,"")),
      Edge(1L, 3L, edgeData("","",0,0,"",0,0,0,0,""))
    ))

    generateBAGraph(sc, inVertices, inEdges, 500)
  }
  */


}
