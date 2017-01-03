package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random


/***
  * Created by spencer on 11/3/16.
  *
  * edu.msstate.dasi.kro_GraphGen: Kronecker based Graph generation given seed matrix.
  */
class kro_GraphGen extends base_GraphGen with data_Parser {
  def run(sc: SparkContext, partitions: Int, mtxFile: String, genIter: Int, outputGraphPrefix: String, noPropFlag: Boolean, debugFlag: Boolean, sparkSession: SparkSession): Boolean = {

    //val probMtx: Array[Array[Float]] = Array(Array(0.1f, 0.9f), Array(0.9f, 0.5f))
    val probMtx: Array[Array[Double]] = parseMtxDataFromFile(sc, mtxFile)

    println()
    print("Matrix: ")
    probMtx.foreach(_.foreach(record => print(record + " ")))
    println()
    println()

    println()
    println("Running Kronecker with " + genIter + " iterations.")
    println()

    //Run Kronecker with the adjacency matrix
    var startTime = System.nanoTime()
    theGraph = generateKroGraph(sc, partitions, probMtx, genIter.toInt)
    var timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating Kronecker graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()


    if(!noPropFlag) {
      println()
      println("Generating Edge and Node properties")
      startTime = System.nanoTime()
      val eRDD: RDD[Edge[edgeData]] = theGraph.edges.map(record => Edge(record.srcId, record.dstId, {
        val ORIGBYTES = DataDistributions.getOrigBytesSample
        val ORIGIPBYTE = DataDistributions.getOrigIPBytesSample(ORIGBYTES)
        val CONNECTSTATE = DataDistributions.getConnectionStateSample(ORIGBYTES)
        val PROTOCOL = DataDistributions.getProtoSample(ORIGBYTES)
        val DURATION = DataDistributions.getDurationSample(ORIGBYTES)
        val ORIGPACKCNT = DataDistributions.getOrigPktsSample(ORIGBYTES)
        val RESPBYTECNT = DataDistributions.getRespBytesSample(ORIGBYTES)
        val RESPIPBYTECNT = DataDistributions.getRespIPBytesSample(ORIGBYTES)
        val RESPPACKCNT = DataDistributions.getRespPktsSample(ORIGBYTES)
        edgeData("", PROTOCOL, DURATION, ORIGBYTES, RESPBYTECNT, CONNECTSTATE, ORIGPACKCNT, ORIGIPBYTE, RESPPACKCNT, RESPIPBYTECNT)
      }))
      val vRDD: RDD[(VertexId, nodeData)] = theGraph.vertices.map(record => (record._1, {
        val DATA = DataDistributions.getIpSample
        nodeData(DATA)
      }))
      theGraph = Graph(vRDD, eRDD, nodeData())
      timeSpan = (System.nanoTime() - startTime) / 1e9
      println()
      println("Finished generating Edge and Node Properties.")
      println("\tTotal time elapsed: " + timeSpan.toString)
      println()
    }

    println()
    println("Saving Kronecker Graph and Veracity measurements.....")
    println()

    //Save the ba graph into a format to be read later
    startTime = System.nanoTime()
//    saveGraph(sc, outputGraphPrefix + "_kro_" + genIter)
//    saveGraphVeracity(sc, outputGraphPrefix + "_kro_" + genIter)
    timeSpan = (System.nanoTime() - startTime) / 1e9

    println()
    println("Finished saving Kronecker graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    true
  }

  def getKroRDD(sc: SparkContext, partitions: Int, nVerts: Int, nEdges: Int, n1: Int, iter: Int, probToRCPosV_Broadcast: Broadcast[Array[(Double, Int, Int)]] ): RDD[Edge[edgeData]] =
  {
    //TODO the algorithm must be commented and meaningful variable names must be used
    val r = Random
    val i: RDD[Int] = sc.parallelize(for (i <- 0 to nEdges) yield i, partitions)

    val edgeList: RDD[Edge[edgeData]] = i.flatMap { _ =>

      var range: Long = nVerts
      var srcId: VertexId = 0
      var dstId: VertexId = 0

      for ( _ <- 1 to iter ) {
        val probToRCPosV: Array[(Double, Int, Int)] = probToRCPosV_Broadcast.value
        val prob = r.nextFloat()
        var n = 0
        while (prob > probToRCPosV(n)._1) {
          n += 1
        }

        val u = probToRCPosV(n)._2
        val v = probToRCPosV(n)._3

        //println("MtxRow " + u + ", MtxCol " + v)
        range = range / n1
        srcId += u * range
        dstId += v * range
        //println("Row " + srcId + ", Col " + dstId)
      }

      val tempEdgeData = edgeData()
      Array(((srcId, dstId), Edge(srcId, dstId, tempEdgeData)))

    }.reduceByKey((left,_) => left).map(record => record._2)

    edgeList
  }

  /**
   *  Computes the RDD of the additional edges that should be added
   *  accordingly to the edge distribution.
   *
   *  @param edgeList The RDD of the edges returned by the Kronecker
   *                  algorithm.
   *
   *  @return The RDD of the additional edges that should be added
   *          to the one returned by Kronecker algorithm.
   */
  def getMultiEdgesRDD(edgeList: RDD[Edge[edgeData]], sc: SparkContext): RDD[Edge[edgeData]] =
  {
  
   val outEdgesDistribution = sc.broadcast(DataDistributions.outEdgesDistribution)
   
   

    
    val multiEdgeList: RDD[Edge[edgeData]] = edgeList.flatMap { edge =>
   
      val r = Random.nextDouble()
      var accumulator :Double= 0

      val iterator = outEdgesDistribution.value.iterator
      var outElem : (Long, Double) = null
      while (accumulator < r && iterator.hasNext) {
        outElem = iterator.next()
        accumulator = accumulator + outElem._2
    }
    


      val multiEdgesNum = outElem._1
      var multiEdges : Array[Edge[edgeData]] = Array()

      for ( _ <- 1 until multiEdgesNum.toInt ) {
        multiEdges :+= Edge(edge.srcId, edge.dstId, edge.attr)
      }
      multiEdges
    }
    multiEdgeList
  }

  /*** Function to generate and return a kronecker graph
    *
    * @param sc Current Sparkcontext
    * @param probMtx Probability Matrix used to generate Kronecker Graph
    * @param iter Number of iterations to perform kronecker
    * @return Graph containing vertices + edu.msstate.dasi.nodeData, edges + edu.msstate.dasi.edgeData
    */
  def generateKroGraph(sc: SparkContext, partitions: Integer, probMtx: Array[Array[Double]], iter: Int): Graph[nodeData, edgeData] = {

    val n1 = probMtx.length
    println("n1 = " + n1)

    val mtxSum: Double = probMtx.map(record => record.sum).sum
    val nVerts = Math.pow(n1, iter).toInt
    val nEdges = Math.pow(mtxSum, iter).toInt
    println("Total # of Vertices: " + nVerts)
    println("Total # of Edges: " + nEdges)

    var cumProb: Double = 0f
    var probToRCPosV_Private: Array[(Double, Int, Int)] = Array.empty

    for(i <- 0 until n1)
      for(j <- 0 until n1) {
        val prob = probMtx(i)(j)
        cumProb+=prob

        //println((cumProb/mtxSum, i, j))

        probToRCPosV_Private = probToRCPosV_Private :+ (cumProb/mtxSum, i, j)
    }

    val probToRCPosV_Broadcast = sc.broadcast(probToRCPosV_Private)

    var edgeList: RDD[Edge[edgeData]] = getKroRDD(sc, partitions, nVerts, nEdges, n1, iter, probToRCPosV_Broadcast).cache()

    var curEdges = edgeList.count().toInt

    while (nEdges > curEdges) {
      val oldRDD = edgeList
      val newRDD = getKroRDD(sc, partitions, nVerts, nEdges - curEdges - 1, n1, iter, probToRCPosV_Broadcast)

      println(s"getKroRDD(sc, $partitions, $nVerts, $nEdges - $curEdges, $n1, $iter, probToRCPosV_Broadcast)")
      edgeList = oldRDD.union(newRDD).map(entry => ((entry.srcId, entry.dstId), entry)).reduceByKey((left,_) => left).map(record => record._2).cache()
      curEdges = edgeList.count().toInt
      println(curEdges)
    }
    val newEdges = getMultiEdgesRDD(edgeList, sc)

    println("Number of edges before union: "+edgeList.count().toInt)
    if (newEdges == null) {
      println("null!!")
    } else println("Not null!!")
    //edgeList = edgeList.union(newEdges)
    val finalEdgeList = edgeList.union(newEdges).cache()
    println("Total # of Edges (including multi edges): " + finalEdgeList.count().toInt)

    val vertList: RDD[(VertexId, nodeData)] = edgeList.flatMap{record =>
      val srcId: VertexId = record.srcId
      val dstId: VertexId = record.dstId
      Array(srcId, dstId)
    }.distinct().map{record: VertexId =>
      val tempNodeData: nodeData = nodeData()
      (record, tempNodeData)
    }

    val vRDD: RDD[(VertexId, nodeData)] = vertList.cache()
    val eRDD: RDD[Edge[edgeData]] = edgeList.cache()
    val theGraph = Graph(vRDD, eRDD, nodeData())

    theGraph
  }

//  def checkProb(prob: Float, u: Int, v: Int, n1: Int, k: Int, probMtx: Array[Array[Float]]): Boolean = {
//
//    val adjProb = prob
//
//    var result = 1f
//
//    for(i <- 0 to (k-1)) {
//
//      val x: Int = Math.floor(u/Math.pow(n1,i)).toInt % n1
//      val y: Int = Math.floor(v/Math.pow(n1,i)).toInt % n1
//      val currProb = probMtx(x)(y)
//
//      result = result * currProb
//
//      if (result < adjProb) return false
//    }
//
//    return true
//  }

  def parseMtxDataFromFile(sc: SparkContext, mtxFilePath: String): Array[Array[Double]] = {
    sc.textFile(mtxFilePath)
      .map(line => line.split(" "))
      .map(record => record.map(number => number.toDouble).array)
      .collect()
  }

  /*** Function to convert an adjaceny matrix to an edge RDD with correct properties, for use with GraphX
    *
    * @param adjMtx The matrix to convert into an edge RDD
    * @return Edge RDD containing the edge data for the graph
    */
  def mtx2Edges(adjMtx: RDD[RDD[Int]]): RDD[Edge[edgeData]] = {
    adjMtx.zipWithIndex
      .map(record => (record._2, record._1))
      .map(record => (record._1, record._2.zipWithIndex.map(record=>(record._2, record._1))))
      .flatMap{record =>
        val edgesTo = record._2.filter(record => record._2!=0).map(record => record._1)
        edgesTo.map(record2 => Edge(record._1, record2, edgeData())).collect()
      }

  }
}
