package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.util.Random

/***
  * Created by spencer on 11/3/16.
  *
  * edu.msstate.dasi.kro_GraphGen: Kronecker based Graph generation given seed matrix.
  */
class kro_GraphGen(sc: SparkContext, partitions: Int, dataDist: DataDistributions, graphPs: GraphPersistence) extends base_GraphGen with data_Parser {
  def run(mtxFile: String, genIter: Int, seedVertFile: String, seedEdgeFile: String, noPropFlag: Boolean, debugFlag: Boolean): Boolean = {

    //val probMtx: Array[Array[Float]] = Array(Array(0.1f, 0.9f), Array(0.9f, 0.5f))
    val probMtx: Array[Array[Double]] = parseMtxDataFromFile(mtxFile)

    println()
    print("Matrix: ")
    probMtx.foreach(_.foreach(record => print(record + " ")))
    println()
    println()

    println()
    println(s"Running Kronecker with $genIter iterations.")
    println()

    //Run Kronecker with the adjacency matrix
    var startTime = System.nanoTime()
    theGraph = generateKroGraph(probMtx, genIter)
    var timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating Kronecker graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    val dataDistBroadcast = sc.broadcast(dataDist)

    if(!noPropFlag) {
      println()
      println("Generating Edge and Node properties")
      startTime = System.nanoTime()
      val eRDD: RDD[Edge[edgeData]] = theGraph.edges.map(record => Edge(record.srcId, record.dstId, {
        val ORIGBYTES = dataDistBroadcast.value.getOrigBytesSample
        val ORIGIPBYTE = dataDistBroadcast.value.getOrigIPBytesSample(ORIGBYTES)
        val CONNECTSTATE = dataDistBroadcast.value.getConnectionStateSample(ORIGBYTES)
        val PROTOCOL = dataDistBroadcast.value.getProtoSample(ORIGBYTES)
        val DURATION = dataDistBroadcast.value.getDurationSample(ORIGBYTES)
        val ORIGPACKCNT = dataDistBroadcast.value.getOrigPktsSample(ORIGBYTES)
        val RESPBYTECNT = dataDistBroadcast.value.getRespBytesSample(ORIGBYTES)
        val RESPIPBYTECNT = dataDistBroadcast.value.getRespIPBytesSample(ORIGBYTES)
        val RESPPACKCNT = dataDistBroadcast.value.getRespPktsSample(ORIGBYTES)
        val DESC = dataDistBroadcast.value.getDescSample(ORIGBYTES)
        edgeData("", PROTOCOL, DURATION, ORIGBYTES, RESPBYTECNT, CONNECTSTATE, ORIGPACKCNT, ORIGIPBYTE, RESPPACKCNT, RESPIPBYTECNT, DESC)
      }))
      val vRDD: RDD[(VertexId, nodeData)] = theGraph.vertices.map(record => (record._1, {
        val DATA = dataDistBroadcast.value.getIpSample
        nodeData(DATA)
      }))
      theGraph = Graph(vRDD, eRDD, nodeData())
      timeSpan = (System.nanoTime() - startTime) / 1e9
      println("Finished generating Edge and Node Properties. Total time elapsed: " + timeSpan.toString)
    }

    println("Saving Kronecker Graph...")
    //Save the ba graph into a format to be read later
    startTime = System.nanoTime()
    graphPs.saveGraph(theGraph, overwrite = true)
    timeSpan = (System.nanoTime() - startTime) / 1e9

    println("Finished saving Kronecker Graph. Total time elapsed: " + timeSpan.toString + "s")

    println("Calculating degrees veracity...")

    val (inVertices, inEdges): (RDD[(VertexId,nodeData)], RDD[Edge[edgeData]]) = readFromSeedGraph(sc, seedVertFile,seedEdgeFile)

    val seedGraph = Graph(inVertices, inEdges, nodeData())

    println("Edges: " + seedGraph.edges.count())

    startTime = System.nanoTime()
    val degVeracity = Veracity.degree(seedGraph.degrees, theGraph.degrees)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println(s"\tDegree Veracity: $degVeracity [$timeSpan s]")

    startTime = System.nanoTime()
    val inDegVeracity = Veracity.degree(seedGraph.inDegrees, theGraph.inDegrees)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println(s"\tIn Degree Veracity: $inDegVeracity [$timeSpan s]")

    startTime = System.nanoTime()
    val outDegVeracity = Veracity.degree(seedGraph.outDegrees, theGraph.outDegrees)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println(s"\tOut Degree Veracity: $outDegVeracity [$timeSpan s]")

    true
  }

  def getKroRDD(nVerts: Long, nEdges: Long, n1: Int, iter: Int, probToRCPosV_Broadcast: Broadcast[Array[(Double, Long, Long)]] ): RDD[Edge[edgeData]] = {
    // TODO: the algorithm must be commented and meaningful variable names must be used

    val r = Random

    val localPartitions = math.min(nEdges, partitions).toInt
    val recordsPerPartition = math.min( (nEdges / localPartitions).toInt, Int.MaxValue )
    val i = sc.parallelize(Seq[Char](), localPartitions).mapPartitions( _ =>  (1 to recordsPerPartition).iterator )

    val edgeList = i.flatMap { _ =>

      var range = nVerts
      var srcId = 0L
      var dstId = 0L

      for ( _ <- 1 to iter ) {
        val probToRCPosV = probToRCPosV_Broadcast.value
        val prob = r.nextDouble()
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

      Array( ( (srcId, dstId), Edge(srcId, dstId, edgeData()) ) )

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
  def getMultiEdgesRDD(edgeList: RDD[Edge[edgeData]]): RDD[Edge[edgeData]] = {
//    val outEdgesDistribution = sc.broadcast(dataDist)
    val dataDistBroadcast = sc.broadcast(dataDist)

    val multiEdgeList: RDD[Edge[edgeData]] = edgeList.flatMap { edge =>
//      val r = Random.nextDouble()
//      var accumulator :Double= 0
//
//      val iterator = outEdgesDistribution.value.iterator
//      var outElem : (Long, Double) = null
//      while (accumulator < r && iterator.hasNext) {
//        outElem = iterator.next()
//        accumulator = accumulator + outElem._2
//      }

//      val multiEdgesNum = outElem._1
      val multiEdgesNum = dataDistBroadcast.value.getOutEdgeSample
      var multiEdges : Array[Edge[edgeData]] = Array()

      for ( _ <- 1L until multiEdgesNum.toLong ) {
        multiEdges :+= Edge(edge.srcId, edge.dstId, edge.attr)
      }

      multiEdges
    }
    multiEdgeList
  }

  /*** Function to generate and return a kronecker graph
    *
    * @param probMtx Probability Matrix used to generate Kronecker Graph
    * @param iter Number of iterations to perform kronecker
    * @return Graph containing vertices + edu.msstate.dasi.nodeData, edges + edu.msstate.dasi.edgeData
    */
  def generateKroGraph(probMtx: Array[Array[Double]], iter: Int): Graph[nodeData, edgeData] = {

    val n1 = probMtx.length
    println("n1 = " + n1)

    val mtxSum: Double = probMtx.map(record => record.sum).sum
    val nVerts = math.pow(n1, iter).toLong
    val nEdges = math.pow(mtxSum, iter).toLong
    println("Total # of Vertices: " + nVerts)
    println("Total # of Edges: " + nEdges)

    var cumProb: Double = 0f
    var probToRCPosV_Private: Array[(Double, Long, Long)] = Array.empty

    for (i <- 0 until n1; j <- 0 until n1) {
        val prob = probMtx(i)(j)
        cumProb+=prob

        //println((cumProb/mtxSum, i, j))

        probToRCPosV_Private = probToRCPosV_Private :+ (cumProb/mtxSum, i.toLong, j.toLong)
    }

    val probToRCPosV_Broadcast = sc.broadcast(probToRCPosV_Private)

    var curEdges: Long = 0
    var edgeList: RDD[Edge[edgeData]] = sc.emptyRDD

    while ( curEdges < nEdges ) {
      println("getKroRDD(" + nVerts + ", " + (nEdges-curEdges) + s", $n1, $iter, probToRCPosV_Broadcast)")
      val newRDD = getKroRDD(nVerts, nEdges - curEdges, n1, iter, probToRCPosV_Broadcast)

      edgeList = edgeList.union(newRDD).map(entry => ((entry.srcId, entry.dstId), entry)).reduceByKey((left,_) => left).map(record => record._2).cache()
      curEdges = edgeList.count()
      println(s"$curEdges $nEdges")
    }

    val newEdges = getMultiEdgesRDD(edgeList)

    println("Number of edges before union: "+edgeList.count())
    val finalEdgeList = edgeList.union(newEdges).cache()
    println("Total # of Edges (including multi edges): " + finalEdgeList.count())

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

  def parseMtxDataFromFile(mtxFilePath: String): Array[Array[Double]] = {
    sc.textFile(mtxFilePath)
      .map(line => line.split(" "))
      .map(record => record.map(number => number.toDouble).array)
      .collect()
  }

//  /*** Function to convert an adjacency matrix to an edge RDD with correct properties, for use with GraphX
//    *
//    * @param adjMtx The matrix to convert into an edge RDD
//    * @return Edge RDD containing the edge data for the graph
//    */
//  def mtx2Edges(adjMtx: RDD[RDD[Long]]): RDD[Edge[edgeData]] = {
//    adjMtx.zipWithIndex
//      .map(record => (record._2, record._1))
//      .map(record => (record._1, record._2.zipWithIndex.map(record=>(record._2, record._1))))
//      .flatMap{record =>
//        val edgesTo = record._2.filter(record => record._2!=0).map(record => record._1)
//        edgesTo.map(record2 => Edge(record._1, record2, edgeData())).collect()
//      }
//
//  }
}
