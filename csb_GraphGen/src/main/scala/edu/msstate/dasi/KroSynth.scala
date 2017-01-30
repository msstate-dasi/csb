package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

/***
  * Created by spencer on 11/3/16.
  *
  * KroSynth: Kronecker based Graph generation given seed matrix.
  */
class KroSynth(sc: SparkContext, partitions: Int, dataDist: DataDistributions, graphPs: GraphPersistence, mtxFile: String, genIter: Int) extends GraphSynth with DataParser {

  private def parseMtxDataFromFile(mtxFilePath: String): Array[Array[Double]] = {
    sc.textFile(mtxFilePath)
      .map(line => line.split(" "))
      .map(record => record.map(number => number.toDouble).array)
      .collect()
  }

  private def getKroRDD(nVerts: Long, nEdges: Long, n1: Int, iter: Int, probToRCPosV_Broadcast: Broadcast[Array[(Double, Long, Long)]] ): RDD[(VertexId, VertexId)] = {
    // TODO: the algorithm must be commented and meaningful variable names must be used

    val r = Random

    val localPartitions = math.min(nEdges, partitions).toInt
    val recordsPerPartition = math.min( (nEdges / localPartitions).toInt, Int.MaxValue )
    val i = sc.parallelize(Seq[Char](), localPartitions).mapPartitions( _ =>  (1 to recordsPerPartition).iterator )

    val edgeList = i.map { _ =>

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
      (srcId, dstId)
    }

    edgeList
  }

  /**
   *  Computes the RDD of the additional edges that should be added accordingly to the edge distribution.
   *
   *  @param edgeList The RDD of the edges returned by the Kronecker algorithm.
   *  @return The RDD of the additional edges that should be added to the one returned by Kronecker algorithm.
   */
  private def getMultiEdgesRDD(edgeList: RDD[(VertexId, VertexId)]): RDD[(VertexId, VertexId)] = {
    val dataDistBroadcast = sc.broadcast(dataDist)

    val multiEdgeList = edgeList.flatMap { case (srcId, dstId) =>
      val multiEdgesNum = dataDistBroadcast.value.getOutEdgeSample
      var multiEdges = Array.empty[(VertexId, VertexId)]

      for ( _ <- 1L until multiEdgesNum.toLong ) {
        multiEdges :+= (srcId, dstId)
      }

      multiEdges
    }
    multiEdgeList
  }

  /***
   * Synthesize a graph from a seed graph and its property distributions.
   */
  protected def genGraph(seed: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, Int] = {
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
    generateKroGraph(probMtx)
  }

  /*** Function to generate and return a kronecker graph
    *
    * @param probMtx Probability Matrix used to generate Kronecker Graph
    * @return Graph containing vertices + VertexData, edges + EdgeData
    */
  private def generateKroGraph(probMtx: Array[Array[Double]]): Graph[VertexData, Int] = {

    val n1 = probMtx.length
    println("n1 = " + n1)

    val mtxSum: Double = probMtx.map(record => record.sum).sum
    val nVerts = math.pow(n1, genIter).toLong
    val nEdges = math.pow(mtxSum, genIter).toLong
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

    var startTime = System.nanoTime()

    val probToRCPosV_Broadcast = sc.broadcast(probToRCPosV_Private)

    var curEdges: Long = 0
    var edgeList = sc.emptyRDD[(VertexId, VertexId)]

    while (curEdges < nEdges) {
      println("getKroRDD(" + (nEdges - curEdges) + ")")

      val oldEdgeList = edgeList

      val newRDD = getKroRDD(nVerts, nEdges - curEdges, n1, genIter, probToRCPosV_Broadcast)
      edgeList = oldEdgeList.union(newRDD).distinct()
        .coalesce(partitions).setName("edgeList#" + curEdges).persist(StorageLevel.MEMORY_AND_DISK)
      curEdges = edgeList.count()

      oldEdgeList.unpersist()

      println(s"Requested/created: $nEdges/$curEdges")
    }

    var timeSpan = (System.nanoTime() - startTime) / 1e9

    println(s"All getKroRDD time: $timeSpan s")

    println("Number of edges before union: " + edgeList.count())

    startTime = System.nanoTime()

    val newEdges = getMultiEdgesRDD(edgeList).setName("newEdges")

    // TODO: finalEdgeList should be un-persisted after the next action (but the action will probably be outside this method)
    val finalEdgeList = edgeList.union(newEdges)
      .coalesce(partitions).setName("finalEdgeList").persist(StorageLevel.MEMORY_AND_DISK)
    println("Total # of Edges (including multi edges): " + finalEdgeList.count())

    timeSpan = (System.nanoTime() - startTime) / 1e9

    println(s"MultiEdges time: $timeSpan s")

    val theGraph = Graph.fromEdgeTuples(
      finalEdgeList,
      // TODO: vertex properties are not needed at this stage, VertexData() could be replaced with Int to improve memory consumption
      VertexData(),
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK
    )

    theGraph
  }

  def run(seedVertFile: String, seedEdgeFile: String, withProperties: Boolean): Boolean = {

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
    var graph = generateKroGraph(probMtx)
    println("Vertices #: " + graph.numVertices + ", Edges #: " + graph.numEdges)

    var timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating Kronecker graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    if (withProperties) {
//      theGraph = genProperties(sc, theGraph, dataDist)
    }

    println("Saving Kronecker Graph...")
    //Save the ba graph into a format to be read later
    startTime = System.nanoTime()
//    graphPs.saveGraph(theGraph, overwrite = true)
    timeSpan = (System.nanoTime() - startTime) / 1e9

    println("Finished saving Kronecker Graph. Total time elapsed: " + timeSpan.toString + "s")

    // TODO: the following should be removed
    val eRDD: RDD[Edge[EdgeData]] = graph.edges.map(record => Edge(record.srcId, record.dstId, EdgeData()))
    val vRDD: RDD[(VertexId, VertexData)] = graph.vertices.map(record => (record._1, VertexData()))
    val theGraph = Graph(vRDD, eRDD, VertexData())
    /////////////////////

    println("Calculating degrees veracity...")

    val (inVertices, inEdges): (RDD[(VertexId,VertexData)], RDD[Edge[EdgeData]]) = readFromSeedGraph(sc, partitions, seedVertFile,seedEdgeFile)

    val seedGraph = Graph(inVertices, inEdges, VertexData())

    println("Edges: " + seedGraph.edges.count())

    startTime = System.nanoTime()
    val degVeracity = Degree(seedGraph, theGraph)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println(s"\tDegree Veracity: $degVeracity [$timeSpan s]")

    startTime = System.nanoTime()
    val inDegVeracity = InDegree(seedGraph, theGraph)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println(s"\tIn Degree Veracity: $inDegVeracity [$timeSpan s]")

    startTime = System.nanoTime()
    val outDegVeracity = OutDegree(seedGraph, theGraph)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println(s"\tOut Degree Veracity: $outDegVeracity [$timeSpan s]")

    startTime = System.nanoTime()
    val pageRankVeracity = PageRank(seedGraph, theGraph)
    timeSpan = (System.nanoTime() - startTime) / 1e9
    println(s"\tPage Rank Veracity: $pageRankVeracity  [$timeSpan s]")

    true
  }
}
