package edu.msstate.dasi.csb

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

/***
  * Created by spencer on 11/3/16.
  *
  * KroSynth: Kronecker based Graph generation given seed matrix.
  */
class KroSynth(partitions: Int, mtxFile: String, genIter: Int) extends GraphSynth {

  private def parseMtxDataFromFile(mtxFilePath: String): Array[Array[Double]] = {
    sc.textFile(mtxFilePath)
      .map(line => line.split(" "))
      .map(record => record.map(number => number.toDouble).array)
      .collect()
  }

  /**
   * Generates a small probability matrix from a graph.
   *
   * The KronFit algorithm is a gradient descent based algorithm which ensures that the probability of generating the
   * original graph from the small probability matrix after performing Kronecker multiplications is very high.
   */
  private def kronFit(seed: Graph[VertexData, EdgeData]): Array[Array[Double]] = {
    //inMtx already has a default value
    KroFit.run(seed)
  }

  private def getKroRDD(nVerts: Long, nEdges: Long, n1: Int, iter: Int, probToRCPosV_Broadcast: Broadcast[Array[(Double, Long, Long)]] ): RDD[Edge[EdgeData]] = {
    // TODO: the algorithm must be commented and meaningful variable names must be used

    val r = Random

    val localPartitions = math.min(nEdges, partitions).toInt
    val recordsPerPartition = math.min( (nEdges / localPartitions).toInt, Int.MaxValue )
    val i = sc.parallelize(Seq.empty[Char], localPartitions).mapPartitions( _ =>  (1 to recordsPerPartition).iterator )

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

        range = range / n1
        srcId += u * range
        dstId += v * range
      }
      Edge[EdgeData](srcId, dstId)
    }

    edgeList
  }

  /**
   *  Computes the RDD of the additional edges that should be added accordingly to the edge distribution.
   *
   *  @param edgeList The RDD of the edges returned by the Kronecker algorithm.
   *  @return The RDD of the additional edges that should be added to the one returned by Kronecker algorithm.
   */
  private def getMultiEdgesRDD(edgeList: RDD[Edge[EdgeData]], seedDists: DataDistributions): RDD[Edge[EdgeData]] = {
    val dataDistBroadcast = sc.broadcast(seedDists)

    val multiEdgeList = edgeList.flatMap { edge =>
      val multiEdgesNum = dataDistBroadcast.value.getOutEdgeSample
      var multiEdges = Array.empty[Edge[EdgeData]]

      for ( _ <- 1L until multiEdgesNum.toLong ) {
        multiEdges :+= Edge[EdgeData](edge.srcId, edge.dstId)
      }

      multiEdges
    }
    multiEdgeList
  }

  /*** Function to generate and return a kronecker graph
    *
    * @param probMtx Probability Matrix used to generate Kronecker Graph
    * @return Graph containing vertices + VertexData, edges + EdgeData
    */
  private def generateKroGraph(probMtx: Array[Array[Double]], seedDists: DataDistributions): Graph[VertexData, EdgeData] = {

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
    var edgeList = sc.emptyRDD[Edge[EdgeData]]

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

    val newEdges = getMultiEdgesRDD(edgeList, seedDists).setName("newEdges")

    // TODO: finalEdgeList should be un-persisted after the next action (but the action will probably be outside this method)
    val finalEdgeList = edgeList.union(newEdges)
      .coalesce(partitions).setName("finalEdgeList").persist(StorageLevel.MEMORY_AND_DISK)
    println("Total # of Edges (including multi edges): " + finalEdgeList.count())

    timeSpan = (System.nanoTime() - startTime) / 1e9

    println(s"MultiEdges time: $timeSpan s")

    Graph.fromEdges(
      finalEdgeList,
      null.asInstanceOf[VertexData],
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK
    )
  }

  /***
   * Synthesize a graph from a seed graph and its property distributions.
   */
  protected def genGraph(seed: Graph[VertexData, EdgeData], seedDists: DataDistributions): Graph[VertexData, EdgeData] = {
    //val probMtx: Array[Array[Float]] = Array(Array(0.1f, 0.9f), Array(0.9f, 0.5f))
    val probMtx: Array[Array[Double]] = kronFit(seed)

    println()
    print("Matrix: ")
    probMtx.foreach(_.foreach(record => print(record + " ")))
    println()
    println()

    println()
    println(s"Running Kronecker with $genIter iterations.")
    println()

    //Run Kronecker with the adjacency matrix
    generateKroGraph(probMtx, seedDists)
  }
}
