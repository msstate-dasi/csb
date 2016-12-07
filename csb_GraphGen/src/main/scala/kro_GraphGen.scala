import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRowMatrix, MatrixEntry}

import scala.util.Random


/***
  * Created by spencer on 11/3/16.
  *
  * kro_GraphGen: Kronecker based Graph generation given seed matrix.
  */
class kro_GraphGen {
  /*** Function to generate and return a kronecker graph
    *
    * @param sc Current Sparkcontext
    * @param probMtx Probability Matrix used to generate Kronecker Graph
    * @param iter Number of iterations to perform kronecker
    * @return Graph containing vertices + nodeData, edges + edgeData
    */
  def generateKroGraph(sc: SparkContext, probMtx: Array[Array[Float]], iter: Int): Graph[nodeData, edgeData] = {
    val r = Random

    val n1 = probMtx.length
    println("n1 = " + n1)
    val nVert = Math.pow(n1, iter).toInt
    println("Vertices: " + nVert)

    var vertexList: Array[(VertexId, nodeData)] = Array.empty[(VertexId, nodeData)]
    var edgeList: Array[Edge[edgeData]] = Array.empty[Edge[edgeData]]

    var probSum: Float = 0

    for(i <- 0 to nVert - 1) {
      if( i % (nVert/10) == 0) {
        println( ((10*i).toDouble/nVert * 10).toInt)

      }

      val tempNodeData = nodeData("")
      val vId: VertexId = i.toLong
      vertexList = vertexList :+ (vId, tempNodeData)
      for(j <- 0 to nVert - 1) {
        val prob = r.nextFloat()

        if (checkProb(prob, i, j, n1, iter, probMtx)) {
          //we add an edge
          val srcId = i.toLong
          val dstId = j.toLong
          val tempEdgeData = edgeData("","",0, 0,0,"",0,0,0,0,"")

          probSum += prob
          edgeList = edgeList :+ Edge(srcId, dstId, tempEdgeData)
        } else {
          //we don't add an edge
        }
      }
    }

    println("Total # of edges: " + edgeList.length)
    println("probSum: " + probSum)

    val vRDD = sc.parallelize(vertexList)
    val eRDD = sc.parallelize(edgeList)
    val theGraph = Graph(vRDD, eRDD, nodeData(""))

    theGraph
  }

  def checkProb(prob: Float, u: Int, v: Int, n1: Int, k: Int, probMtx: Array[Array[Float]]): Boolean = {

    val adjProb = prob

    var result = 1f

    for(i <- 0 to (k-1)) {

      val x: Int = Math.floor(u/Math.pow(n1,i)).toInt % n1
      val y: Int = Math.floor(v/Math.pow(n1,i)).toInt % n1
      val currProb = probMtx(x)(y)

      result = result * currProb

      if (result < adjProb) return false
    }

    return true
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
        edgesTo.map(record2 => Edge(record._1, record2, edgeData("","",0, 0,0,"",0,0,0,0,""))).collect()
      }

  }
}
