import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD


/***
  * Created by spencer on 11/3/16.
  *
  * kro_GraphGen: Kronecker based Graph generation given seed matrix.
  */
class kro_GraphGen {
  /*** Function to generate and return a kronecker graph
    *
    * @param sc Current Sparkcontext
    * @param startMtx Seed adjacency matrix
    * @param iter Number of iterations to perform kronecker
    * @return Graph containing vertices + nodeData, edges + edgeData
    */
  def generateKroGraph(sc: SparkContext, startMtx: RDD[RDD[Int]], iter: Int): Graph[nodeData, edgeData] = {

    var adjMtx = startMtx

    for (x <- 1 to iter) {
      var tmpMtx = adjMtx.map(record => expMtx(sc, record, adjMtx))
      adjMtx = tmpMtx.flatMap(record => flatMtx(sc, record).collect())
    }

    var eRDD = mtx2Edges(adjMtx)

    var vertices = (for (x <- 1 to adjMtx.count().toInt) yield (x.toLong,nodeData("Node " + x.toString))).toArray

    var vRDD: RDD[(VertexId, nodeData)] = sc.parallelize(vertices)

    var theGraph = Graph(vRDD, eRDD, nodeData(""))

    theGraph
  }

  /*** Function to flatten an expanded adjacency matrix
    *
    * @param sc Current Sparkcontext
    * @param row Current row of the expanded matrix
    * @return Row of the compressed matrix
    */
  def flatMtx(sc: SparkContext, row: RDD[RDD[RDD[Int]]]): RDD[RDD[Int]] = {

    val redRow: RDD[RDD[Int]] = row.flatMap(record => record.collect())

    redRow
  }

  /*** Function to expand an adjacency matrix according to the Kronecker Model
    *
    * @param sc Current Sparkcontext
    * @param row Current row in the seed matrix
    * @param adjMtx The seed matrix
    * @return Expanded matrix row
    */
  def expMtx(sc: SparkContext, row: RDD[Int], adjMtx: RDD[RDD[Int]]): RDD[RDD[RDD[Int]]] = {
    val zeroMtx: RDD[RDD[Int]] = adjMtx.map(record => record.map(record => 0))
    val n = row.count().toInt - 1

    val temp: RDD[RDD[RDD[Int]]] = row.map (record =>
     if (record == 1) adjMtx
     else zeroMtx)

    temp
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
        edgesTo.map(record2 => Edge(record._1, record2, edgeData("","",0,0,"",0,0,0,0,""))).collect()
      }

  }
}
