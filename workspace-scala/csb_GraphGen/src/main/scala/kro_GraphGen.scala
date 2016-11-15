import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD


/**
  * Created by spencer on 11/3/16.
  */
class kro_GraphGen {
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

  def flatMtx(sc: SparkContext, row: RDD[RDD[RDD[Int]]]): RDD[RDD[Int]] = {

    val redRow: RDD[RDD[Int]] = row.flatMap(record => record.collect())

    redRow
  }

  def expMtx(sc: SparkContext, row: RDD[Int], adjMtx: RDD[RDD[Int]]): RDD[RDD[RDD[Int]]] = {
    val zeroMtx: RDD[RDD[Int]] = adjMtx.map(record => record.map(record => 0))
    val n = row.count().toInt - 1

    val temp: RDD[RDD[RDD[Int]]] = row.map (record =>
     if (record == 1) adjMtx
     else zeroMtx)

    temp
  }

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
