import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD


/**
  * Created by spencer on 11/3/16.
  */
class kro_GraphGen {
  def kro_GraphGen(sc: SparkContext, startMtx: RDD[RDD[Int]], iter: Int): Graph[nodeData, edgeData] = {

    var adjMtx = startMtx

    for (x <- 1 to iter) {
      var tmpMtx = adjMtx.map(record => expMtx(record, adjMtx))
      adjMtx = tmpMtx.flatMap(record => flatMtx(record))

    }

    var edges: Array[Edge[String]] = mtx2Edges(adjMtx)

    var eRDD = sc.parallelize(edges)

    var vertices = Array(for (x <- 1 to adjMtx.length) yield (x.toLong,"Node " + x.toString))

    var vRDD: RDD[(VertexId, String)] = sc.parallelize(vertices)

    var theGraph = Graph(vRDD, eRDD, nodeData(""))

    theGraph
  }

  def flatMtx(row: RDD[RDD[RDD[Int]]]): RDD[RDD[Int]] = {
    val n = (row.count()-1).toInt

    (for (i <- 0 to n) yield {
      //for every element in the row, grab each element
      val newRow = (for (j <- 0 to n) yield row(j)(i)).flatMap(record => for (x <- record) yield x).toArray

      newRow
    }).toArray

  }

  def expMtx(row: RDD[Int], adjMtx: RDD[RDD[Int]]): RDD[RDD[RDD[Int]]] = {
    val zeroMtx: RDD[RDD[Int]] = (for (x <- 1 to adjMtx.length) yield (for (x <- 1 to adjMtx.length) yield 0).toArray).toArray
    val n = row.length - 1
    (for (x <- 0 to n) yield if(row(x)==0) zeroMtx else adjMtx).toArray
  }

  def mtx2Edges(adjMtx: RDD[RDD[Int]]): RDD[Edge[edgeData]] = {
    adjMtx.zipWithIndex
      .map(record => (record._2, record._1))
      //.map(record => (record._1, record._2.zipWithIndex.map(record=>(record._2, record._1))))
      .flatMap{record =>
      for {
        x <- 1 to record._2.row if record._2(x) == 1
      } yield Edge(record._1.toLong, x.toLong, "")
    }
  }
}
