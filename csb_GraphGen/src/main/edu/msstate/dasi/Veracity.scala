package edu.msstate.dasi

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph, VertexRDD}

/**
 * Helper methods to compute veracity metrics for [[org.apache.spark.graphx.Graph]]
 */
object Veracity {

  /**
   * Computes the neighboring vertex degrees distribution
   *
   * @param degrees The degrees to analyze
   * @return RDD containing the normalized degrees distributions
   */
  def degreesDistRDD(degrees: VertexRDD[Int]): RDD[(Int, Double)] = {
    val degreesCount = degrees.map(x => (x._2, 1L)).reduceByKey(_ + _).cache()
    val degreesTotal = degreesCount.map(_._2).reduce(_ + _)
    degreesCount.map(x => (x._1, x._2 / degreesTotal.toDouble)).sortBy(_._2, ascending = false)
  }

  /**
   * Computes the squared Euclidean distance between two vectors
   *
   * @note If the arrays differ in size, the lower size defines how many elements are taken from the beginning
   */
  private def squaredDistance(v1: Array[Double], v2: Array[Double]): Double = {
    var d = 0.0

    for (i <- 0 until v1.length.min(v2.length)) {
      d += math.pow( v1(i) - v2(i), 2 )
    }
    d
  }

  /**
   * Computes the veracity factor between two normalized degrees distributions
   */
  def degreesVeracity(d1: RDD[(Int, Double)], d2: RDD[(Int, Double)]): Double = {
    val v1 = d1.map(_._2).collect()
    val v2 = d2.map(_._2).collect()
    squaredDistance(v1,v2)
  }

  /**
   * Computes the effective diameter of the graph, defined as the minimum number of links (steps/hops) in which some
   * fraction (or quantile q, say q = 0.9) of all connected pairs of nodes can reach each other
   *
   * @param graph The graph to analyze
   * @return The value of the effective diameter
   */
  def effectiveDiameter(graph: Graph[nodeData, edgeData]): Long = {
    0
  }
}
