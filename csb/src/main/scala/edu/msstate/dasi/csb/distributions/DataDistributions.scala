package edu.msstate.dasi.csb.distributions

import edu.msstate.dasi.csb.{EdgeData, VertexData}
import org.apache.spark.graphx.Graph

/**
 * Contains all the probability distributions of the domain. The conditional
 * distributions are conditioned by the origin bytes values.
 *
 * @param graph the graph used to compute the distributions
 */
class DataDistributions(graph: Graph[VertexData, EdgeData]) {

  /**
   * The in-degree distribution.
   */
  val inDegree: Distribution[Int] = new Distribution(graph.inDegrees.values)

  /**
   * The out-degree distribution.
   */
  val outDegree: Distribution[Int] = new Distribution(graph.outDegrees.values)

  /**
   * The origin bytes distribution.
   */
  val origBytes: Distribution[Long] = new Distribution(graph.edges.map(_.attr.origBytes))

  /**
   * The response bytes conditional distribution.
   */
  val respBytes: ConditionalDistribution[Long, Long] = {
    val data = graph.edges.map(e => (e.attr.respBytes, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The protocol conditional distribution.
   */
  val proto: ConditionalDistribution[String, Long] = {
    val data = graph.edges.map(e => (e.attr.proto, e.attr.origBytes))
    new ConditionalDistribution(data)
  }
}
