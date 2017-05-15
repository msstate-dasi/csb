package edu.msstate.dasi.csb.distributions

import edu.msstate.dasi.csb.{EdgeData, VertexData}
import org.apache.spark.graphx.Graph

/**
 * Contains all the probability distributions of the domain. All conditional
 * distributions are conditioned by origin bytes values.
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
   * The protocol conditional distribution.
   */
  val proto: ConditionalDistribution[String, Long] = {
    val data = graph.edges.map(e => (e.attr.proto, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The duration conditional distribution.
   */
  val duration: ConditionalDistribution[Double, Long] = {
    val data = graph.edges.map(e => (e.attr.duration, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The response bytes conditional distribution.
   */
  val respBytes: ConditionalDistribution[Long, Long] = {
    val data = graph.edges.map(e => (e.attr.respBytes, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The connection state conditional distribution.
   */
  val connState: ConditionalDistribution[String, Long] = {
    val data = graph.edges.map(e => (e.attr.connState, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The origin packets conditional distribution.
   */
  val origPkts: ConditionalDistribution[Long, Long] = {
    val data = graph.edges.map(e => (e.attr.origPkts, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The origin IP bytes conditional distribution.
   */
  val origIpBytes: ConditionalDistribution[Long, Long] = {
    val data = graph.edges.map(e => (e.attr.origIpBytes, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The response packets conditional distribution.
   */
  val respPkts: ConditionalDistribution[Long, Long] = {
    val data = graph.edges.map(e => (e.attr.respPkts, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The response IP bytes conditional distribution.
   */
  val respIpBytes: ConditionalDistribution[Long, Long] = {
    val data = graph.edges.map(e => (e.attr.respIpBytes, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The description conditional distribution.
   */
  val desc: ConditionalDistribution[String, Long] = {
    val data = graph.edges.map(e => (e.attr.desc, e.attr.origBytes))
    new ConditionalDistribution(data)
  }
}
