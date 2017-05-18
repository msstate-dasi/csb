package edu.msstate.dasi.csb.distributions

import edu.msstate.dasi.csb.{EdgeData, VertexData}
import org.apache.spark.graphx.Graph

/**
 * Contains all the probability distributions of the domain. All conditional
 * distributions are conditioned by origin bytes values.
 *
 * @param graph the graph used to compute the distributions
 */
class DataDistributions(graph: Graph[VertexData, EdgeData], bucketSize: Option[Int] = None) extends Serializable {

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
  val origBytes: Distribution[Long] = new Distribution(graph.edges.map(_.attr.origBytes.intoBucket))

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
    val data = graph.edges.map(e => (e.attr.duration.intoBucket, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The response bytes conditional distribution.
   */
  val respBytes: ConditionalDistribution[Long, Long] = {
    val data = graph.edges.map(e => (e.attr.respBytes.intoBucket, e.attr.origBytes))
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
    val data = graph.edges.map(e => (e.attr.origPkts.intoBucket, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The origin IP bytes conditional distribution.
   */
  val origIpBytes: ConditionalDistribution[Long, Long] = {
    val data = graph.edges.map(e => (e.attr.origIpBytes.intoBucket, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The response packets conditional distribution.
   */
  val respPkts: ConditionalDistribution[Long, Long] = {
    val data = graph.edges.map(e => (e.attr.respPkts.intoBucket, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The response IP bytes conditional distribution.
   */
  val respIpBytes: ConditionalDistribution[Long, Long] = {
    val data = graph.edges.map(e => (e.attr.respIpBytes.intoBucket, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * The description conditional distribution.
   */
  val desc: ConditionalDistribution[String, Long] = {
    val data = graph.edges.map(e => (e.attr.desc, e.attr.origBytes))
    new ConditionalDistribution(data)
  }

  /**
   * Provides implicit methods for any [[AnyVal]] subtype.
   *
   * @param value the value
   * @tparam Val the value type
   */
  implicit private class AnyValLike[Val <: AnyVal](value: Val) {

    /**
     * Normalizes the [[value]] as if it is inserted inside a bucket of [[bucketSize]] size.
     *
     * @note only [[Long]] and [[Double]] values are normalized as the current domain doesn't use other types.
     * @return the normalized value if [[bucketSize]] exists, [[value]] otherwise.
     */
    def intoBucket: Val = {
      bucketSize match {
        case Some(size) =>
          value match {
            case long: Long => (long - long % size).asInstanceOf[Val]
            case double: Double => (double - double % size).asInstanceOf[Val]
          }
        case None => value
      }
    }
  }
}
