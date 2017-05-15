package edu.msstate.dasi.csb.distributions

import edu.msstate.dasi.csb.{EdgeData, VertexData}
import org.apache.spark.graphx.Graph

class DataDistributions(graph: Graph[VertexData, EdgeData]) {

  private val inDegreeDistribution = new Distribution(graph.inDegrees.values)
  private val outDegreeDistribution = new Distribution(graph.outDegrees.values)

  private val origBytesDistribution = new Distribution(graph.edges.map(_.attr.origBytes))

  /**
   * Returns an in-degree sample.
   */
  def inDegreeSample: Int = inDegreeDistribution.sample

  /**
   * Returns an out-degree sample.
   */
  def outDegreeSample: Int = outDegreeDistribution.sample

  /**
   * Returns an origin bytes sample.
   */
  def origBytesSample: Long = origBytesDistribution.sample
}
