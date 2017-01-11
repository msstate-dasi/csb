package edu.msstate.dasi

import org.apache.spark.graphx.Graph

/**
 * Created by scordio on 1/4/17.
 */
trait GraphPersistence {

  /**
   * Save the graph
   *
   * @param graph
   * @param overwrite
   */
  def saveGraph(graph: Graph[nodeData, edgeData], overwrite :Boolean = false): Unit
}
