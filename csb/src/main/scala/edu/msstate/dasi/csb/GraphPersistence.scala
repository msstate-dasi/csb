package edu.msstate.dasi.csb

import org.apache.spark.graphx.Graph

trait GraphPersistence {

  /**
   * Load a graph.
   */
  def loadGraph(name: String): Graph[VertexData, EdgeData]

  /**
   * Save a graph.
   */
  def saveGraph(graph: Graph[VertexData, EdgeData], name: String, overwrite :Boolean = false): Unit
}
