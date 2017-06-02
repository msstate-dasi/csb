package edu.msstate.dasi.csb

import edu.msstate.dasi.csb.model.{EdgeData, VertexData}
import org.apache.spark.graphx.Graph

trait GraphPersistence {

  /**
   * Load a graph.
   */
  def loadGraph(graphName: String, partitions: Int): Graph[VertexData, EdgeData]

  /**
   * Save a graph.
   */
  def saveGraph(graph: Graph[VertexData, EdgeData], graphName: String, overwrite :Boolean = false): Unit

  /**
   * Load a graph from a textual representation.
   */
  def loadFromText(graphName: String, partitions: Int): Graph[VertexData, EdgeData]

  /**
   * Save a graph as a textual representation.
   */
  def saveAsText(graph: Graph[VertexData, EdgeData], graphName: String, overwrite: Boolean = false): Unit
}
