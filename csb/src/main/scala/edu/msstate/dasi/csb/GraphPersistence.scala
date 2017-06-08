package edu.msstate.dasi.csb

import edu.msstate.dasi.csb.model.{EdgeData, VertexData}
import org.apache.spark.graphx.Graph

/**
  * Defines universal methods to load and save graphs.
  */
trait GraphPersistence {

  /**
    * Loads a graph.
    * @param graphName Identifier for a graph.
    * @param partitions Number of partitions to use when loading the graph
    * @return Reference to a Graph object loaded into memory.
    */
  def loadGraph(graphName: String, partitions: Int): Graph[VertexData, EdgeData]

  /**
    * Saves a graph.
    * @param graph Identifier for a graph.
    * @param graphName Identifier for a graph.
    * @return True if saving was successful, otherwise false.
    */
  def saveGraph(graph: Graph[VertexData, EdgeData], graphName: String, overwrite :Boolean = false): Unit

  /**
    * Loads a graph from a textual representation.
    * @param graphName Identifier for a graph.
    * @param partitions Number of partitions to use when loading the graph
    * @return Reference to a Graph object loaded into memory.
    */
  def loadFromText(graphName: String, partitions: Int): Graph[VertexData, EdgeData]

  /**
    * Saves a graph to a textual representation.
    * @param graph Identifier for a graph.
    * @param graphName Identifier for a graph.
    * @return True if saving was successful, otherwise false.
    */
  def saveAsText(graph: Graph[VertexData, EdgeData], graphName: String, overwrite: Boolean = false): Unit
}
