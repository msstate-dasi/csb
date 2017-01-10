package edu.msstate.dasi

import org.apache.spark.graphx.Graph

/**
 * Created by scordio on 1/4/17.
 */
class ObjectPersistence(path: String) extends GraphPersistence with java.io.Serializable{
  private val edges_suffix = "_edges"
  private val vertices_suffix = "_vertices"

  /**
   * Save the graph
   *
   * @param graph
   */
  override def saveGraph(graph: Graph[nodeData, edgeData]): Unit = {
    graph.edges.coalesce(16).saveAsObjectFile(path + edges_suffix)
    graph.vertices.coalesce(16).saveAsObjectFile(path + vertices_suffix)
  }
}
