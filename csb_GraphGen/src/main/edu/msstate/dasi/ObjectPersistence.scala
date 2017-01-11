package edu.msstate.dasi

import java.io.File

import org.apache.hadoop.fs.FileUtil
import org.apache.spark.graphx.Graph

/**
 * Created by scordio on 1/4/17.
 */
class ObjectPersistence(path: String) extends GraphPersistence {
  private val edges_suffix = "_edges"
  private val vertices_suffix = "_vertices"

  /**
   * Save the graph
   *
   * @param graph
   * @param overwrite
   */
  override def saveGraph(graph: Graph[nodeData, edgeData], overwrite :Boolean = false): Unit = {
    if (overwrite) {
      FileUtil.fullyDelete(new File(path + edges_suffix))
      FileUtil.fullyDelete(new File(path + vertices_suffix))
    }

    graph.edges.coalesce(16).saveAsObjectFile(path + edges_suffix)
    graph.vertices.coalesce(16).saveAsObjectFile(path + vertices_suffix)
  }
}
