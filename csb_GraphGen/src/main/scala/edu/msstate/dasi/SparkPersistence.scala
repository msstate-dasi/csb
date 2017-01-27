package edu.msstate.dasi

import java.io.File

import org.apache.hadoop.fs.FileUtil
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by scordio on 1/4/17.
 */
class SparkPersistence(path: String, asText : Boolean = false) extends GraphPersistence {
  private val vertices_suffix = "_vertices"
  private val edges_suffix = "_edges"

  /**
   * Save the graph.
   *
   * @param graph
   * @param overwrite
   */
  def saveGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], overwrite :Boolean = false): Unit = {
    val verticesPath = path + vertices_suffix
    val edgesPath = path + edges_suffix

    if (overwrite) {
      FileUtil.fullyDelete(new File(verticesPath))
      FileUtil.fullyDelete(new File(edgesPath))
    }

    if (asText) {
      graph.vertices.saveAsTextFile(verticesPath)
      graph.edges.saveAsTextFile(edgesPath)
    } else {
      graph.vertices.saveAsObjectFile(verticesPath)
      graph.edges.saveAsObjectFile(edgesPath)
    }
  }
}
