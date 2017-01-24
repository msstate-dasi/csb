package edu.msstate.dasi

import java.io.File

import org.apache.hadoop.fs.FileUtil
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by scordio on 1/4/17.
 */
class SparkPersistence(sc: SparkContext, path: String, asText : Boolean = false) extends GraphPersistence {
  private val vertices_suffix = "_vertices"
  private val edges_suffix = "_edges"

  /**
   * Save the graph.
   *
   * @param graph
   * @param overwrite
   */
  override def saveGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], overwrite :Boolean = false): Unit = {
    val verticesPath = path + vertices_suffix
    val edgesPath = path + edges_suffix

    if (overwrite) {
      FileUtil.fullyDelete(new File(verticesPath))
      FileUtil.fullyDelete(new File(edgesPath))
    }

    val coalescedVertices = graph.vertices.coalesce(sc.defaultParallelism)
    val coalescedEdges = graph.edges.coalesce(sc.defaultParallelism)

    if (asText) {
      coalescedVertices.saveAsTextFile(verticesPath)
      coalescedEdges.saveAsTextFile(edgesPath)
    } else {
      coalescedVertices.saveAsObjectFile(verticesPath)
      coalescedEdges.saveAsObjectFile(edgesPath)
    }
  }
}
