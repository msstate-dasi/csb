package edu.msstate.dasi

import java.io.File

import org.apache.hadoop.fs.FileUtil
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by scordio on 1/4/17.
 */
class SparkPersistence(path: String, asText : Boolean = false) extends GraphPersistence {
  private val numPartitions = 16
  private val vertices_suffix = "_vertices"
  private val edges_suffix = "_edges"

  /**
   * Save the graph
   *
   * @param graph
   * @param overwrite
   */
  override def saveGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], overwrite :Boolean = false): Unit = {
    if (overwrite) {
      FileUtil.fullyDelete(new File(path + vertices_suffix))
      FileUtil.fullyDelete(new File(path + edges_suffix))
    }

    val coalescedVertices = graph.vertices.coalesce(numPartitions)
    val coalescedEdges = graph.edges.coalesce(numPartitions)

    if (asText) {
      coalescedVertices.saveAsTextFile(path + vertices_suffix)
      coalescedEdges.saveAsTextFile(path + edges_suffix)
    } else {
      coalescedVertices.saveAsObjectFile(path + vertices_suffix)
      coalescedEdges.saveAsObjectFile(path + edges_suffix)
    }
  }
}
