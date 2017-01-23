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
   * Return the current active executors count excluding the driver.
   */
  private def getActiveExecutorsCount = {
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driver = sc.getConf.get("spark.driver.host")
    allExecutors.filter( ! _.contains(driver) ).toList.length
  }

  /**
   * Save the graph.
   *
   * @param graph
   * @param overwrite
   */
  override def saveGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], overwrite :Boolean = false): Unit = {
    if (overwrite) {
      FileUtil.fullyDelete(new File(path + vertices_suffix))
      FileUtil.fullyDelete(new File(path + edges_suffix))
    }

    val numPartitions = math.max(getActiveExecutorsCount, sc.defaultMinPartitions)

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
