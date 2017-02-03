package edu.msstate.dasi.csb

import java.io.File

import org.apache.hadoop.fs.FileUtil
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class SparkPersistence() extends GraphPersistence {
  private val vertices_suffix = "_vertices"
  private val edges_suffix = "_edges"

  def loadGraph[VD: ClassTag, ED: ClassTag](name: String): Graph[VD, ED] = {
    val verticesPath = name + vertices_suffix
    val edgesPath = name + edges_suffix

    val vertices = sc.objectFile[(VertexId, VD)](verticesPath)
    val edges = sc.objectFile[Edge[ED]](edgesPath)

    Graph(
      vertices,
      edges,
      null.asInstanceOf[VD],
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK
    )
  }

  /**
   * Save the graph.
   */
  def saveGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], name: String, overwrite :Boolean = false): Unit = {
    val verticesPath = name + vertices_suffix
    val edgesPath = name + edges_suffix

    if (overwrite) {
      FileUtil.fullyDelete(new File(verticesPath))
      FileUtil.fullyDelete(new File(edgesPath))
    }

    graph.vertices.saveAsObjectFile(verticesPath)
    graph.edges.saveAsObjectFile(edgesPath)
  }

  /**
   * Save the graph.
   */
  def saveAsText[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], name: String, overwrite :Boolean = false): Unit = {
    val verticesPath = name + vertices_suffix
    val verticesTmpPath = "__" + verticesPath
    val edgesPath = name + edges_suffix
    val edgesTmpPath = "__" + edgesPath

    if (overwrite) {
      FileUtil.fullyDelete(new File(verticesPath))
      FileUtil.fullyDelete(new File(edgesPath))
    }

    graph.vertices.saveAsTextFile(verticesTmpPath)
    Util.merge(verticesTmpPath, verticesPath)
    FileUtil.fullyDelete(new File(verticesTmpPath))

    graph.edges.saveAsTextFile(edgesTmpPath)
    Util.merge(edgesTmpPath, edgesPath)
    FileUtil.fullyDelete(new File(edgesTmpPath))
  }
}
