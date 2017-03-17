package edu.msstate.dasi.csb

import java.io.File

import org.apache.hadoop.fs.FileUtil
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.storage.StorageLevel

class SparkPersistence extends GraphPersistence {
  private val vertices_suffix = "_vertices"
  private val edges_suffix = "_edges"

  /**
   * Load a graph.
   */
  def loadGraph(graphName: String): Graph[VertexData, EdgeData] = {
    val verticesPath = graphName + vertices_suffix
    val edgesPath = graphName + edges_suffix

    val vertices = sc.objectFile[(VertexId, VertexData)](verticesPath)
    val edges = sc.objectFile[Edge[EdgeData]](edgesPath)

    Graph(vertices, edges, null.asInstanceOf[VertexData], StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * Save a graph.
   */
  def saveGraph(graph: Graph[VertexData, EdgeData], graphName: String, overwrite: Boolean = false): Unit = {
    val verticesPath = graphName + vertices_suffix
    val edgesPath = graphName + edges_suffix

    if (overwrite) {
      FileUtil.fullyDelete(new File(verticesPath))
      FileUtil.fullyDelete(new File(edgesPath))
    }

    graph.vertices.saveAsObjectFile(verticesPath)
    graph.edges.saveAsObjectFile(edgesPath)
  }

  /**
   * Load a graph from a textual representation.
   */
  def loadFromText(graphName: String): Graph[VertexData, EdgeData] = {
    val verticesPath = graphName + vertices_suffix
    val edgesPath = graphName + edges_suffix

    val verticesText = sc.textFile(verticesPath)
    val edgesText = sc.textFile(edgesPath)

    // Vertex example: (175551085347081,null)
    val verticesRegex = "[(,)]"

    val vertices = verticesText.map(line => line.replaceFirst("^" + verticesRegex, "").split(verticesRegex) match {
      case Array(id, textProperties) => (id.toLong, VertexData(textProperties))
    })

    // Edge example: Edge(230520062210,227807592450,EdgeData(udp,0.003044,116,230,SF,2,172,2,286,))
    val edgesRegex = "\\w+\\(|,"

    val edges = edgesText.map(line => line.replaceFirst("^" + edgesRegex, "").dropRight(1).split(edgesRegex, 3) match {
      case Array(srcId, dstId, textProperties) => Edge(srcId.toLong, dstId.toLong, EdgeData(textProperties))
    })

    Graph(vertices, edges, null.asInstanceOf[VertexData], StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * Save a graph as a textual representation.
   */
  def saveAsText(graph: Graph[VertexData, EdgeData], graphName: String, overwrite: Boolean = false): Unit = {
    val verticesPath = graphName + vertices_suffix
    val verticesTmpPath = "__" + verticesPath
    val edgesPath = graphName + edges_suffix
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
