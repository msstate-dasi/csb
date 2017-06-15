package edu.msstate.dasi.csb.persistence

import java.io.{File, PrintWriter}

import edu.msstate.dasi.csb.model.{EdgeData, VertexData}
import edu.msstate.dasi.csb.util.Util
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.graphx.Graph

object Neo4jPersistence extends GraphPersistence {
  private val vertices_suffix = "_nodes"
  private val edges_suffix = "_relationships"

  /**
   * Load a graph.
   */
  def loadGraph(graphName: String, partitions: Int): Graph[VertexData, EdgeData] = ???

  /**
   * Save a graph.
   */
  def saveGraph(graph: Graph[VertexData, EdgeData], graphName: String, overwrite :Boolean = false): Unit = ???

  /**
   * Load a graph from a textual representation.
   */
  def loadFromText(graphName: String, partitions: Int): Graph[VertexData, EdgeData] = ???

  /**
   * Save a graph as a textual representation.
   *
   * The graph will be saved to a format compatible with the "neo4j-admin import" tool.
   * The following files will be produced:
   * * The nodes header
   * * The actual nodes
   * * The edges header
   * * The actual edges
   */
  def saveAsText(graph: Graph[VertexData, EdgeData], graphName: String, overwrite :Boolean = false): Unit = {
    val verticesPath = graphName + vertices_suffix
    val verticesTmpPath = "__" + verticesPath
    val edgesPath = graphName + edges_suffix
    val edgesTmpPath = "__" + edgesPath

    if (overwrite) {
      FileUtil.fullyDelete(new File(verticesPath + "-header"))
      FileUtil.fullyDelete(new File(verticesPath))
      FileUtil.fullyDelete(new File(edgesPath + "-header"))
      FileUtil.fullyDelete(new File(edgesPath))
    }

    val nodeHeader = s"name:ID($graphName),:LABEL\n"

    val nodeHeaderWriter = new PrintWriter(new File(verticesPath + "-header"))
    nodeHeaderWriter.write(nodeHeader)
    nodeHeaderWriter.close()

    graph.vertices.map {
      case (id, _) => s"$id,$graphName"
    }.saveAsTextFile(verticesTmpPath)

    Util.merge(verticesTmpPath, verticesPath)
    FileUtil.fullyDelete(new File(verticesTmpPath))

    val relationshipHeader = s":START_ID($graphName),:END_ID($graphName),:TYPE,${EdgeData.neo4jCsvHeader}\n"

    val relHeaderWriter = new PrintWriter(new File(edgesPath + "-header"))
    relHeaderWriter.write(relationshipHeader)
    relHeaderWriter.close()

    graph.edges.map(edge =>
      edge.attr match {
        case edgeData: EdgeData => s"${edge.srcId},${edge.dstId},EDGE,${edgeData.toCsv}"
        case _ => s"${edge.srcId},${edge.dstId},EDGE"
      }
    ).saveAsTextFile(edgesTmpPath)

    Util.merge(edgesTmpPath, edgesPath)
    FileUtil.fullyDelete(new File(edgesTmpPath))
  }
}
