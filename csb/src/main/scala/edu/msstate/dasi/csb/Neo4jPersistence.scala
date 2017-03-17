package edu.msstate.dasi.csb

import java.io._

import org.apache.hadoop.fs.FileUtil
import org.apache.spark.graphx.Graph

//import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Values}

import org.neo4j.spark.Executor
import org.neo4j.spark.Neo4jConfig

import scala.collection.JavaConverters._

import sys.process._

class Neo4jPersistence() extends GraphPersistence {
  private val vertices_suffix = "_nodes"
  private val edges_suffix = "_relationships"

  /**
   * Load a graph.
   */
  def loadGraph(graphName: String): Graph[VertexData, EdgeData] = ???

  /**
   * Save a graph.
   */
  def saveGraph(graph: Graph[VertexData, EdgeData], graphName: String, overwrite :Boolean = false): Unit = {
    if (overwrite) {
      // delete existing graph in the DB
    }

    val config = Neo4jConfig(sc.getConf)


    val query = s"CREATE CONSTRAINT ON (v:$graphName) ASSERT v.name IS UNIQUE"
    Executor.execute(config, query, Map("parameters" -> ""))

    Util.time( "Save vertices to neo4j", {
      graph.vertices.foreachPartition(partition => {
        val vertices = partition.map {
          case (vertexId, vertexData: VertexData) => (Map("name" -> vertexId) ++ vertexData.toMap).asJava
          case (vertexId, _) => (Map("name" -> vertexId) ++ VertexData.toNullMap).asJava
        }.toList.asJava

        val query = s"UNWIND {vertices} AS vertex " + s"CREATE (:$graphName { name:vertex.name ${VertexData.neo4jQueryTemplate("vertex")} })"

        Executor.execute(config, query, Map("vertices" -> vertices))
      })
    })

    Util.time( "Save edges to neo4j", {

      /** Runs in parallel - High risk of deadlocks ------------------------------------------------------------------*/
      graph.edges.foreachPartition( partition => {
        val edges = partition.map( edge =>
          edge.attr match {
            case edgeData: EdgeData => (Map("srcId" -> edge.srcId, "dstId" -> edge.dstId) ++ edgeData.toMap).asJava
            case _ => (Map("srcId" -> edge.srcId, "dstId" -> edge.dstId) ++ EdgeData.toNullMap).asJava
          }
        ).toList.asJava
        /**-------------------------------------------------------------------------------------------------------------*/

        /** Runs sequentially - Slow -----------------------------------------------------------------------------------*/
        //      val groupSize = graph.edges.mapPartitions(iter => Array(iter.size).iterator, preservesPartitioning = true).max()
        //
        //      for (edgeGroup <- graph.edges.toLocalIterator.grouped(groupSize)) {
        //        val edges = edgeGroup.map( edge =>
        //          edge.attr match {
        //            case edgeData: EdgeData => (Map("srcId" -> edge.srcId, "dstId" -> edge.dstId) ++ edgeData.toMap).asJava
        //            case _ => (Map("srcId" -> edge.srcId, "dstId" -> edge.dstId) ++ EdgeData.toNullMap).asJava
        //          }
        //        ).toList.asJava
        /**-------------------------------------------------------------------------------------------------------------*/

        val query = s"UNWIND {edges} AS edge MATCH (src:$graphName { name:edge.srcId }), (dst:$graphName { name:edge.dstId }) " +
          s"CREATE (src)-[:EDGE {${EdgeData.neo4jQueryTemplate("edge")}} ]->(dst)"

        Executor.execute(config, query, Map("edges" -> edges))
      })
    })
  }

  /**
   * Load a graph from a textual representation.
   */
  def loadFromText(graphName: String): Graph[VertexData, EdgeData] = ???

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

  def useImportTool(graph: Graph[VertexData, EdgeData], graphName: String, overwrite :Boolean = false): Unit = {
    val vertices_suffix = "_nodes"
    val edges_suffix = "_relationships"

    val verticesPath = graphName + vertices_suffix
    val verticesHeaderPath = verticesPath + "-header"
    val edgesPath = graphName + edges_suffix
    val edgesHeaderPath = edgesPath + "-header"

    if (s"mkfifo $verticesHeaderPath".! != 0) throw new Exception(s"Failed to create $verticesHeaderPath fifo")
    if (s"mkfifo $verticesPath".! != 0) throw new Exception(s"Failed to create $verticesPath fifo")
    if (s"mkfifo $edgesHeaderPath".! != 0) throw new Exception(s"Failed to create $edgesHeaderPath fifo")
    if (s"mkfifo $edgesPath".! != 0) throw new Exception(s"Failed to create $edgesPath fifo")
    println(s"neo4j-admin import --database=$graphName.db --nodes " + "\"" + verticesHeaderPath + "," + verticesPath +"\"")
    val importTool = (s"neo4j-admin import --database=$graphName.db --nodes " + "\"" + verticesHeaderPath + "," + verticesPath +"\"").run()

    var pipe = new DataOutputStream(new FileOutputStream(verticesHeaderPath))
    pipe.writeChars(s"name:ID,:LABEL\n")
    pipe.close()

    pipe = new DataOutputStream(new FileOutputStream(verticesPath))
    for ( (vertexId, _) <- graph.vertices.toLocalIterator ) {
      pipe.writeChars(s"$vertexId,$graphName\n")
    }
    pipe.close()

    //    pipe = new DataOutputStream(new FileOutputStream(edgesHeaderPath))
    //    pipe.writeChars(s"name:ID($graphName),:LABEL\n")
    //    pipe.close()

    if (importTool.exitValue() != 0) throw new Exception(s"Failed to import $graphName")

    FileUtil.fullyDelete(new File(verticesHeaderPath))
    FileUtil.fullyDelete(new File(verticesPath))
  }
}
