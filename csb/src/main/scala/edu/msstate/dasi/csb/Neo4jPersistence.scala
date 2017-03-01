package edu.msstate.dasi.csb

import org.apache.spark.graphx.Graph

//import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Values}

import org.neo4j.spark.Executor
import org.neo4j.spark.Neo4jConfig

import scala.collection.JavaConverters._

class Neo4jPersistence() extends GraphPersistence {
  /**
   * Load a graph.
   */
  def loadGraph(graphName: String): Graph[VertexData, EdgeData] = {
    val config = Neo4jConfig(sc.getConf)

    Util.time( "Load edges from neo4j", {
      val query = s"MATCH (src:$graphName)-[edge:EDGE]-(dst:$graphName) RETURN"
    })
    null
  }

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

        val query = s"UNWIND {vertices} AS vertex " + s"CREATE (:$graphName { name:vertex.name ${VertexData.neo4jTemplate("vertex")} })"

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
          s"CREATE (src)-[:EDGE {${EdgeData.neo4jTemplate("edge")}} ]->(dst)"

        Executor.execute(config, query, Map("edges" -> edges))
      })
    })
  }
}
