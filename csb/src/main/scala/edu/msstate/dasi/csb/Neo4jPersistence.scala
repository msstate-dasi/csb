package edu.msstate.dasi.csb

import org.apache.spark.graphx.Graph
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}

class Neo4jPersistence(uri: String, username: String, password: String) extends GraphPersistence {
  /**
   * Load a graph.
   */
  def loadGraph(name: String): Graph[VertexData, EdgeData] = {
    null
  }

  /**
   * Save a graph.
   */
  def saveGraph(graph: Graph[VertexData, EdgeData], name: String, overwrite :Boolean = false): Unit = {
    val driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password))
    val session = driver.session()

    // graph.edges should be replaced by graph.triplets if VertexData has fields

    session.run(s"CREATE CONSTRAINT ON (v:$name) ASSERT v.name IS UNIQUE")

    val edge = graph.edges.first()

    session.run(s"CREATE (:$name { name:${edge.srcId} })")
    session.run(s"CREATE (:$name { name:${edge.dstId} })")

    println("CREATE (src)-[:EDGE { " + edge.attr.toNeo4jString + " }]->(dst)")

    session.run(s"MATCH (src:$name { name:${edge.srcId} }), (dst:$name { name:${edge.dstId} }) " +
      "CREATE (src)-[:EDGE { " + edge.attr.toNeo4jString + " }]->(dst)")

//    if (overwrite) {
//      // delete existing graph in the DB
//    }

    // save graph to the DB
  }
}
