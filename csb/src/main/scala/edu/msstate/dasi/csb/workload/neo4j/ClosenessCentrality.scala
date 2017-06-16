package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
 * Closeness Centrality algorithm implementation.
 */
class ClosenessCentrality(engine: Neo4jEngine, vertex: VertexId) extends Workload {
  val name = "Closeness Centrality"

  /**
   * Runs Closeness Centrality.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (a), (b {name:\"" + vertex + "\"}) WHERE a<>b " +
      "WITH length(shortestPath((a)-[]-(b))) AS dist, a, b" +
      "RETURN DISTINCT a, sum(1.0/dist) AS close_central ORDER BY close_central DESC;"

    engine.run(query)
  }
}
