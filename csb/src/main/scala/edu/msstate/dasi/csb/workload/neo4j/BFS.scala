package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
 * Breadth-first Search algorithm implementation.
 */
class BFS(engine: Neo4jEngine, src: VertexId, dst: VertexId) extends Workload {
  val name = "Breadth-first Search"

  /**
   * Runs Breadth-first Search.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH path = shortestPath(({name:\"" + src + "\"})-[*]-({name:\"" + dst + "\"})) RETURN path;"

    engine.run(query)
  }
}
