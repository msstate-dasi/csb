package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
 * Single Source Shortest Path algorithm implementation.
 */
class SSSP(engine: Neo4jEngine, src: VertexId) extends Workload {
  val name = "Single Source Shortest Path"

  /**
   * Runs Single Source Shortest Path.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (src {name:\"" + src + "\"}), (dst), " +
      "path = shortestPath((src)-[*]->(dst)) " +
      "WHERE dst.name <> src.name " +
      "RETURN src, dst, path;"

    engine.run(query)
  }
}
