package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Computes the in-degree of each vertex in the graph.
 *
 * @note Vertices with no in-edges are ignored.
 */
class InDegree(engine: Neo4jEngine) extends Workload {
  val name = "In-degree"

  /**
   * Computes the in-degree of each vertex in the graph.
   *
   * @note Vertices with no in-edges are ignored.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n)<--() RETURN n, count(*);"

    engine.run(query)
  }
}
