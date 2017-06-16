package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Collects the neighbors for each vertex.
 *
 * @note Vertices with no out-edges are ignored.
 */
class OutNeighbors(engine: Neo4jEngine) extends Workload {
  val name = "Out-neighbors"

  /**
   * Collects the neighbors for each vertex.
   *
   * @note Vertices with no out-edges are ignored.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n)-->(m) RETURN n, collect(m);"

    engine.run(query)
  }
}
