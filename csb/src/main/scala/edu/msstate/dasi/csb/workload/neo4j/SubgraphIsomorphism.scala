package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Finds one or more subgraphs of the graph which are isomorphic to a pattern.
 */
class SubgraphIsomorphism(engine: Neo4jEngine) extends Workload {
  val name = "Subgraph Isomorphism"

  /**
   * Finds one or more subgraphs of the graph which are isomorphic to a pattern.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val patternLabel = "pattern"
    val graphLabel = "graph"

    val query = "CALL csb.subgraphIsomorphism(\"" + patternLabel + "\", \"" + graphLabel + "\")"

    engine.run(query)
  }
}
