package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Connected Components algorithm implementation.
 */
class ConnectedComponents(engine: Neo4jEngine) extends Workload {
  val name = "Connected Components"

  /**
   * Runs Connected Components.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n) WITH COLLECT(n) as nodes " +
      "RETURN REDUCE(graphs = [], n in nodes | " +
      "case when " +
      "ANY (g in graphs WHERE shortestPath( (n)-[*]-(g) ) ) " +
      "then graphs " +
      "else graphs + [n]" +
      "end );"

    engine.run(query)
  }
}
