package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Counts the number of edges in the graph.
 */
class CountEdges(engine: Neo4jEngine) extends Workload {
  val name = "Count edges"

  /**
   * Counts the number of edges in the graph.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH ()-->() RETURN count(*);"

    engine.run(query)
  }
}
