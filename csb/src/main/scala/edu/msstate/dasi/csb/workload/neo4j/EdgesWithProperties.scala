package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Finds all edges with given properties.
 */
class EdgesWithProperties(engine: Neo4jEngine) extends Workload {
  val name = "Edges with properties"

  /**
   * Finds all edges with given properties.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = ???
}
