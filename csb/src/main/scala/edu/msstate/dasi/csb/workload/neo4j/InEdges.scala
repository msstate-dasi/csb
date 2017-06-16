package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Collects all incoming edges for each vertex.
 */
class InEdges(engine: Neo4jEngine) extends Workload {
  val name = "In-edges"

  /**
   * Collects all incoming edges for each vertex.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n)<-[r]-() RETURN n, collect(r);"

    engine.run(query)
  }
}
