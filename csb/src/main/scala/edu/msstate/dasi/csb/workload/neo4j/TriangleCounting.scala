package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Computes the number of triangles passing through each vertex.
 */
class TriangleCounting(engine: Neo4jEngine) extends Workload {
  val name = "Triangle Counting"

  /**
   * Computes the number of triangles passing through each vertex.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n)-->()-->()-->(n) RETURN n, count(*);"

    engine.run(query)
  }
}
