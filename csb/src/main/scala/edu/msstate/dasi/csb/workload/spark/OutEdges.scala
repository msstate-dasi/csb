package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Collects all outgoing edges for each vertex.
 */
class OutEdges(engine: SparkEngine) extends Workload {
  val name = "Out-edges"

  /**
   * Collects all outgoing edges for each vertex.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    graph.edges.groupBy(record => record.srcId).foreach(engine.doNothing)
  }
}
