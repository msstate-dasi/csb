package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Collects all incoming edges for each vertex.
 */
class InEdges(engine: SparkEngine) extends Workload {
  val name = "In-edges"

  /**
   * Collects all incoming edges for each vertex.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    graph.edges.groupBy(record => record.dstId).foreach(engine.doNothing)
  }
}
