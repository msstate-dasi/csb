package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Computes the in-degree of each vertex in the graph.
 *
 * @note Vertices with no in-edges are ignored.
 */
class InDegree(engine: SparkEngine) extends Workload {
  val name = "In-degree"

  /**
   * Computes the in-degree of each vertex in the graph.
   *
   * @note Vertices with no in-edges are ignored.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = graph.inDegrees.foreach(engine.doNothing)
}
