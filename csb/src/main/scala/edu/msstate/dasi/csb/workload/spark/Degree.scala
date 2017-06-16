package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Computes the degree of each vertex in the graph.
 *
 * @note Vertices with no edges are ignored.
 */
class Degree(engine: SparkEngine) extends Workload {
  val name = "Degree"

  /**
   * Computes the degree of each vertex in the graph.
   *
   * @note Vertices with no edges are ignored.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = graph.degrees.foreach(engine.doNothing)
}
