package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Strongly Connected Components algorithm implementation.
 */
class StronglyConnectedComponents(engine: SparkEngine, iterations: Int) extends Workload {
  val name = "Strongly Connected Components"

  /**
   * Runs Strongly Connected Components.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    graph.stronglyConnectedComponents(iterations).vertices.foreach(engine.doNothing)
  }
}
