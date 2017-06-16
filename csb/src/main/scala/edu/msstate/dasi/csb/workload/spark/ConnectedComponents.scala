package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Connected Components algorithm implementation.
 */
class ConnectedComponents(engine: SparkEngine) extends Workload {
  val name = "Connected Components"

  /**
   * Runs Connected Components.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    graph.connectedComponents().vertices.foreach(engine.doNothing)
  }
}
