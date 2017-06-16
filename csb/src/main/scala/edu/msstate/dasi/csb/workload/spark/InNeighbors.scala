package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.{EdgeDirection, Graph}

import scala.reflect.ClassTag

/**
 * Collects the neighbors for each vertex.
 *
 * @note Vertices with no in-edges are ignored.
 */
class InNeighbors(engine: SparkEngine) extends Workload {
  val name = "In-neighbors"

  /**
   * Collects the neighbors for each vertex.
   *
   * @note Vertices with no in-edges are ignored.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    graph.collectNeighbors(EdgeDirection.In).foreach(engine.doNothing)
  }
}
