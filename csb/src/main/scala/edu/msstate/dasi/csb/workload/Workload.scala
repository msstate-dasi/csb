package edu.msstate.dasi.csb.workload

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

trait Workload {
  val name: String

  /**
   * Runs a workload on top of a graph.
   *
   * @param graph the target graph
   * @tparam VD the vertex attribute type
   * @tparam ED the edge attribute type
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit
}
