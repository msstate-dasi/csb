package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Computes the number of triangles passing through each vertex.
 */
class TriangleCounting(engine: SparkEngine) extends Workload {
  val name = "Triangle Counting"

  /**
   * Computes the number of triangles passing through each vertex.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    graph.triangleCount().vertices.foreach(engine.doNothing)
  }
}
