package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * PageRank algorithm implementation.
 */
class PageRank(engine: SparkEngine) extends Workload {
  val name = "PageRank"

  /**
   * Runs PageRank.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val tol = 0.001
    graph.pageRank(tol).vertices.foreach(engine.doNothing)
  }
}
