package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Finds all edges with given properties.
 */
class EdgesWithProperties(engine: SparkEngine) extends Workload {
  val name = "Edges with properties"

  /**
   * Finds all edges with given properties.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = ???

//  /**
//   * Finds all edges with a given property.
//   */
//  def edgesWithProperty[VD: ClassTag](graph: Graph[VD, EdgeData], filter: Edge[EdgeData] => Boolean): Unit = {
//    graph.edges.filter(filter).foreach(doNothing)
//  }
}
