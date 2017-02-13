package edu.msstate.dasi.csb

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

trait GraphPersistence {

  /**
   * Load a graph.
   */
  def loadGraph[VD: ClassTag, ED: ClassTag](name: String): Graph[VD, ED]

  /**
   * Save a graph.
   */
  def saveGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], name: String, overwrite :Boolean = false): Unit
}
