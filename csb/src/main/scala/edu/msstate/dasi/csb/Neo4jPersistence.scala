package edu.msstate.dasi.csb

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by scordio on 1/4/17.
 */
class Neo4jPersistence() extends GraphPersistence {

  def loadGraph[VD: ClassTag, ED: ClassTag](name: String): Graph[VD, ED] = {
    null
  }

  /**
   * Save the graph
   */
  def saveGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], name: String, overwrite :Boolean = false): Unit = {
    if (overwrite) {
      // delete existing graph in the DB
    }

    // save graph to the DB
  }
}
