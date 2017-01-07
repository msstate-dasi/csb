package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import org.neo4j.spark._

/**
 * Created by scordio on 1/4/17.
 */
class Neo4jPersistence(sc: SparkContext) extends GraphPersistence {
  /**
   * Save the graph
   *
   * @param graph
   */
  override def saveGraph(graph: Graph[nodeData, edgeData]): Unit = {
    Neo4jGraph.saveGraph(sc, graph)
  }
}
