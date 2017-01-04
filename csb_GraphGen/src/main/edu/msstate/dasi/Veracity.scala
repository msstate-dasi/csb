package edu.msstate.dasi

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

/**
 * Veracity metrics helpers for [[org.apache.spark.graphx.Graph]]
 */
object Veracity {

  // TODO: the following methods could be replaced using a single method and an object EdgeDirection as input

  /**
   * Computes the neighboring vertex degrees distribution
   *
   * @param graph The graph to analyze
   * @return RDD containing degree numbers, and the number of nodes at that specific degree
   */
  def degreesDist(graph: Graph[nodeData, edgeData]): RDD[(Long, Long)] = {
    graph.degrees.map(record => (record._2.toLong, 1L)).reduceByKey(_ + _)
  }

  /**
   * Computes the neighboring vertex in-degrees distribution
   *
   * @param graph The graph to analyze
   * @return RDD containing degree numbers, and the number of nodes at that specific degree
   */
  def inDegreesDist(graph: Graph[nodeData, edgeData]): RDD[(Long, Long)] = {
    graph.inDegrees.map(record => (record._2.toLong, 1L)).reduceByKey(_ + _)
  }

  /**
   * Computes the neighboring vertex out-degrees distribution
   *
   * @param graph The graph to analyze
   * @return RDD containing degree numbers, and the number of nodes at that specific degree
   */
  def outDegreesDist(graph: Graph[nodeData, edgeData]): RDD[(Long, Long)] = {
    graph.outDegrees.map(record => (record._2.toLong, 1L)).reduceByKey(_ + _)
  }

}
