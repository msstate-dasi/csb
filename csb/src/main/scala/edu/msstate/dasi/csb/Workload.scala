package edu.msstate.dasi.csb

import org.apache.spark.graphx.{Graph, VertexRDD}

import scala.reflect.ClassTag

object Workload {
  /**
   * The number of vertices in the graph.
   */
  def countVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Long = graph.numVertices

  /**
   * The number of edges in the graph.
   */
  def countEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Long = graph.numEdges

  /**
   * The degree of each vertex in the graph.
   */
  def degree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Int] = graph.degrees

  /**
   * The in-degree of each vertex in the graph.
   */
  def inDegree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Int] = graph.inDegrees

  /**
   * The out-degree of each vertex in the graph.
   */
  def outDegree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Int] = graph.outDegrees

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   */
  def pageRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] = {
    graph.pageRank(tol, resetProb)
  }
}
