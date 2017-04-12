package edu.msstate.dasi.csb

import org.apache.spark.graphx.Graph

class ParallelKroSynth(partitions: Int, genIter: Int) extends GraphSynth {

  /**
   * Generates a graph using a parallel implementation of the deterministic Kronecker algorithm.
   */
  private def parallelKro(seed: Graph[VertexData, EdgeData], seedDists: DataDistributions): Graph[VertexData,EdgeData] = {
    seed
  }

  /**
   * Generates a synthetic graph with no properties starting from a seed graph.
   */
  protected def genGraph(seed: Graph[VertexData, EdgeData], seedDists: DataDistributions): Graph[VertexData, EdgeData] = {
    parallelKro(seed, seedDists)
  }
}
