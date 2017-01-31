package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * The GraphSynth trait contains the basic operations available on all synthesis algorithms.
 */
trait GraphSynth {
  /**
   * Generates a synthetic graph with no properties starting from a seed graph.
   */
  protected def genGraph(sc: SparkContext, seed: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, EdgeData]

  /**
   * Fills the properties of a synthesized graph using the property distributions of the seed.
   */
  private def genProperties(sc: SparkContext, synth: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, EdgeData] = {
    val dataDistBroadcast = sc.broadcast(seedDists)

    synth
      .mapVertices(
        (_, _) => {
          val dataDist = dataDistBroadcast.value
          VertexData(
            dataDist.getIpSample
          )
        }
      )
      .mapEdges(
        _ => {
          val dataDist = dataDistBroadcast.value
          val origBytes = dataDist.getOrigBytesSample
          EdgeData(
            "",
            dataDist.getProtoSample(origBytes),
            dataDist.getDurationSample(origBytes),
            origBytes,
            dataDist.getRespBytesSample(origBytes),
            dataDist.getConnectionStateSample(origBytes),
            dataDist.getOrigPktsSample(origBytes),
            dataDist.getOrigIPBytesSample(origBytes),
            dataDist.getRespPktsSample(origBytes),
            dataDist.getRespIPBytesSample(origBytes),
            dataDist.getDescSample(origBytes)
          )
        }
      )
  }

  /**
   * Synthesizes a graph from a seed graph and its properties distributions.
   */
  def synthesize(sc: SparkContext, seed: Graph[VertexData, EdgeData], seedDists : DataDistributions, withProperties: Boolean): Graph[VertexData, EdgeData] = {
    var startTime = System.nanoTime()

    var synth = genGraph(sc, seed, seedDists)
    println("Vertices #: " + synth.numVertices + ", Edges #: " + synth.numEdges)

    var timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()

    if (withProperties) {
      startTime = System.nanoTime()
      println()
      println("Generating Edge and Node properties")

      synth = genProperties(sc, synth, seedDists)

      println("Vertices #: " + synth.numVertices + ", Edges #: " + synth.numEdges)

      timeSpan = (System.nanoTime() - startTime) / 1e9
      println("Finished generating Edge and Node Properties. Total time elapsed: " + timeSpan.toString)
    }

    synth
  }
}
