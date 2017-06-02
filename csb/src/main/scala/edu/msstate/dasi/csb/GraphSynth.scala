package edu.msstate.dasi.csb

import edu.msstate.dasi.csb.distributions.DataDistributions
import org.apache.spark.graphx.Graph

/**
 * The GraphSynth trait contains the basic operations available on all synthesis algorithms.
 */
trait GraphSynth {
  /**
   * Generates a synthetic graph with no properties starting from a seed graph.
   */
  protected def genGraph(seed: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, EdgeData]

  /**
   * Fills the properties of a synthesized graph using the property distributions of the seed.
   */
  private def genProperties(synth: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, EdgeData] = {
    val dataDistBroadcast = sc.broadcast(seedDists)

    // There is no need to invoke also mapVertices because the vertex has no properties in the current implementation
    synth.mapEdges( _ => {
      val dataDist = dataDistBroadcast.value
      val origBytes = dataDist.origBytes.sample

      EdgeData(
        ts = dataDist.ts.sample,
        origPort = dataDist.origPort.sample,
        respPort = dataDist.respPort.sample,
        proto = dataDist.proto.sample(origBytes),
        duration = dataDist.duration.sample(origBytes),
        origBytes = origBytes,
        respBytes = dataDist.respBytes.sample(origBytes),
        connState = dataDist.connState.sample(origBytes),
        origPkts = dataDist.origPkts.sample(origBytes),
        origIpBytes = dataDist.origIpBytes.sample(origBytes),
        respPkts = dataDist.respPkts.sample(origBytes),
        respIpBytes = dataDist.respIpBytes.sample(origBytes))
    })
  }

  /**
   * Synthesizes a graph from a seed graph and its properties distributions.
   */
  def synthesize(seed: Graph[VertexData, EdgeData], seedDists : DataDistributions, withProperties: Boolean): Graph[VertexData, EdgeData] = {

    var synth = null.asInstanceOf[Graph[VertexData, EdgeData]]

    Util.time( "Gen Graph", {
      synth = genGraph(seed, seedDists)
      println("Vertices #: " + synth.numVertices + ", Edges #: " + synth.numEdges)
    } )

    if (withProperties) {
      Util.time( "Gen Properties", {
        synth = genProperties(synth, seedDists)
        println("Vertices #: " + synth.numVertices + ", Edges #: " + synth.numEdges)
      } )
    }

    synth
  }
}
