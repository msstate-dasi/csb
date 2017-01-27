package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/***
  * The GraphSynth trait contains the basic operations available on all synthesis algorithms.
  */
trait GraphSynth {

  /***
   * Generates a graph with empty properties from a seed graph.
   */
  protected def genGraph(seed: Graph[nodeData, edgeData], seedDists : DataDistributions): Graph[nodeData, edgeData]

  /***
   * Fills the properties of a synthesized graph using the property distributions of the seed.
   */
  private def genProperties(sc: SparkContext, synth: Graph[nodeData, edgeData], seedDists : DataDistributions): Graph[nodeData, edgeData] = {
    val dataDistBroadcast = sc.broadcast(seedDists)

    val eRDD: RDD[Edge[edgeData]] = synth.edges.map(record => Edge(record.srcId, record.dstId, {
      val ORIGBYTES = dataDistBroadcast.value.getOrigBytesSample
      val ORIGIPBYTE = dataDistBroadcast.value.getOrigIPBytesSample(ORIGBYTES)
      val CONNECTSTATE = dataDistBroadcast.value.getConnectionStateSample(ORIGBYTES)
      val PROTOCOL = dataDistBroadcast.value.getProtoSample(ORIGBYTES)
      val DURATION = dataDistBroadcast.value.getDurationSample(ORIGBYTES)
      val ORIGPACKCNT = dataDistBroadcast.value.getOrigPktsSample(ORIGBYTES)
      val RESPBYTECNT = dataDistBroadcast.value.getRespBytesSample(ORIGBYTES)
      val RESPIPBYTECNT = dataDistBroadcast.value.getRespIPBytesSample(ORIGBYTES)
      val RESPPACKCNT = dataDistBroadcast.value.getRespPktsSample(ORIGBYTES)
      val DESC = dataDistBroadcast.value.getDescSample(ORIGBYTES)
      edgeData("", PROTOCOL, DURATION, ORIGBYTES, RESPBYTECNT, CONNECTSTATE, ORIGPACKCNT, ORIGIPBYTE, RESPPACKCNT, RESPIPBYTECNT, DESC)
    }))
    val vRDD: RDD[(VertexId, nodeData)] = synth.vertices.map(record => (record._1, {
      val DATA = dataDistBroadcast.value.getIpSample
      nodeData(DATA)
    }))
    val newSynth = Graph(vRDD, eRDD, nodeData())

    newSynth
  }

  /***
   * Synthesizes a graph from a seed graph and its properties distributions.
   */
  def synthesize(sc: SparkContext, seed: Graph[nodeData, edgeData], seedDists : DataDistributions, withProperties: Boolean): Graph[nodeData, edgeData] = {
    var startTime = System.nanoTime()

    var synth = genGraph(seed, seedDists)
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

      // TODO: a RDD action should precede the following in order to have a significant timeSpan
      timeSpan = (System.nanoTime() - startTime) / 1e9

      println("Finished generating Edge and Node Properties. Total time elapsed: " + timeSpan.toString)
    }

    synth
  }
}
