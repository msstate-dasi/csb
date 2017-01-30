package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/***
  * The GraphSynth trait contains the basic operations available on all synthesis algorithms.
  */
trait GraphSynth {
  /***
   * Generates a synthetic graph with no properties starting from a seed graph.
   */
  protected def genGraph(sc: SparkContext, seed: Graph[VertexData, EdgeData], seedDists : DataDistributions): Graph[VertexData, Int]

  /***
   * Fills the properties of a synthesized graph using the property distributions of the seed.
   */
  private def genProperties(sc: SparkContext, synthNoProp: Graph[VertexData, Int], seedDists : DataDistributions): Graph[VertexData, EdgeData] = {
    val dataDistBroadcast = sc.broadcast(seedDists)

    val eRDD: RDD[Edge[EdgeData]] = synthNoProp.edges.map(record => Edge(record.srcId, record.dstId, {
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
      EdgeData("", PROTOCOL, DURATION, ORIGBYTES, RESPBYTECNT, CONNECTSTATE, ORIGPACKCNT, ORIGIPBYTE, RESPPACKCNT, RESPIPBYTECNT, DESC)
    }))
    val vRDD: RDD[(VertexId, VertexData)] = synthNoProp.vertices.map(record => (record._1, {
      val DATA = dataDistBroadcast.value.getIpSample
      VertexData(DATA)
    }))
    val synth = Graph(vRDD, eRDD, VertexData())

    synth
  }

  /***
   * Fills the properties of a synthesized graph using empty data.
   */
  private def genProperties(synthNoProp: Graph[VertexData, Int]): Graph[VertexData, EdgeData] = {
    val eRDD: RDD[Edge[EdgeData]] = synthNoProp.edges.map(record => Edge(record.srcId, record.dstId, EdgeData()))
    val vRDD: RDD[(VertexId, VertexData)] = synthNoProp.vertices.map(record => (record._1, VertexData()))
    val synth = Graph(vRDD, eRDD, VertexData())

    synth
  }

  /***
   * Synthesizes a graph from a seed graph and its properties distributions.
   */
  def synthesize(sc: SparkContext, seed: Graph[VertexData, EdgeData], seedDists : DataDistributions, withProperties: Boolean): Graph[VertexData, EdgeData] = {
    var startTime = System.nanoTime()

    val synthNoProp = genGraph(sc, seed, seedDists)
    println("Vertices #: " + synthNoProp.numVertices + ", Edges #: " + synthNoProp.numEdges)

    var timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished generating graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println()


    startTime = System.nanoTime()
    println()
    println("Generating Edge and Node properties")

    if (withProperties) {
      val synth = genProperties(sc, synthNoProp, seedDists)

      println("Vertices #: " + synth.numVertices + ", Edges #: " + synth.numEdges)

      timeSpan = (System.nanoTime() - startTime) / 1e9
      println("Finished generating Edge and Node Properties. Total time elapsed: " + timeSpan.toString)

      synth
    } else {
      val synth = genProperties(synthNoProp)

      println("Vertices #: " + synth.numVertices + ", Edges #: " + synth.numEdges)

      timeSpan = (System.nanoTime() - startTime) / 1e9
      println("Finished generating Edge and Node Properties. Total time elapsed: " + timeSpan.toString)

      synth
    }
  }
}
