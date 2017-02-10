package edu.msstate.dasi.csb

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

/**
  * Kro(n)Fit
  * Created by spencer on 1/27/2017.
  */
class KroFit(sc: SparkContext, partitions: Int, initMtxStr: String, gradIter: Int, connLog: String, inMtx: Array[Double]) {

    def run(sc:SparkContext, G: Graph[EdgeData, Long], lrnRate: Double, mnStep: Double, mxStep: Double, warmUp: Int, nSamples: Int): Unit = {

      val edgeList: Array[(Long, Long)] = G.edges.map(record => (record.srcId, record.dstId)).collect()
      val nodeList: Array[Long] = G.vertices.map(record => (record._1)).collect()

      val permSwapNodeProb = 0.2
      val scaleInitMtx = true

      /*
      val lrnRate = 0.00005
      val mnStep = 0.005
      val mxStep = 0.05
      val warmUp = 10000
      val nSamples = 100000

      val initKronMtx = new kronMtx(sc, Array(.9,.7,.5,.2))
      */

      val initKronMtx = new kronMtx(sc, inMtx)

      println("INIT PARAM")
      initKronMtx.dump()

      val kronLL = new kroneckerLL(sc, edgeList, nodeList, initKronMtx, permSwapNodeProb)

      if(scaleInitMtx)
      {
        kronLL.kronIters = initKronMtx.setForEdges(kronLL.nodes, kronLL.edges) //we very much need this
      }
      kronLL.InitLL(edgeList, nodeList, initKronMtx)

      initKronMtx.dump()

      kronLL.setPerm()

      var logLike: Double = 0
      logLike = kronLL.gradDescent(gradIter, lrnRate, mnStep, mxStep, warmUp, nSamples);

//      logLike = kronLL.gradDescent(100, lrnRate, mnStep, mxStep, 10000, 100000);

    }

}
