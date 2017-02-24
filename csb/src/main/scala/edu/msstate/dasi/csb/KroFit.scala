package edu.msstate.dasi.csb

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Kro(n)Fit
  * Created by spencer on 1/27/2017.
  */
object KroFit {

    def run(G: Graph[VertexData, EdgeData], gradIter: Int = 100, lrnRate: Double = 0.00005, mnStep: Double = 0.005,
            mxStep: Double = 0.05, warmUp: Int = 10000, nSamples: Int = 100000, inMtx: Array[Double] = Array(.9,.7,.5,.2)): Array[Array[Double]] = {

//      val edgeList: RDD[(Long, Long)] = G.edges.map(record => (record.srcId, record.dstId))
//      val nodeList: RDD[Long] = G.vertices.map(record => record._1)

//      val tempNodes = sc.parallelize(Array(0L,1L,2L,3L))
//      val tempEdges = sc.parallelize(Array((0L,1L),(1L,2L),(3L,2L),(2L, 0L), (1L, 3L)))


      var newGraph = Util.convertLabelsToStandardForm(G)
      val edgeList = newGraph.edges.map(record => (record.srcId, record.dstId))
      val nodeList = newGraph.vertices.map(record => record._1)
//      val (edgeList, nodeList) = (tempEdges, tempNodes)

      val permSwapNodeProb = 0.2
      val scaleInitMtx = true

      /*
      val lrnRate = 0.00005
      val mnStep = 0.005
      val mxStep = 0.05
      val warmUp = 10000
      val nSamples = 100000

      val initKronMtx = new kronMtx(sc, )
      */

      val initKronMtx = new kronMtx(inMtx)

      println("INIT PARAM")
      initKronMtx.dump()

      val kronLL = new kroneckerLL(edgeList, nodeList, initKronMtx, permSwapNodeProb)

      if(scaleInitMtx)
      {
        kronLL.kronIters = initKronMtx.setForEdges(kronLL.nodes, kronLL.edges) //we very much need this
      }
      kronLL.InitLL(edgeList, nodeList, initKronMtx)

      initKronMtx.dump()

      kronLL.setPerm()

      var logLike: Double = 0
      logLike = kronLL.gradDescent(gradIter, lrnRate, mnStep, mxStep, warmUp, nSamples)

//      logLike = kronLL.gradDescent(100, lrnRate, mnStep, mxStep, 10000, 100000);
      val fittedMtx = kronLL.probMtx.seedMtx
      val mtxDim = fittedMtx.length / 2
      val result = Array.ofDim[Double](mtxDim, mtxDim)

      for (i <- 0 until mtxDim)
        for (j <- 0 until mtxDim) {
          result(i)(j) = fittedMtx(i+j)
        }

      return result
    }



}
