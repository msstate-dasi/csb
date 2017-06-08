package edu.msstate.dasi.csb

import edu.msstate.dasi.csb.model.{EdgeData, VertexData}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Kro(n)Fit
  * Created by spencer on 1/27/2017.
  */

/**
  * KronFit object that fits a seed graph into a Kronecker Matrix
  */
object KroFit {

  /**
    * Method to perform Gradient Descent on a seed graph and output the resulting seed matrix.
    * @param G Graph to use as the seed graph.
    * @param gradIter Gradient iterations to use during fitting.
    * @param lrnRate Percent to learn from gradient each iteration.
    * @param mnStep Minimum value to compare with learning rate and adjust learning rate accordingly.
    * @param mxStep Maximum value to compare with learning rate and adjust learning rate accordingly.
    * @param warmUp Number of samples to perform as a warm-up.
    * @param nSamples Number of samples to perform during fitting.
    * @param inMtx Initial matrix to start gradient descent with.
    * @return
    */
    def run(G: Graph[VertexData, EdgeData], gradIter: Int = 100, lrnRate: Double = 0.00005, mnStep: Double = 0.005,
            mxStep: Double = 0.05, warmUp: Int = 10000, nSamples: Int = 100000, inMtx: Array[Double] = Array(.9,.7,.5,.2)): Array[Array[Double]] = {

      var newGraph = Util.convertLabelsToStandardForm(G)
      newGraph = Util.stripMultiEdges(newGraph)
      val edgeList = newGraph.edges.map(record => (record.srcId, record.dstId))
      val nodeList = newGraph.vertices.map(record => record._1)

      val permSwapNodeProb = 0.2
      val scaleInitMtx = true

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
        for (j <- 0 until mtxDim)
        {
          result(i)(j) = fittedMtx(i+j)
        }

      return result
    }
}
