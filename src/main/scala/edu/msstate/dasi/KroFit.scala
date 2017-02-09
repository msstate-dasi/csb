package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

/**
  * Kro(n)Fit
  * Created by B1nary on 1/27/2017.
  */
class KroFit(sc: SparkContext, partitions: Int, initMtxStr: String, gradIter: Int, connLog: String) extends DataParser {

    def run(): Unit = {

//      var edgeList: Array[(Long,Long)] = Array.empty[(Long,Long)]
//      var nodeList: Array[Long] = Array.empty[(Long)]
      val permSwapNodeProb = 0.2
      val scaleInitMtx = true
      var (nodeList, edgeList) = tempReadFromConn(sc, 120, connLog);
//      nodeList = tempReadFromConn(sc, 120, connLog)._1

      val lrnRate = 0.00005
      val mnStep = 0.005
      val mxStep = 0.05
      val warmUp = 10000
      val nSamples = 100000

      val initKronMtx = new kronMtx(sc, Array(.9,.7,.5,.2))

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
//      logLike = kronLL.gradDescent(gradIter, lrnRate, mnStep, mxStep, warmUp, nSamples);
      logLike = kronLL.gradDescent(100, lrnRate, mnStep, mxStep, 10000, 100000);

    }

}
