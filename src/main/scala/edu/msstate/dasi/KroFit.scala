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

      var edgeList: Array[(Long,Long)] = Array.empty[(Long,Long)]
      var nodeList: Array[Long] = null
      val permSwapNodeProb = 0.2
      val scaleInitMtx = true
      edgeList = tempReadFromConn(sc, 120, "as20graph.txt")._2.collect()
      nodeList = tempReadFromConn(sc, 120, "as20graph.txt")._1.collect()

      val lrnRate = 0
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
        initKronMtx.setForEdges(kronLL.nodes, kronLL.edges)
      }
      kronLL.InitLL(edgeList, nodeList, initKronMtx)

      initKronMtx.dump()

      kronLL.setPerm()


      var logLike: Double = 0
//      logLike = kronLL.gradDescent(gradIter, lrnRate, mnStep, mxStep, warmUp, nSamples);
      logLike = kronLL.gradDescent(1, lrnRate, mnStep, mxStep, 10000, 100000);

    }

}
