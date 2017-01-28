package edu.msstate.dasi

import org.apache.spark.SparkContext

/**
  * Created by B1nary on 1/27/2017.
  */
class kroneckerLL(sc: SparkContext) {

  var nodes = -1
  var kronIters = -1
  var permSwapNodeProb = 0.2
  var realNodes = -1
  var logLike = new kronMtx(sc)
  var EMType = 0
  var missEdges = -1
  var nodePerm = Vector.empty[Int]


  def this(edgeList: Array[(Long,Long)], paramV: Vector[Double], permPSSwapNd: Double = 0.2) = {
    this(sc)
    InitLL()
  }
  def this(edgeList: Array[(Long,Long)], paramMtx: kronMtx, permPSSwapNd: Double = 0.2) = {
    this(sc)
    InitLL()
  }
  def this(edgeList: Array[(Long,Long)], paramMtx: kronMtx, nodeIdPermV: Vector[Int], permPSSwapNd: Double = 0.2) = {
    this(sc)
    InitLL()
    nodePerm = nodeIdPermV
  }
}
