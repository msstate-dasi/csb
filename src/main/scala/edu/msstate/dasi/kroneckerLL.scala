package edu.msstate.dasi

import breeze.numerics.log
import org.apache.spark.SparkContext

import scala.util.Random

/**
  * Created by B1nary on 1/27/2017.
  */
class kroneckerLL(sc: SparkContext) {

  val Rnd = new Random()

  var nodes = -1
  var edges = -1
  var kronIters = -1
  var permSwapNodeProb = 0.2
  var realNodes = -1
  var realEdges = -1
  val NInf: Double = -Double.MaxValue
  var logLike: Double = 0
  var gradV: Array[Double] = Array.empty[Double]
  var EMType = 0
  var missEdges = -1
  var nodePerm = Array.empty[Long]
  var invertPerm = Array.empty[Long]
  var probMtx: kronMtx = null
  var LLMtx: kronMtx = null
  var nodeList: Array[Long] = Array.empty[Long]
  var edgeList: Array[(Long,Long)] = Array.empty[(Long,Long)]
  var adjList: Array[(Long, Array[Long])] = Array.empty[(Long, Array[Long])]
  var lEdgeList: Array[(Long,Long)] = Array.empty[(Long,Long)]
  var lSelfEdge = 0

  def this(sc: SparkContext, edgeList: Array[(Long,Long)], paramV: Array[Double]) = {
    this(sc)
    InitLL(edgeList, new kronMtx(sc, paramV))
  }
  def this(sc: SparkContext, edgeList: Array[(Long,Long)], paramMtx: kronMtx) = {
    this(sc)
    InitLL(edgeList, paramMtx)
  }
  def this(sc: SparkContext, edgeList: Array[(Long,Long)], paramMtx: kronMtx, permSwapNodeProb: Double) = {
    this(sc)
    this.permSwapNodeProb = permSwapNodeProb
    InitLL(edgeList, paramMtx)
  }
  def this(sc: SparkContext, edgeList: Array[(Long,Long)], paramMtx: kronMtx, nodeIdPermV: Array[Long]) = {
    this(sc)
    InitLL(edgeList, paramMtx)
    nodePerm = nodeIdPermV
    setIPerm(nodePerm)
  }

  def InitLL(edgeList: Array[(Long,Long)], paramMtx: kronMtx): Unit = {
    probMtx = paramMtx
    LLMtx = probMtx.getLLMtx()
    setGraph(edgeList)
    logLike = NInf
    if(gradV.length != probMtx.Len()) {
      gradV = Array.fill(probMtx.Len())(0)
    }
  }

  def setIPerm(perm: Array[Long]): Unit = {
    invertPerm = Array.fill(perm.length)(0L)
    for (i<-0 until perm.length) {
      invertPerm(perm(i).toInt) = i
    }
  }

  def setGraph(edgeList: Array[(Long,Long)]): Unit ={
    nodeList = edgeList.flatMap(record => Array(record._1, record._2)).distinct
    this.edgeList = edgeList
    nodes = nodeList.length
    edges = edgeList.length
    adjList = Array.fill(nodes)(0,Array.empty[Long])

    for(edge <- edgeList) {
      val oldList: Array[Long] = adjList(edge._1.toInt)._2
      adjList(edge._1.toInt) = (edge._1, oldList :+ edge._2)
    }

    kronIters = (math.ceil(math.log(nodes)) / log(probMtx.mtxDim)).toInt

    realNodes = nodes
    realEdges = edgeList.length
    //lEdgeList = Array.empty[(Long,Long)]
    lSelfEdge = 0
  }

  def getParams(): Int = {
    return probMtx.Len()
  }

  def gradDescent(nIter: Int, lrnRate: Double, mnStep: Double, inMxStep: Double, warmUp: Int, nSamples: Int): Double = {
    var mxStep = inMxStep

    var oldLL: Double = -1e10
    var curLL: Double = 0

    val eZero = math.pow(edges.toDouble, 1.0/kronIters.toDouble)

    var curGradV: Array[Double] = Array.empty[Double]
    var learnRateV: Array[Double] = Array.fill(getParams())(lrnRate)
    var lastStep: Array[Double] = Array.fill(getParams())(0)

    var newProbMtx: kronMtx = probMtx

    for(i <- 0 until nIter) {
      val result = sampleGradient(warmUp, nSamples, curLL, curGradV)
      curLL = result._1
      curGradV = result._2
      for(p <- 0 until getParams()) {
        learnRateV(p) *= 0.95
        if (i<1) {
          while(math.abs(learnRateV(p)*curGradV(p)) > mxStep) {learnRateV(p) *= 0.95}
          while(math.abs(learnRateV(p)*curGradV(p)) < 0.02) {learnRateV(p) *= 1.0/0.95}
        } else {
          while(math.abs(learnRateV(p)*curGradV(p)) > mxStep) {learnRateV(p) *= 0.95}
          while(math.abs(learnRateV(p)*curGradV(p)) < mnStep) {learnRateV(p) *= 1.0/0.95}
          if(mxStep > 3*mnStep) { mxStep *= 0.95}
        }
        newProbMtx.seedMtx(p) = probMtx.At(p) + learnRateV(p) * curGradV(p)
        if(newProbMtx.At(p) > 0.9999) { newProbMtx.seedMtx(p) = 0.9999}
        if(newProbMtx.At(p) < 0.0001) { newProbMtx.seedMtx(p) = 0.0001}
      }
      if (i+1 < nIter) {
        probMtx = newProbMtx
        LLMtx = probMtx.getLLMtx()
      }
      oldLL = curLL
    }

    println("FITTED PARAMS")
    probMtx.dump()

    return curLL
  }

  def sampleGradient(warmUp: Int, nSamples: Int, inAvgLL: Double, inAvgGradV: Array[Double]): (Double, Array[Double]) = {
    var avgLL = inAvgLL
    var avgGradV = inAvgGradV

    var NId1: Long = 0
    var NId2: Long = 0
    var NAccept: Int = 0

    if(warmUp > 0) {
      calcApxGraphLL()
      for(s <- 0 until warmUp) {
        sampleNextPerm(NId1, NId2)
      }
    }

    calcApxGraphLL()
    calcApxGraphDLL()
    avgLL = 0
    avgGradV = Array.fill(LLMtx.Len())(0)
    for(s <- 0 until nSamples) {
      if (sampleNextPerm(NId1, NId2)) {
        updateGraphDLL(NId1, NId2) ///!!!
        NAccept += 1
      }
      for(m <- 0 until LLMtx.Len) {
        avgGradV(m) += gradV(m)
      }
      avgLL += logLike
    }

    avgLL = avgLL / nSamples.toDouble

    for(m <- 0 until LLMtx.Len) {
      avgGradV(m) = avgGradV(m) / nSamples.toDouble
    }

    return (avgLL, avgGradV)
  }

  def calcApxGraphLL(): Double = {
    logLike = getApxEmptyGraphLL()
    for ((nid, outNids) <- adjList) {
      for (oNid <- outNids) {
        logLike = logLike - LLMtx.getApxNoEdgeLL(nid, oNid, kronIters) + LLMtx.getEdgeLL(nid, oNid, kronIters)
      }
    }
    return logLike
  }

  def getApxEmptyGraphLL(): Double = {
    var sum = 0.0
    var sumSq = 0.0
    for(i<- 0 until probMtx.Len()) {
      sum+=probMtx.At(i)
      sumSq+=math.pow(probMtx.At(i), 2)
    }
    return -math.pow(sum, kronIters) - 0.5*math.pow(sumSq, kronIters)
  }

  def sampleNextPerm(inid1: Long, inid2: Long): Boolean = {
    var nid1 = inid1
    var nid2 = inid2

    if (Rnd.nextDouble() < permSwapNodeProb) {
      nid1 = Rnd.nextLong % nodes
      nid2 = Rnd.nextLong % nodes
      while(nid2 == nid1) {nid2 = Rnd.nextLong % nodes}
    } else {
      val e = Rnd.nextInt % edges

      val edge = edgeList(e)
      nid1 = edge._1
      nid2 = edge._2
    }
    val u = Rnd.nextDouble()
    val oldLL = logLike
    val newLL = swapNodesLL(nid1, nid2)
    val logU = math.log(u)

    if(logU > newLL - oldLL) {
      logLike = oldLL
      swapNodesNodePerm(nid2, nid1)
      swapNodesInvertPerm(nodePerm(nid2.toInt), nodePerm(nid1.toInt))
      return false
    }
    return true
  }

  def swapNodesLL(nid1: Long, nid2: Long): Double = {
    logLike = logLike - nodeLLDelta(nid1) - nodeLLDelta(nid2)
    val (pid1, pid2) = (nodePerm(nid1.toInt), nodePerm(nid2.toInt))

    if(edgeList.contains((nid1, nid2))) {
      logLike += -LLMtx.getApxNoEdgeLL(pid1, pid2, kronIters) + LLMtx.getEdgeLL(pid1, pid2, kronIters)
    }
    if(edgeList.contains((nid2, nid1))) {
      logLike += -LLMtx.getApxNoEdgeLL(pid2, pid1, kronIters) + LLMtx.getEdgeLL(pid2, pid1, kronIters)
    }

    swapNodesNodePerm(nid1, nid2)
    swapNodesInvertPerm(nodePerm(nid1.toInt), nodePerm(nid2.toInt))

    logLike = logLike + nodeLLDelta(nid1) + nodeLLDelta(nid2)
    val (nnid1, nnid2) = (nodePerm(nid1.toInt),nodePerm(nid2.toInt))

    if(edgeList.contains((nid1, nid2))) {
      logLike += -LLMtx.getApxNoEdgeLL(nnid1, nnid2, kronIters) + LLMtx.getEdgeLL(nnid1, nnid2, kronIters)
    }
    if(edgeList.contains((nid2, nid1))) {
      logLike += -LLMtx.getApxNoEdgeLL(nnid2, nnid1, kronIters) + LLMtx.getEdgeLL(nnid2, nnid1, kronIters)
    }

    return logLike
  }

  def nodeLLDelta(nid: Long): Double = {
    if (!nodeList.contains(nid)) return 0.0
    var delta = 0.0

    val srcRow = nodePerm(nid.toInt)
    for(e <- 0 until adjList(nid.toInt)._2.length ) {
      val dstCol = adjList(nid.toInt)._2(e)
      delta += -LLMtx.getApxNoEdgeLL(srcRow, dstCol, kronIters) + LLMtx.getEdgeLL(srcRow, dstCol, kronIters)
    }

    val srcCol = nodePerm(nid.toInt)
    for(e <- 0 until adjList(nid.toInt)._2.length ) {
      val dstRow = adjList(nid.toInt)._2(e)
      delta += -LLMtx.getApxNoEdgeLL(dstRow, srcCol, kronIters) + LLMtx.getEdgeLL(dstRow, srcCol, kronIters)
    }

    if(edgeList.contains((nid, nid))) {
      delta += LLMtx.getApxNoEdgeLL(srcRow, srcCol, kronIters) - LLMtx.getEdgeLL(srcRow, srcCol, kronIters)
    }

    return delta
  }

  def swapNodesNodePerm(nid1: Long, nid2: Long) = {
    nodePerm(nid2.toInt) = nid1
    nodePerm(nid1.toInt) = nid2
  }

  def swapNodesInvertPerm(nid1: Long, nid2: Long) = {
    invertPerm(nid2.toInt) = nid1
    invertPerm(nid1.toInt) = nid2
  }

  def calcApxGraphDLL(): Array[Double] = {
    for(paramId <- 0 until LLMtx.Len()) {
      var DLL = getApxEmptyGraphDLL(paramId)
      for((nid,outNids) <- adjList) {
        for(dstNid <- outNids) {
          DLL = DLL - LLMtx.getApxNoEdgeDLL(paramId, nid, dstNid, kronIters) + LLMtx.getEdgeDLL(paramId, nid, dstNid, kronIters)
        }
      }
      gradV(paramId) = DLL
    }
    return gradV
  }

  def getApxEmptyGraphDLL(paramId: Int): Double = {
    var sum = 0.0
    var sumSq = 0.0
    for (i <- 0 until probMtx.Len()) {
      sum += probMtx.At(i)
      sumSq += math.pow(probMtx.At(i),2)
    }

    return -kronIters*math.pow(sum, kronIters - 1) - kronIters*math.pow(sumSq, kronIters-1)*probMtx.At(paramId)
  }

  def updateGraphDLL(snid1: Long, snid2: Long): Unit = {
    for(paramId <- 0 until LLMtx.Len()) {
      swapNodesNodePerm(snid1, snid2)

      var DLL = gradV(paramId)
      DLL = DLL - nodeDLLDelta(paramId, snid1) - nodeDLLDelta(paramId, snid2)

      val (pid1,pid2) = (nodePerm(snid1.toInt), nodePerm(snid2.toInt))

      if(edgeList.contains((snid1,snid2))) {
        DLL += -LLMtx.getApxNoEdgeDLL(paramId, pid1, pid2, kronIters) + LLMtx.getEdgeDLL(paramId, pid1, pid2, kronIters)
      }
      if(edgeList.contains((snid2,snid1))) {
        DLL += -LLMtx.getApxNoEdgeDLL(paramId, pid2, pid1, kronIters) + LLMtx.getEdgeDLL(paramId, pid2, pid1, kronIters)
      }

      swapNodesNodePerm(snid1, snid2)
      DLL = DLL + nodeDLLDelta(paramId, snid1) + nodeDLLDelta(paramId, snid2)
      val (nnid1,nnid2) = (nodePerm(snid1.toInt), nodePerm(snid2.toInt))

      if(edgeList.contains((snid1,snid2))) {
        DLL += -LLMtx.getApxNoEdgeDLL(paramId, nnid1, nnid2, kronIters) + LLMtx.getEdgeDLL(paramId, nnid1, nnid2, kronIters)
      }
      if(edgeList.contains((snid2,snid1))) {
        DLL += -LLMtx.getApxNoEdgeDLL(paramId, nnid2, nnid1, kronIters) + LLMtx.getEdgeDLL(paramId, nnid2, nnid1, kronIters)
      }
    }
  }

  def nodeDLLDelta(paramId: Int, nid: Long): Double = {
    if(!nodeList.contains(nid)) return 0.0
    var delta = 0.0

    val srcRow = nodePerm(nid.toInt)
    for(e <- 0 until adjList(nid.toInt)._2.length ) {
      val dstCol = adjList(nid.toInt)._2(e)
      delta += -LLMtx.getApxNoEdgeDLL(paramId, srcRow, dstCol, kronIters) + LLMtx.getEdgeDLL(paramId, srcRow, dstCol, kronIters)
    }

    val srcCol = nodePerm(nid.toInt)
    for(e <- 0 until adjList(nid.toInt)._2.length ) {
      val dstRow = adjList(nid.toInt)._2(e)
      delta += -LLMtx.getApxNoEdgeDLL(paramId, dstRow, srcCol, kronIters) + LLMtx.getEdgeDLL(paramId, dstRow, srcCol, kronIters)
    }

    if(edgeList.contains((nid, nid))) {
      delta += LLMtx.getApxNoEdgeDLL(paramId, srcRow, srcCol, kronIters) - LLMtx.getEdgeDLL(paramId, srcRow, srcCol, kronIters)
    }

    return delta

  }
}
