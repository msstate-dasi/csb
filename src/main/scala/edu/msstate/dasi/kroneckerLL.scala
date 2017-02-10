package edu.msstate.dasi

import breeze.numerics.log
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.parallel.immutable
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
  var nodeHash: mutable.HashMap[Long, Boolean] = null
  var edgeHash: mutable.HashMap[(Long, Long), Boolean] = null
  var adjHash: mutable.HashMap[Long, Array[Long]] = null
  var inAdjHash: mutable.HashMap[Long, Array[Long]] = null
  val NInf: Double = -Double.MaxValue
  var logLike: Double = 0
  var gradV: Array[Double] = Array.empty[Double]
  var EMType = 0
  var missEdges = -1
  var nodePerm: mutable.HashMap[Long, Long] = null
  var invertPerm: mutable.HashMap[Long, Long] = null
  var probMtx: kronMtx = null
  var LLMtx: kronMtx = null
  //var nodeList: Array[Long] = Array.empty[Long]
  //var edgeList: Array[(Long,Long)] = Array.empty[(Long,Long)]
  //var adjList: Array[(Long, Array[Long])] = Array.empty[(Long, Array[Long])]
  var lEdgeList: Array[(Long,Long)] = Array.empty[(Long,Long)]
  var lSelfEdge = 0

  def this(sc: SparkContext, edgeList: Array[(Long,Long)], nodeList: Array[Long],  paramV: Array[Double]) = {
    this(sc)
    InitLL(edgeList, nodeList, new kronMtx(sc, paramV))
  }
  def this(sc: SparkContext, edgeList: Array[(Long,Long)], nodeList: Array[Long], paramMtx: kronMtx) = {
    this(sc)
    InitLL(edgeList, nodeList, paramMtx)
  }
  def this(sc: SparkContext, edgeList: Array[(Long,Long)], nodeList: Array[Long], paramMtx: kronMtx, permSwapNodeProb: Double) = {
    this(sc)
    this.permSwapNodeProb = permSwapNodeProb
//    println(paramMtx.Len())
    InitLL(edgeList, nodeList, paramMtx)
  }
//  def this(sc: SparkContext, edgeList: Array[(Long,Long)], nodeList: Array[Long], paramMtx: kronMtx, nodeIdPermV: Array[Long]) = {
//    this(sc)
//    InitLL(edgeList, nodeList, paramMtx)
//    nodePerm = nodeIdPermV
//    setIPerm(nodePerm)
//  }


  def setPerm(): Unit =
  {




    //gather every (deg, nid)
    //undirected
    val DegNIdVUnsorted =
    sc.parallelize(edgeHash.keys.toSeq)
      .flatMap(record => Array((record._1, 1L), (record._2, 1L)))
      .reduceByKey(_+_)
      .map(record => (record._2, record._1))
      .collect //DO NOT REMOVE THIS!!!

    val DegNIdV = DegNIdVUnsorted.toSeq.sortBy(row => (row._1, row._2)).reverse

    /*
    for(i <- DegNIdV)
    {
      println("deg: " + i._1 + " nid: " + i._2)
    }
    */

    nodePerm = new mutable.HashMap[Long, Long]()

    //var superbigstring = ""

    for(i <- 0 until nodes)
      {
        nodePerm.put(i, DegNIdV(i)._2)
        //superbigstring += "("+i+","+DegNIdV(i)._2+")"
//        println("setting node " + DegNIdV(i)._2 + " to new node index " + nodePerm(i) + " which corresponds to node label " + nodeList(i))
      }

    //println(superbigstring)

    setIPerm(nodePerm)
  }


  def InitLL(edgeList: Array[(Long,Long)], nodeList: Array[Long], paramMtx: kronMtx): Unit = {
    probMtx = paramMtx
    LLMtx = probMtx.getLLMtx()
    setGraph(edgeList, nodeList)
    logLike = NInf
    if(gradV.length != probMtx.Len()) {
      gradV = Array.fill(probMtx.Len())(0)
    }
  }

  def setIPerm(perm: mutable.HashMap[Long,Long]): Unit = {
    invertPerm = new mutable.HashMap[Long,Long]()
    for ((i,j) <- perm) {
      invertPerm.put(j,i)
    }
  }

  def setGraph(edgeList: Array[(Long,Long)], nodeList: Array[Long]): Unit ={
    this.nodeHash = new mutable.HashMap()
    nodeList.distinct
      .foreach(record =>
        this.nodeHash.put(record, true))

    this.edgeHash = new mutable.HashMap[(Long, Long), Boolean]()
    edgeList.distinct
      .foreach(record =>
        this.edgeHash.put(record, true))

    nodes = this.nodeHash.size
    edges = this.edgeHash.size

    this.adjHash = new mutable.HashMap[Long, Array[Long]]()
    this.inAdjHash = new mutable.HashMap[Long, Array[Long]]()

    for(edge <- edgeHash.keys)
    {
      try {
        var oldList: Array[Long] = adjHash(edge._1)
        adjHash(edge._1) = oldList :+ edge._2

      } catch {
        case _: Throwable => adjHash.put(edge._1, Array(edge._2))
      }

      try {
        var oldList: Array[Long] = inAdjHash(edge._2)
        inAdjHash(edge._2) = oldList :+ edge._1

      } catch {
        case _: Throwable => inAdjHash.put(edge._2, Array(edge._1))
      }
    }

//    kronIters = (math.ceil(math.log(nodes)) / log(probMtx.mtxDim)) computed eariler

    realNodes = nodes
    realEdges = edgeHash.size
    //lEdgeList = Array.empty[(Long,Long)]
    lSelfEdge = 0
  }

  def getParams(): Int = {
    return probMtx.Len()
  }

  def gradDescent(nIter: Int, lrnRate: Double, mnStep: Double, inMxStep: Double, warmUp: Int, nSamples: Int): Double = {
    println("----------------------------------------------------------------------")
    println("Fitting graph on " + nodes + " nodes, " + edges + " edges.")
    println("Kron iters: " + kronIters + " (== " + math.pow(probMtx.mtxDim, kronIters) + " nodes)\n")

    var mxStep = inMxStep

    var oldLL: Double = -1e10
    var curLL: Double = 0

    val eZero = math.pow(edges.toDouble, 1.0/kronIters.toDouble)

    var curGradV: Array[Double] = Array.empty[Double]
    var learnRateV: Array[Double] = Array.fill(getParams())(lrnRate)
    var lastStep: Array[Double] = Array.fill(getParams())(0)

    var newProbMtx: kronMtx = probMtx

    for(i <- 0 until nIter)
    {
      println(i+"]")
      val result = sampleGradient(warmUp, nSamples, curLL, curGradV)
            curLL = result._1
      curGradV = result._2
      for(p <- 0 until getParams()) {
        learnRateV(p) *= 0.95
        val constant = 0.95

        if (i<1) {
          while(math.abs(learnRateV(p)*curGradV(p)) > mxStep) {learnRateV(p) *= constant}
          while(math.abs(learnRateV(p)*curGradV(p)) < mnStep) {learnRateV(p) *= 1.0/constant}
        }
        else
        {
          while(math.abs(learnRateV(p)*curGradV(p)) > mxStep) {learnRateV(p) *= constant}
          while(math.abs(learnRateV(p)*curGradV(p)) < mnStep) {learnRateV(p) *= 1.0/constant}
          if(mxStep > 3*mnStep) { mxStep *= constant}
        }
        newProbMtx.seedMtx(p) = probMtx.At(p) + learnRateV(p) * curGradV(p)
        if(newProbMtx.At(p) > 0.9999) { newProbMtx.seedMtx(p) = 0.9999}
        if(newProbMtx.At(p) < 0.0001) { newProbMtx.seedMtx(p) = 0.0001}
      }
      println("  trueE0:  " + eZero + " (" + edges + "),  estE0:  " + probMtx.getMtxSum() + " (" + math.pow(probMtx.mtxDim, kronIters) + ")  ERR:  " + math.abs(eZero - probMtx.getMtxSum()))
      println("  curLL:  " + curLL + ", deltaLL:  " + (curLL - oldLL))
      for (p <- 0 until getParams()) {
        println("    " + p + "]  " + newProbMtx.At(p) + "  <--  " + probMtx.At(p) + " + " + learnRateV(p)*curGradV(p) + "  Grad: " + curGradV(p) + "  Rate: " + learnRateV(p))
      }


      if (i+1 < nIter)
      {
        probMtx = newProbMtx
        LLMtx = probMtx.getLLMtx()
      }
      oldLL = curLL

      println("FITTED PARAMS")
      probMtx.dump()
    }


    return curLL
  }

  def sampleGradient(warmUp: Int, nSamples: Int, inAvgLL: Double, inAvgGradV: Array[Double]): (Double, Array[Double]) = {
    println("SampleGradient: " + nSamples / 1000 + "K ("+ warmUp / 1000+"K warm-up):")
    var avgLL = inAvgLL
    var avgGradV = inAvgGradV

    var NId1: Long = 0
    var NId2: Long = 0
    var NAccept: Int = 0

    if(warmUp > 0)
    {
      calcApxGraphLL()
      print("empty graph " + logLike)
      print("Warmup Stage: ")
      var startTime = System.nanoTime()
      for(s <- 0 until warmUp) {
        if (1.0*s/warmUp % 0.1 == 0.0)
          print(s + " ")
        sampleNextPerm(NId1, NId2)
      }
      var endTime = System.nanoTime()
      println()
      print("  warm-up: " + (endTime - startTime) / 1e9 + "s,")
    }

    calcApxGraphLL()
    calcApxGraphDLL()
    avgLL = 0
    avgGradV = Array.fill(LLMtx.Len())(0)
    print("  samp")
    var startTime = System.nanoTime()
    for(s <- 0 until nSamples) {
//      println("sample " + s)
      val ((temp1, temp2), result) = sampleNextPerm(NId1, NId2)
      NId1 = temp1
      NId2 = temp2
//      println(" temp1 = " + temp1 + "  temp2 = " + temp2)
//      if (result)
      if(result)
      {
        updateGraphDLL(NId1, NId2) ///!!! lol
        NAccept += 1
      }
      for(m <- 0 until LLMtx.Len)
      {
        avgGradV(m) += gradV(m)
      }
      avgLL += logLike
    }
    var endTime = System.nanoTime()
    print("ling: " + (endTime - startTime)/ 1e9 + "s" + "  accept " + 1.0*100*NAccept/nSamples)
    println()
    avgLL = avgLL / nSamples.toDouble

    for(m <- 0 until LLMtx.Len) {
      avgGradV(m) = avgGradV(m) / nSamples.toDouble
    }

    return (avgLL, avgGradV)
  }

  def calcApxGraphLL(): Double = {
    logLike = getApxEmptyGraphLL()
    //print(" empty graph " + logLike + " ")
    var i = 0.0
    var j = 0.0
    val adjListSorted = adjHash.toSeq.sortBy(_._2.length).reverse
    for ((nid, outNids) <- adjListSorted) {
      for (oNid <- outNids)
      {
//        println("NODE PERM " + nodePerm(nodeHash(nid)))
        logLike = logLike - LLMtx.getApxNoEdgeLL(nodePerm(nid), nodePerm(oNid), kronIters) + LLMtx.getEdgeLL(nodePerm(nid), nodePerm(oNid), kronIters)
        i += LLMtx.getApxNoEdgeLL(nodePerm(nid), nodePerm(oNid), kronIters)
        j += LLMtx.getEdgeLL(nodePerm(nid), nodePerm(oNid), kronIters)
//        println("nid: " + nid + " oNid: " + oNid + " logLike: " + logLike)
//        println("stuff = " + (- LLMtx.getApxNoEdgeLL(nodePerm(nodeHash(nid)), nodePerm(nodeHash(oNid)), kronIters) + LLMtx.getEdgeLL(nodePerm(nodeHash(nid)), nodePerm(nodeHash(oNid)), kronIters)))
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
//    println("sum " + sum + " sumsq = " + sumSq + " kronIters = " + kronIters)
    return -math.pow(sum, kronIters) - 0.5*math.pow(sumSq, kronIters)
  }

  def sampleNextPerm(inid1: Long, inid2: Long): ((Long, Long), Boolean) = {

    var nid1 = inid1
    var nid2 = inid2



    if (Rnd.nextDouble() < permSwapNodeProb)
    {
      nid1 = math.abs(Rnd.nextLong) % nodes
      nid2 = math.abs(Rnd.nextLong) % nodes
      while(nid2 == nid1) {nid2 = math.abs(Rnd.nextLong) % nodes}
    }
    else
    {
      val e = math.abs(Rnd.nextInt) % edges

      val edgeList = edgeHash.keys.toArray
      val edge = edgeList(e)
      nid1 = edge._1
      nid2 = edge._2
    }

//    nid1 = 1
//    nid2 = 0

    var u = Rnd.nextDouble()
//    u = 0
    val oldLL = logLike
    val newLL = swapNodesLL(nid1, nid2)
    val logU = math.log(u)

    if(logU > newLL - oldLL)
    {
      logLike = oldLL
      swapNodesNodePerm(nid2, nid1)
      swapNodesInvertPerm(nodePerm(nid2), nodePerm(nid1))
//      println("changing")
      return ((-1,-1), false)
    }
//    println("did not change")
    return ((nid1, nid2), true)
  }

  def swapNodesLL(nid1: Long, nid2: Long): Double = {
    logLike = logLike - nodeLLDelta(nid1) - nodeLLDelta(nid2)
    val (pid1, pid2) = (nodePerm(nid1), nodePerm(nid2))

//    if(edgeList.contains((nid1, nid2)))
    if(edgeHash.contains((nid1, nid2)))
    {

      logLike += -LLMtx.getApxNoEdgeLL(pid1, pid2, kronIters) + LLMtx.getEdgeLL(pid1, pid2, kronIters)
    }
//    if(edgeList.contains((nid2, nid1)))
    if(edgeHash.contains((nid2, nid1)))
    {
      logLike += -LLMtx.getApxNoEdgeLL(pid2, pid1, kronIters) + LLMtx.getEdgeLL(pid2, pid1, kronIters)
    }

    swapNodesNodePerm(nid1, nid2)
    swapNodesInvertPerm(nodePerm(nid1), nodePerm(nid2))

    logLike = logLike + nodeLLDelta(nid1) + nodeLLDelta(nid2)
    val (nnid1, nnid2) = (nodePerm(nid1),nodePerm(nid2))


//    if(edgeList.contains((nid1, nid2)))
    if(edgeHash.contains((nid1, nid2)))
    {
      logLike += +LLMtx.getApxNoEdgeLL(nnid1, nnid2, kronIters) - LLMtx.getEdgeLL(nnid1, nnid2, kronIters)
    }
//    if(edgeList.contains((nid2, nid1)))
    if(edgeHash.contains((nid2, nid1)))
    {
      logLike += +LLMtx.getApxNoEdgeLL(nnid2, nnid1, kronIters) - LLMtx.getEdgeLL(nnid2, nnid1, kronIters)
    }

    return logLike
  }


  /**
    * I ADDED NODEPERM IN HERE  IT MAY OR MAY NOT BE RIGHT
    * @param nid
    * @return
    */
  def nodeLLDelta(nid: Long): Double = {
    if (!nodeHash.contains(nid))
      {
        return 0.0
      }
    var delta = 0.0

//    println(nodePerm.length)
    val srcRow = nodePerm(nid)
    for(e <- 0 until adjHash(nid).length){// adjHash(nodePerm(nid)) ) {
//      println("here")
      val dstCol = nodePerm(adjHash(nid)(e))
      delta += -LLMtx.getApxNoEdgeLL(srcRow, dstCol, kronIters) + LLMtx.getEdgeLL(srcRow, dstCol, kronIters)
    }

    val srcCol = nodePerm(nid)
    for(e <- 0 until inAdjHash(nid).length )
    {
      val dstRow = nodePerm(inAdjHash(nid)(e))
      delta += -LLMtx.getApxNoEdgeLL(dstRow, srcCol, kronIters) + LLMtx.getEdgeLL(dstRow, srcCol, kronIters)
    }

    if(edgeHash.contains((nid, nid))) {
      delta += LLMtx.getApxNoEdgeLL(srcRow, srcCol, kronIters) - LLMtx.getEdgeLL(srcRow, srcCol, kronIters)
    }

    return delta
  }

  def swapNodesNodePerm(nid1: Long, nid2: Long) = {
    val temp1 = nodePerm(nid1)
    val temp2 = nodePerm(nid2)

    nodePerm(nid1) = temp2
    nodePerm(nid2) = temp1
  }

  def swapNodesInvertPerm(nid1: Long, nid2: Long) = {
    invertPerm(nid2) = nid1
    invertPerm(nid1) = nid2
  }

  def calcApxGraphDLL(): Array[Double] = {
    for(paramId <- 0 until LLMtx.Len()) {
      var DLL = getApxEmptyGraphDLL(paramId)
//      println("begining DLL = " + DLL)
      val adjListSorted = adjHash.toSeq.sortBy(_._2.length).reverse
      for((nid,outNids) <- adjHash) {
//        println("at node id:" + nid)
        for(dstNid <- outNids) {
//          print("\t" + dstNid)
          DLL = DLL - LLMtx.getApxNoEdgeDLL(paramId, nodePerm(nid), nodePerm.get(dstNid).head, kronIters) + LLMtx.getEdgeDLL(paramId, nodePerm.get(nid).head, nodePerm.get(dstNid).head, kronIters)
//          print("\tno edge: " + LLMtx.getApxNoEdgeDLL(paramId, nodePerm(nid), nodePerm.get(dstNid).head, kronIters))
//          println("\tedge: " + LLMtx.getEdgeDLL(paramId, nodePerm.get(nid).head, nodePerm.get(dstNid).head, kronIters))
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


  def updateGraphDLL(inid1: Long, inid2: Long): Unit = {
    var snid1 = inid1
    var snid2 = inid2
    for(paramId <- 0 until LLMtx.Len()) {

      swapNodesNodePerm(snid1, snid2)

      var DLL = gradV(paramId)
      DLL = DLL - nodeDLLDelta(paramId, snid1) - nodeDLLDelta(paramId, snid2)

      val (pid1,pid2) = (nodePerm(snid1), nodePerm(snid2))

//      if(edgeList.contains((snid1,snid2)))
      if(edgeHash.contains((snid1, snid2)))
      {
//        println("OMG WE DID IT")
        DLL += -LLMtx.getApxNoEdgeDLL(paramId, pid1, pid2, kronIters) + LLMtx.getEdgeDLL(paramId, pid1, pid2, kronIters)
      }
//      if(edgeList.contains((snid2,snid1)))
      if(edgeHash.contains((snid2, snid1)))
      {
//        println("OMG WE DID IT AGAIN")
        DLL += -LLMtx.getApxNoEdgeDLL(paramId, pid2, pid1, kronIters) + LLMtx.getEdgeDLL(paramId, pid2, pid1, kronIters)
      }

      swapNodesNodePerm(snid1, snid2)
      DLL = DLL + nodeDLLDelta(paramId, snid1) + nodeDLLDelta(paramId, snid2)
      val (nnid1,nnid2) = (nodePerm(snid1), nodePerm(snid2))

//      if(edgeList.contains((snid1,snid2)))
      if(edgeHash.contains((snid1, snid2)))
      {
        DLL += +LLMtx.getApxNoEdgeDLL(paramId, nnid1, nnid2, kronIters) - LLMtx.getEdgeDLL(paramId, nnid1, nnid2, kronIters)
      }
//      if(edgeList.contains((snid2,snid1)))
      if(edgeHash.contains((snid2, snid1)))
      {
        DLL += +LLMtx.getApxNoEdgeDLL(paramId, nnid2, nnid1, kronIters) - LLMtx.getEdgeDLL(paramId, nnid2, nnid1, kronIters)
      }
      gradV(paramId) = DLL
    }
    return (snid1, snid2)
  }


  def nodeDLLDelta(paramId: Int, nid: Long): Double = {
    if(!nodeHash.contains(nid)) return 0.0
    var delta = 0.0


//    println("nodeDLLDelta")
    val srcRow = nodePerm(nid)
    //nodeHash(nid)
    for(e <- 0 until adjHash(nid).length )
    {
      val dstCol = nodePerm(adjHash(nid)(e))
      delta += -LLMtx.getApxNoEdgeDLL(paramId, srcRow, dstCol, kronIters) + LLMtx.getEdgeDLL(paramId, srcRow, dstCol, kronIters)
    }

    val srcCol = nodePerm(nid)
    for(e <- 0 until inAdjHash(nid).length )
    {
      val dstRow = nodePerm(inAdjHash(nid)(e))
      delta += -LLMtx.getApxNoEdgeDLL(paramId, dstRow, srcCol, kronIters) + LLMtx.getEdgeDLL(paramId, dstRow, srcCol, kronIters)
    }

//    if(edgeList.contains((nid, nid)))
    if(edgeHash.contains((nid, nid)))
    {
      delta += +LLMtx.getApxNoEdgeDLL(paramId, srcRow, srcCol, kronIters) - LLMtx.getEdgeDLL(paramId, srcRow, srcCol, kronIters)
    }
//    println("END nodeDLLDelta")
    return delta

  }
}
