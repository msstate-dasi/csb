package edu.msstate.dasi.csb

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random

/**
  * Created by spencer on 1/27/2017.
  */
class kroneckerLL() {

  val Rnd = new Random()

  var nodes = -1
  var edges = -1
  var kronIters = -1
  var permSwapNodeProb = 0.2
  var realNodes = -1
  var realEdges = -1
  var nodeHash: mutable.HashMap[Long, Boolean] = null
//  var edgeHash: mutable.HashMap[(Long, Long), Boolean] = null
  var edgeList: Array[(Long, Long)] = null
  var adjArrHash: Array[mutable.LongMap[Boolean]] = null
  var inAdjArrHash: Array[mutable.LongMap[Boolean]] = null
  var adjArrArr: Array[Array[Long]] = null
  var inAdjArrArr: Array[Array[Long]] = null
//  var adjHash: mutable.HashMap[Long, Array[Long]] = null
//  var inAdjHash: mutable.HashMap[Long, Array[Long]] = null
//  var immutableAdjhash: scala.collection.immutable.Map[Long, Array[Long]] = null
//  var immutableinAdjHash: scala.collection.immutable.Map[Long, Array[Long]] = null
  val NInf: Double = -Double.MaxValue
  var logLike: Double = 0
  var gradV: Array[Double] = Array.empty[Double]
  var EMType = 0
  var missEdges = -1
  var nodePerm: Array[Long] = null
  var probMtx: kronMtx = null
  var LLMtx: kronMtx = null
  var lEdgeList: Array[(Long,Long)] = Array.empty[(Long,Long)]
  var lSelfEdge = 0

  def this(edgeList: RDD[(Long,Long)], nodeList: RDD[Long],  paramV: Array[Double]) = {
    this
    InitLL(edgeList, nodeList, new kronMtx(paramV))
  }
  def this(edgeList: RDD[(Long,Long)], nodeList: RDD[Long], paramMtx: kronMtx) = {
    this
    InitLL(edgeList, nodeList, paramMtx)
  }
  def this(edgeList: RDD[(Long,Long)], nodeList: RDD[Long], paramMtx: kronMtx, permSwapNodeProb: Double) = {
    this
    this.permSwapNodeProb = permSwapNodeProb
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
    sc.parallelize(edgeList.toSeq)
      .flatMap(record => Array((record._1, 1L), (record._2, 1L)))
      .reduceByKey(_+_)
      .map(record => (record._2, record._1))
      .collect //DO NOT REMOVE THIS!!!

    val DegNIdV = DegNIdVUnsorted.toSeq.sortBy(row => (row._1, row._2)).reverse

    nodePerm = new Array[Long](nodes)

    for(i <- 0 until nodes)
      {
        nodePerm(i) = DegNIdV(i)._2
      }
//    setIPerm(nodePerm)
  }


  def InitLL(edgeList: RDD[(Long,Long)], nodeList: RDD[Long], paramMtx: kronMtx): Unit = {
    probMtx = paramMtx
    LLMtx = probMtx.getLLMtx()
    setGraph(edgeList, nodeList)
    logLike = NInf
    if(gradV.length != probMtx.Len()) {
      gradV = Array.fill(probMtx.Len())(0)
    }
  }


  def setGraph(edgeList: RDD[(Long,Long)], nodeList: RDD[Long]): Unit = {
    this.nodeHash = new mutable.HashMap()
    //    val broadcastNodeHash = sc.broadcast(nodeHash)
    nodeList.distinct.collect()
      .foreach(record =>
        nodeHash.put(record, true))

    //    this.edgeHash = new mutable.HashMap[(Long, Long), Boolean]()
    //    val broadcastEdgeHash = sc.broadcast(edgeHash)
    //    edgeList.distinct.collect()
    //      .foreach(record =>
    //        edgeHash.put(record, true))
    //    this.edgeList = edgeHash.keySet.toArray
    this.edgeList = edgeList.collect()

    nodes = this.nodeHash.size
    edges = this.edgeList.size

    this.adjArrHash = new Array[mutable.LongMap[Boolean]](nodes)
    this.inAdjArrHash = new Array[mutable.LongMap[Boolean]](nodes)
    for (n <- 0 until nodes) {
      adjArrHash(n) = new mutable.LongMap[Boolean]()
      inAdjArrHash(n) = new mutable.LongMap[Boolean]()
    }
    for (edge <- this.edgeList) {
      try {
        var oldMap: mutable.LongMap[Boolean] = adjArrHash(edge._1.toInt)
        oldMap.put(edge._2, true)
        adjArrHash(edge._1.toInt) = oldMap

      } catch {

        case _: Throwable =>
          {
            val temp = new mutable.LongMap[Boolean]
            temp.put(edge._2, true)
            adjArrHash(edge._1.toInt) = null
          }
      }

      try {
        var oldList: mutable.LongMap[Boolean] = inAdjArrHash(edge._2.toInt)
        oldList.put(edge._1, true)
        inAdjArrHash(edge._2.toInt) = oldList

      } catch {

        case _: Throwable =>
          {
            val temp = new mutable.LongMap[Boolean]
            temp.put(edge._1, true)
            inAdjArrHash(edge._2.toInt) = temp
          }
      }
    }

    adjArrArr = new Array[Array[Long]](nodes)
    inAdjArrArr = new Array[Array[Long]](nodes)
    for(a <- 0 until adjArrHash.length)
      {
        adjArrArr(a) = adjArrHash(a).keySet.toArray
        inAdjArrArr(a) = inAdjArrHash(a).keySet.toArray
      }
//    immutableAdjhash = this.adjHash.toMap
//    immutableinAdjHash = this.inAdjHash.toMap


    realNodes = nodes
    realEdges = this.edgeList.length
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
      val ((temp1, temp2), result) = sampleNextPerm(NId1, NId2)
      NId1 = temp1
      NId2 = temp2
      if(result)
      {
        updateGraphDLL(NId1, NId2)
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
    var i = 0.0
    var j = 0.0
    val adjListSorted = adjArrHash.toSeq.sortBy(_.size).reverse//immutableAdjhash.toSeq.sortBy(_._2.length).reverse

    for (nid <- 0 until adjListSorted.length) {
      for (oNid <- adjListSorted(nid))
      {
        logLike = logLike - LLMtx.getApxNoEdgeLL(nodePerm(nid.toInt), nodePerm(oNid._1.toInt), kronIters) + LLMtx.getEdgeLL(nodePerm(nid.toInt), nodePerm(oNid._1.toInt), kronIters)
//        i += LLMtx.getApxNoEdgeLL(nodePerm(nid), nodePerm(oNid), kronIters)
//        j += LLMtx.getEdgeLL(nodePerm(nid), nodePerm(oNid), kronIters)
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

//      val edgeList = edgeHash.keys.toArray
      val edge = edgeList(e)
      nid1 = edge._1
      nid2 = edge._2
    }

    var u = Rnd.nextDouble()
    val oldLL = logLike
    val newLL = swapNodesLL(nid1, nid2)
    val logU = math.log(u)

    if(logU > newLL - oldLL)
    {
      logLike = oldLL
      swapNodesNodePerm(nid2, nid1)
//      swapNodesInvertPerm(nodePerm(nid2), nodePerm(nid1))
      return ((-1,-1), false)
    }
    return ((nid1, nid2), true)
  }

  def swapNodesLL(nid1: Long, nid2: Long): Double = {
    logLike = logLike - nodeLLDelta(nid1) - nodeLLDelta(nid2)
    val (pid1, pid2) = (nodePerm(nid1.toInt), nodePerm(nid2.toInt))


    if(adjArrHash(nid1.toInt).contains(nid2) != null)
    {

      logLike += -LLMtx.getApxNoEdgeLL(pid1, pid2, kronIters) + LLMtx.getEdgeLL(pid1, pid2, kronIters)
    }

    if(adjArrHash(nid2.toInt).contains(nid1) != null)
    {
      logLike += -LLMtx.getApxNoEdgeLL(pid2, pid1, kronIters) + LLMtx.getEdgeLL(pid2, pid1, kronIters)
    }

    swapNodesNodePerm(nid1, nid2)
//    swapNodesInvertPerm(nodePerm(nid1), nodePerm(nid2))

    logLike = logLike + nodeLLDelta(nid1) + nodeLLDelta(nid2)
    val (nnid1, nnid2) = (nodePerm(nid1.toInt),nodePerm(nid2.toInt))


//    if(edgeList.contains((nid1, nid2)))
    if(adjArrHash(nid1.toInt).contains(nid2) != null)
    {
      logLike += +LLMtx.getApxNoEdgeLL(nnid1, nnid2, kronIters) - LLMtx.getEdgeLL(nnid1, nnid2, kronIters)
    }
//    if(edgeList.contains((nid2, nid1)))
    if(adjArrHash(nid2.toInt).contains(nid1) != null)
    {
      logLike += +LLMtx.getApxNoEdgeLL(nnid2, nnid1, kronIters) - LLMtx.getEdgeLL(nnid2, nnid1, kronIters)
    }

    return logLike
  }


  /**
    *
    * @param nid
    * @return
    */
  def nodeLLDelta(nid: Long): Double = {
    if (!nodeHash.contains(nid))
      {
        return 0.0
      }
    var delta = 0.0

    val srcRow = nodePerm(nid.toInt)
    val rowStop = adjArrArr(nid.toInt).length//immutableAdjhash(nid).length
//    val outNids = adjHash(nid.toInt)
//    for(e <- outNids)
//      {
//        val dstCol = nodePerm(e._1.toInt)
//        delta += -LLMtx.getApxNoEdgeLL(srcRow, dstCol, kronIters) + LLMtx.getEdgeLL(srcRow, dstCol, kronIters)
//      }


    var counter = 0
    while(counter < rowStop)
      {
          val dstCol = nodePerm(adjArrArr(nid.toInt)(counter).toInt)
          delta += -LLMtx.getApxNoEdgeLL(srcRow, dstCol, kronIters) + LLMtx.getEdgeLL(srcRow, dstCol, kronIters)

    counter += 1
      }
//    for(e <- 0 until immutableadjHash(nid).length){
//      val dstCol = nodePerm(immutableadjHash(nid)(e))
//      delta += -LLMtx.getApxNoEdgeLL(srcRow, dstCol, kronIters) + LLMtx.getEdgeLL(srcRow, dstCol, kronIters)
//    }


    val srcCol = nodePerm(nid.toInt)
    val colStop = inAdjArrArr(nid.toInt).length
//    val inNids = inAdjHash(nid.toInt)
//    for(e <- inNids)
//      {
//        val dstRow = nodePerm(e._1.toInt)
//        delta += -LLMtx.getApxNoEdgeLL(dstRow, srcCol, kronIters) + LLMtx.getEdgeLL(dstRow, srcCol, kronIters)
//      }


    counter = 0
    while(counter < colStop)
      {
        val dstRow = nodePerm(inAdjArrArr(nid.toInt)(counter).toInt)
        delta += -LLMtx.getApxNoEdgeLL(dstRow, srcCol, kronIters) + LLMtx.getEdgeLL(dstRow, srcCol, kronIters)
        counter += 1
      }
//    for(e <- 0 until inAdjHash(nid).length )
//    {
//      val dstRow = nodePerm(inAdjHash(nid)(e))
//      delta += -LLMtx.getApxNoEdgeLL(dstRow, srcCol, kronIters) + LLMtx.getEdgeLL(dstRow, srcCol, kronIters)
//    }

    if(adjArrHash(nid.toInt).contains(nid))//edgeHash.contains((nid, nid)))
    {
      delta += LLMtx.getApxNoEdgeLL(srcRow, srcCol, kronIters) - LLMtx.getEdgeLL(srcRow, srcCol, kronIters)
    }

    return delta
  }

  def swapNodesNodePerm(nid1: Long, nid2: Long) = {
    val temp1 = nodePerm(nid1.toInt)
    val temp2 = nodePerm(nid2.toInt)

    nodePerm(nid1.toInt) = temp2
    nodePerm(nid2.toInt) = temp1
  }


  def calcApxGraphDLL(): Array[Double] = {
    for(paramId <- 0 until LLMtx.Len()) {
      var DLL = getApxEmptyGraphDLL(paramId)
      val adjListSorted = adjArrHash.toSeq.sortBy(_.size).reverse
      for(nid <- 0 until adjArrHash.length) {
        for(dstNid <- adjArrHash(nid)) {
          DLL = DLL - LLMtx.getApxNoEdgeDLL(paramId, nodePerm(nid), nodePerm(dstNid._1.toInt), kronIters) + LLMtx.getEdgeDLL(paramId, nodePerm(nid.toInt), nodePerm(dstNid._1.toInt), kronIters)
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
//    val copyGradV = new Array[Double](4)
//    copyGradV(0) = gradV(0)
//    copyGradV(1) = gradV(1)
//    copyGradV(2) = gradV(2)
//    copyGradV(3) = gradV(3)
//
//    val broadGradV = sc.broadcast(copyGradV)
//    val arr = new Array[Int](4)
//    arr(0) = 0
//    arr(1) = 1
//    arr(2) = 2
//    arr(3) = 3
//    val parallel = sc.parallelize(arr)
//    parallel.foreach(record => {
//      swapNodesNodePerm(snid1, snid2)
//
//      var DLL = broadGradV.value(record)
//      DLL = DLL - nodeDLLDelta(record, snid1) - nodeDLLDelta(record, snid2)
//
//      val (pid1, pid2) = (nodePerm(snid1), nodePerm(snid2))
//
//      if (edgeHash.contains((snid1, snid2))) {
//        DLL += -LLMtx.getApxNoEdgeDLL(record, pid1, pid2, kronIters) + LLMtx.getEdgeDLL(record, pid1, pid2, kronIters)
//      }
//      if (edgeHash.contains((snid2, snid1))) {
//        DLL += -LLMtx.getApxNoEdgeDLL(record, pid2, pid1, kronIters) + LLMtx.getEdgeDLL(record, pid2, pid1, kronIters)
//      }
//
//      swapNodesNodePerm(snid1, snid2)
//      DLL = DLL + nodeDLLDelta(record, snid1) + nodeDLLDelta(record, snid2)
//      val (nnid1, nnid2) = (nodePerm(snid1), nodePerm(snid2))
//
//      if (edgeHash.contains((snid1, snid2))) {
//        DLL += +LLMtx.getApxNoEdgeDLL(record, nnid1, nnid2, kronIters) - LLMtx.getEdgeDLL(record, nnid1, nnid2, kronIters)
//      }
//      if (edgeHash.contains((snid2, snid1))) {
//        DLL += +LLMtx.getApxNoEdgeDLL(record, nnid2, nnid1, kronIters) - LLMtx.getEdgeDLL(record, nnid2, nnid1, kronIters)
//      }
//      broadGradV.value(record) = DLL
//    })
//    return copyGradV

    for(paramId <- 0 until LLMtx.Len()) {

      swapNodesNodePerm(snid1, snid2)

      var DLL = gradV(paramId)
      DLL = DLL - nodeDLLDelta(paramId, snid1) - nodeDLLDelta(paramId, snid2)

      val (pid1,pid2) = (nodePerm(snid1.toInt), nodePerm(snid2.toInt))


      if(adjArrHash(snid1.toInt).get(snid2) != null)
      {
        DLL += -LLMtx.getApxNoEdgeDLL(paramId, pid1, pid2, kronIters) + LLMtx.getEdgeDLL(paramId, pid1, pid2, kronIters)
      }

      if(adjArrHash(snid2.toInt).get(snid1) != null )
      {
        DLL += -LLMtx.getApxNoEdgeDLL(paramId, pid2, pid1, kronIters) + LLMtx.getEdgeDLL(paramId, pid2, pid1, kronIters)
      }

      swapNodesNodePerm(snid1, snid2)
      DLL = DLL + nodeDLLDelta(paramId, snid1) + nodeDLLDelta(paramId, snid2)
      val (nnid1,nnid2) = (nodePerm(snid1.toInt), nodePerm(snid2.toInt))


      if(adjArrHash(snid1.toInt).get(snid2) != null)
      {
        DLL += +LLMtx.getApxNoEdgeDLL(paramId, nnid1, nnid2, kronIters) - LLMtx.getEdgeDLL(paramId, nnid1, nnid2, kronIters)
      }
      if(adjArrHash(snid2.toInt).get(snid1) != null)
      {
        DLL += +LLMtx.getApxNoEdgeDLL(paramId, nnid2, nnid1, kronIters) - LLMtx.getEdgeDLL(paramId, nnid2, nnid1, kronIters)
      }
      gradV(paramId) = DLL
    }
  }


  def nodeDLLDelta(paramId: Int, nid: Long): Double = {
    if(!nodeHash.contains(nid)) return 0.0
    var delta = 0.0


    val srcRow = nodePerm(nid.toInt)
    for(e <- 0 until adjArrArr(nid.toInt).length )
    {

      val dstCol = nodePerm(adjArrArr(nid.toInt)(e).toInt)
      delta += -LLMtx.getApxNoEdgeDLL(paramId, srcRow, dstCol, kronIters) + LLMtx.getEdgeDLL(paramId, srcRow, dstCol, kronIters)
    }

    val srcCol = nodePerm(nid.toInt)
    for(e <- 0 until inAdjArrArr(nid.toInt).length )
    {
      val dstRow = nodePerm(inAdjArrArr(nid.toInt)(e).toInt)
      delta += -LLMtx.getApxNoEdgeDLL(paramId, dstRow, srcCol, kronIters) + LLMtx.getEdgeDLL(paramId, dstRow, srcCol, kronIters)
    }

    if(adjArrHash(nid.toInt).contains(nid))//edgeHash.contains((nid, nid)))
    {
      delta += +LLMtx.getApxNoEdgeDLL(paramId, srcRow, srcCol, kronIters) - LLMtx.getEdgeDLL(paramId, srcRow, srcCol, kronIters)
    }
    return delta

  }
}
