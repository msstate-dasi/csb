package edu.msstate.dasi

import org.apache.spark.SparkContext

import scala.util.Random

/**
  * Created by B1nary on 1/27/2017.
  */
class kronMtx(sc: SparkContext) {

  val NInf: Double = Double.MaxValue
  var rnd: Random = new Random()
  var mtxDim: Int = -1
  var seedMtx: Array[Double] = Array.empty[Double]

  def this(sc: SparkContext, dim: Int) = {
    this(sc)
    mtxDim = dim
    seedMtx = Array.fill(dim*dim)(0d)
  }
  def this(sc: SparkContext, sMtx: Array[Double]) = {
    this(sc)
    mtxDim = sMtx.length / 2 //Added this since mtxes are squares
    seedMtx = sMtx
  }
  def this(sc: SparkContext, kMtx: kronMtx) = {
    this(sc)
    mtxDim = kMtx.mtxDim
    seedMtx = kMtx.seedMtx
  }

  def dump(): Unit = {
    val iter = seedMtx.iterator
    for (i <- 0 until mtxDim)
      for (j <- 0 until mtxDim) {
        if (iter.hasNext) {
          print(iter.next().toString + "\t")
        }
      }
      println()
  }

  def At(i: Int): Double = {
    return seedMtx(i)
  }

  def At(row: Int, col: Int): Double = {
    return seedMtx(mtxDim*row+col)
  }

  def Len(): Int = {return mtxDim * mtxDim}

  def getLLMtx(): kronMtx = {
    var LLMtx: kronMtx = new kronMtx(sc, mtxDim)
    println(Len())
    for(i <- 0 until Len()) {
      if (At(i) != 0.0) {
        LLMtx.seedMtx(i) = math.log(At(i))
      } else {
        LLMtx.seedMtx(i) = NInf
      }
    }
    return LLMtx
  }

  def setForEdges(nodes: Int, edges: Int): Int = {
    val kronIter = getKronIter(nodes)
    println(kronIter)
    val eZero = math.pow(edges.toDouble, 1.0/kronIter.toDouble)
    println(eZero)
    val factor: Double = eZero / getMtxSum()
    println(factor)
    for(i<-0 until Len()) {
      seedMtx(i) *= factor
      if (seedMtx(i) > 1) { seedMtx(i) = 1 }
    }
    return kronIter
  }

  def getKronIter(nodes: Int): Int =  {
    println("number of nodes " + nodes)
    return math.ceil(math.log(nodes.toDouble)/math.log(mtxDim.toDouble)).toInt
  }

  def getMtxSum(): Double = {
    var sum = 0.0
    for(i <- 0 until Len()) {
      sum += At(i)
    }
    return sum
  }

  def getApxNoEdgeLL(nid1: Long, nid2: Long, nKronIters: Int): Double = {
//    println("node " + nid1 + " and node " + nid2)
    val edgeLL = getEdgeLL(nid1, nid2, nKronIters)
//    println("no edge = " + edgeLL)
//    sys.exit(1)
    return -math.exp(edgeLL) - 0.5*math.exp(2*edgeLL)
  }

  def getEdgeLL(inid1: Long, inid2: Long, nKronIters: Int): Double = {
    var nid1 = inid1
    var nid2 = inid2
    var LL = 0.0
    for (level <- 0 until nKronIters) {
      val LLVal = At((nid1 % mtxDim).toInt,(nid2 % mtxDim).toInt)
      if (LLVal == NInf) return NInf
      LL += LLVal
      nid1 /= mtxDim
      nid2 /= mtxDim
    }
    return LL
  }

  def getApxNoEdgeDLL(paramId: Int, inid1: Long, inid2: Long, nKronIters: Int): Double = {
    var nid1 = inid1
    var nid2 = inid2
    val thetaX = paramId % mtxDim
    val thetaY = paramId / mtxDim
    var thetaCnt = 0
    var DLL: Double = 0

    for (level <- 0 until nKronIters) {
      val x = (nid1 % mtxDim).toInt
      val y = (nid2 % mtxDim).toInt
      val lVal: Double = At(x,y)

      if(x==thetaX && y==thetaY) {
        if(thetaCnt != 0) {
          DLL += lVal
        }
        thetaCnt += 1
      } else {
        DLL += lVal
      }
      nid1 /= mtxDim
      nid2 /= mtxDim
    }

    return -thetaCnt*math.exp(DLL) - thetaCnt*math.exp(At(thetaX, thetaY)+2*DLL)
  }

  def getEdgeDLL(paramId: Int, inid1: Long, inid2: Long, nKronIters: Int): Double = {
    var nid1 = inid1
    var nid2 = inid2
    val thetaX = paramId % mtxDim
    val thetaY = paramId / mtxDim

    var thetaCnt = 0

    for(level<-0 to nKronIters) {
      if((nid1 % mtxDim) == thetaX && (nid2%mtxDim) == thetaY) {
        thetaCnt += 1
      }
      nid1 /= mtxDim
      nid2 /= mtxDim
    }

    return thetaCnt.toDouble / math.exp(At(paramId))
  }
}
