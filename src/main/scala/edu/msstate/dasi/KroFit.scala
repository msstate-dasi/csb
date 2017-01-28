package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by justin on 1/26/2017.
  */
class KroFit(sc: SparkContext, partitions: Int, initMtxStr: String, gradIter: Int, connLog: String) extends DataParser {

  var nodes:RDD[Long] = null
  var edges:RDD[(Long, Long)] = null
  var mtx:Array[Double] = null
  var LLmtx:Array[Double] = null
  var KronIters: Int = 0

  def evaluateMtxStr(): Array[Double] =
  {
    var mtx = new Array[Double](4)
    var counter = 0
    for(x <- initMtxStr.split(","))
    {
      if(counter > 4)
      {
        println("ERROR (TO BIG) MTX MUST BE 4X4")
        System.exit(-1)
      }
      println(counter)
      mtx(counter) = x.toDouble
      counter += 1
    }
    if(counter < 4)
    {
      println("ERROR (TO SMALL) MTX MUST BE 4X4")
    }
    return mtx
  }

  def mtxSum(mtx: Array[Double]): Double =
  {
    var sum: Double = 0
    for(x <- mtx)
      {
        sum += x.toDouble
      }
    return sum
  }

  def scaleMtx(mtx: Array[Double]): Array[Double] =
  {
    var rdd = tempReadFromConn(sc, partitions, connLog)
    KronIters = math.ceil(math.log(rdd._1.count()) / math.log(mtx.length / 2)).toInt //return (int) ceil(log(double(Nodes)) / log(double(GetDim()))); // upper bound
    println("KronIter: " + KronIters)
    val Ezero = math.pow(rdd._2.count(), 1.0 / KronIters.toDouble)  //const double EZero = pow((double) Edges, 1.0/double(KronIter));
    println("Ezero: " + Ezero)
    val factor = Ezero / mtxSum(mtx) //const double Factor = EZero / GetMtxSum();
    println("factor: " + factor)

    for(x <- 0 to mtx.length-1)
    {
      mtx(x) = mtx(x) * factor
      println(mtx(x))
    }
    return mtx
  }

  def sortByDegree(): (RDD[(Long,Long)], RDD[Long]) =
  {
    var rdd = tempReadFromConn(sc, partitions, connLog)
    val edges = rdd._2.flatMap(record => Array((record._1, record._2),(record._2, record._1)))
    var arr = edges.groupByKey().sortBy(record => record._2.size, ascending = false).collect()
    println("before perm")
//    for(x <- arr)
//      {
//        println("Node " + x._1 + " has degree " + x._2.size)
//      }


    var newNodesID = new mutable.HashMap[Long, Long]()
    println("\n\n\n\n\n\n\n\n\n\nafter perm")
    for(x <- 0L to arr.length - 1)
      {
        newNodesID.put(arr(x.toInt)._1, x + 1)
      }
    var permEdges = edges.map(record => (newNodesID.get(record._1).head, newNodesID.get(record._2).head))//.collect()
    var permNodes = edges.flatMap(record => Array(record._1, record._2))
//    var testing = perm.groupByKey().sortBy(record => record._2.size, ascending = false).collect()
//    for(x <- testing)
//      {
//        println("Node " + x._1 + " has degree " + x._2.size)
//      }

    return (permEdges, permNodes)
  }


  def gradDec(NIter: Int, lrnRate: Double, mnStep: Double, mxStep: Double, WarmUp: Int, NSamples: Int): Unit =
  {
    var CruLL = 0.0
    var OldLL = -1e10
    var learnRateVec = Array[Double](lrnRate, lrnRate, lrnRate, lrnRate)
    sampGrad(WarmUp, NSamples, CruLL, null)
    for(iter <- 1 to NIter)
      {

      }
  }

  def sampGrad(WarmUp: Int, NSamples: Int, AvgLL: Double, AvgGradV: Array[Double]): Unit =
  {
    var NID1 = 0
    var NID2 = 0
    var NAccept = 0

    val nodeCount = nodes.count()
    for(nid <- 0 until nodeCount.toInt)
      {
        calcAproxGraphLL()
      }
  }

  def calcEmptyGraphLL(): Double =
  {
    var sum: Double = 0.0
    var sumSq: Double = 0.0

    for(i <- 0 until mtx.length)
      {
        sum += mtx(i)
        sumSq += math.pow(mtx(i), 2)
      }
    return -math.pow(sum, KronIters) - 0.5 * math.pow(sumSq, KronIters)
  }


  def calcAproxGraphLL(): Double =
  {
    var loglike = calcEmptyGraphLL()
    println("loglike = " + loglike)
    val nodeCount = nodes.count().toInt
    for(nid <- 0 until nodeCount.toInt)
      {
        val degree = edges.lookup(nid).size
        val dsts = edges.lookup(nid).toArray
        for(e <- 0 until degree)
          {
            loglike = loglike - getAproxNoEdgeLL(nid, dsts(e).toInt) + GetEdgeLL(nid, dsts(e).toInt)
          }
      }
    return 0.0
  }

  def GetEdgeLL(NID1: Int, NID2: Int): Double =
  {
   return 0.0
  }

  def getAproxNoEdgeLL(NID1: Int, NID2: Int): Double =
  {
    val EdgeLL = getEdgeLL(NID1, NID2)
    return math.pow(math.E, EdgeLL) - 0.5 * math.pow(math.E, 2 * EdgeLL)
  }

  def getEdgeLL(NID1: Long, NID2: Long): Double =
  {
    var cNID1 = NID1
    var cNID2 = NID2
    var LL:Double = 0.0
    for(lvl <- 0 until KronIters)
      {
        val LLVal = computeLLat(cNID1.toInt % (mtx.length/2), cNID2.toInt % (mtx.length/2))
        LL += LLVal
        cNID1 = cNID1 / (mtx.length / 2)
        cNID2 = cNID2 / (mtx.length / 2)
      }
    println("LL is " + LL)
    return LL
  }

  def computeLLat(row: Int, col: Int): Double =
  {
    return mtx(mtx.length/2 * row + col)
  }

  def run(): Boolean =
  {
    mtx = evaluateMtxStr()
    for(x <- mtx)
      {
        println(x)
      }
    mtx = scaleMtx(mtx)
    for(x <- 0 until mtx.length)
      {
        if(mtx(x) != 0) LLmtx(x) = math.log(mtx(x))
        else LLmtx(x) = Int.MinValue
      }

    val (permEdges, permNodes) = sortByDegree()
    nodes = permNodes
    edges = permEdges
    val learnRate: Double = 1e-5
    gradDec(50, learnRate, 0.005, 0.05, 10000, 100000)


    return true
  }

}