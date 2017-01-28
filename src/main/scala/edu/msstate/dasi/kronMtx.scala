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
  var seedMtx: Vector[Double] = Vector.empty

  def this(sc: SparkContext, dim: Int) = {
    this(sc)
    mtxDim = dim
    seedMtx = Vector.fill(dim*dim)(0)
  }
  def this(sc: SparkContext, sMtx: Vector[Double]) = {
    this(sc)
    mtxDim = sMtx.length
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
      for (j<- 0 until mtxDim) {
        print(iter.next().toString + "\t")
        iter
      }
      println()
  }
}
