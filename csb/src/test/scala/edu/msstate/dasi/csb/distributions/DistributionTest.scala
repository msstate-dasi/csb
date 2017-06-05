package edu.msstate.dasi.csb.distributions

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

import scala.collection.mutable

class DistributionTest extends FunSuite with SharedSparkContext {

  private val samples = 1000000
  private val tolerance = 0.01

  private def testProbability[T](input: Map[T, Double], distribution: Distribution[T]) = {
    val counts = mutable.Map() ++ input.mapValues(_ => 0)

    for (_ <- 0 until samples) {
      counts(distribution.sample) += 1
    }

    for ((value, prob) <- input) {
      assert((counts(value).toDouble / samples - prob).abs < tolerance)
    }
  }

  test("testSample") {
    val input = Map('A' -> 0.55, 'B' -> 0.25, 'C' -> 0.15, 'D' -> 0.05)
    val distribution = new Distribution(input.toArray)

    testProbability(input, distribution)
  }

  test("testApply") {
    val sequence = Seq(0, 0, 0, 0, 1, 1, 1, 2, 2, 3, 4, 4)
    val input = sequence.groupBy(identity).mapValues(_.length.toDouble / sequence.length)

    val distribution = Distribution(sc.parallelize(sequence))

    testProbability(input, distribution)
  }
}
