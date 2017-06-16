package edu.msstate.dasi.csb.data.distributions

import com.holdenkarau.spark.testing.RDDGenerator
import edu.msstate.dasi.csb.test.PropertySpec
import org.scalacheck.Arbitrary
import org.scalactic.anyvals.PosZInt

import scala.collection.mutable
import scala.util.Random

class ConditionalDistributionTest extends PropertySpec {

  private val samples = 1000000
  private val condSamples = 10
  private val tolerance = 0.01

  private val inputMinSize = PosZInt(10)
  private val inputSizeRange = PosZInt(20)

  override implicit val generatorDrivenConfig =
    PropertyCheckConfiguration(minSize = inputMinSize, sizeRange = inputSizeRange)

  private def testProbability[Value, Cond](input: Map[Value, Double], conditioner: Cond, distribution: ConditionalDistribution[Value, Cond]) = {
    val counts = mutable.Map() ++ input.mapValues(_ => 0)

    for (_ <- 0 until samples) {
      counts(distribution.sample(conditioner)) += 1
    }

    for ((value, prob) <- input) {
      assert((counts(value).toDouble / samples - prob).abs < tolerance)
    }
  }

  private def testConditional[Value, Cond](input: Map[Cond, Map[Value, Double]], distribution: ConditionalDistribution[Value, Cond]) = {
    val conditioners = input.keys.toSeq
    val size = conditioners.size

    for (_ <- 0 until condSamples) {
      val conditioner = conditioners(Random.nextInt(size))

      testProbability(input(conditioner), conditioner, distribution)
    }
  }

  test("a Conditional Distribution from an RDD should return samples according to the probability of the input occurrences") {
    forAll(RDDGenerator.genRDD[(Int, Long)](sc)(Arbitrary.arbitrary[(Int, Long)])) { rdd =>
      val sequence = rdd.collect

      val sums = sequence.map { case (_, cond) => cond }.groupBy(identity).mapValues(_.length)

      val input = sequence.groupBy(identity).mapValues(_.length)
        .map { case ((value, cond), count) => (cond, value, count.toDouble / sums(cond)) }
        .groupBy { case (cond, _, _) => cond }
        .mapValues(_.map { case (_, value, prob) => (value, prob) }.toMap)

      val distribution = new ConditionalDistribution(rdd)

      testConditional(input, distribution)
    }
  }
}
