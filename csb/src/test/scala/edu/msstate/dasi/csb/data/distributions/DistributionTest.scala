package edu.msstate.dasi.csb.data.distributions

import com.holdenkarau.spark.testing.RDDGenerator
import edu.msstate.dasi.csb.test.PropertySpec
import org.scalacheck.{Arbitrary, Gen}
import org.scalactic.anyvals.PosZInt

import scala.collection.mutable

class DistributionTest extends PropertySpec {

  private val samples = 1000000
  private val tolerance = 0.01

  private val inputMinSize = PosZInt(10)
  private val inputSizeRange = PosZInt(20)

  override implicit val generatorDrivenConfig =
    PropertyCheckConfiguration(minSize = inputMinSize, sizeRange = inputSizeRange)

  private def testProbability[T](input: Map[T, Double], distribution: Distribution[T]) = {
    val counts = mutable.Map() ++ input.mapValues(_ => 0)

    for (_ <- 0 until samples) {
      counts(distribution.sample) += 1
    }

    for ((value, prob) <- input) {
      assert((counts(value).toDouble / samples - prob).abs < tolerance)
    }
  }

  test("a Distribution from an array should return samples according to the array probabilities") {
    val pairGen = for {
      value <- Gen.alphaChar
      prob <- Gen.posNum[Long]
    } yield (value, prob)

    val elementsGen = for (elements <- Gen.nonEmptyMap(pairGen)) yield elements

    forAll(elementsGen) { elements =>
      val sum = elements.values.sum
      val input = elements.mapValues(_ / sum.toDouble)
      val distribution = new Distribution(input.toArray)

      testProbability(input, distribution)
    }
  }

  test("a Distribution from an RDD should return samples according to the probability of the input occurrences") {
    forAll(RDDGenerator.genRDD[Char](sc)(Arbitrary.arbitrary[Char])) { rdd =>
      val sequence = rdd.collect
      val input = sequence.groupBy(identity).mapValues(_.length.toDouble / sequence.length)
      val distribution = Distribution(rdd)

      testProbability(input, distribution)
    }
  }
}
