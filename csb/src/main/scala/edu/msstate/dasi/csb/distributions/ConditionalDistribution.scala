package edu.msstate.dasi.csb.distributions

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Represents a conditional probability distribution.
 *
 * @note the resulting distribution is expected to be small, as it is loaded into the driver's memory.
 *
 * @param data the input data on which the distribution will be computed
 * @tparam Value the input data type
 * @tparam Cond the data type of the conditioning value
 */
class ConditionalDistribution[Value: ClassTag, Cond: ClassTag](data: RDD[(Value, Cond)]) extends Serializable {

  /**
   * The internal representation, a Map of conditioning values to [[Distribution]] objects.
   */
  private val distributions = {

    val occurrences = data.map((_, 1L)).reduceByKey(_+_).cache() // Count how many occurrences for each value

    // Compute the total amount of elements for each conditioning value
    val occurrencesSums = occurrences.map{ case ((_, cond), count) => (cond, count) }.reduceByKey(_+_)

    val distributions = occurrences.map{ case ((value, cond), count) => (cond, (value, count)) }
      .join(occurrencesSums)
      .map{ case (cond, ((value, count), sum)) => (cond, Array((value, count / sum.toDouble))) } // Normalize to obtain probabilities
      .reduceByKey(_++_)
      .map{ case (cond, array) =>
        (cond, new Distribution(array.sortBy(_._2)(Ordering[Double].reverse))) // Descending order to maximize sampling performance
      }

    val result = distributions.collectAsMap()

    occurrences.unpersist()

    result
  }

  /**
   * Returns a sample of the distribution given the conditioning value.
   */
  def sample(conditioner: Cond): Value = {
    distributions(conditioner).sample
  }
}
