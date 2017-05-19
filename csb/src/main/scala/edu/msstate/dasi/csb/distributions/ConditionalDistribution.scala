package edu.msstate.dasi.csb.distributions

import org.apache.spark.rdd.RDD

import scala.collection.mutable
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
   * The internal representation, a Map of `Conditioner` values to [[Distribution]] objects.
   */
  private val distributions: mutable.Map[Cond, Distribution[Value]] = {

    var distributions = mutable.Map.empty[Cond, Distribution[Value]]

    val occurrences = data.map((_, 1L)).reduceByKey(_+_) // Count how many occurrences for each value
      .sortBy(_._2, ascending = false) // Descending order to maximize sampling performance
      .cache()

    val occurrencesSums = occurrences.map{ case ((_, cond), count) => (cond, count) }.reduceByKey(_+_).collect()

    for ( (conditioningValue, occurrencesSum) <- occurrencesSums ) {
      // For each conditioning value, create a Distribution object
      val conditionedData = occurrences.filter{ case ((_, conditioner), _) => conditioner == conditioningValue }
        .map{ case ((value, _), count) => (value, count) }
      distributions += (conditioningValue -> Distribution(conditionedData, occurrencesSum))
    }

    occurrences.unpersist()

    distributions
  }

  /**
   * Returns a sample of the distribution given the conditioning value.
   */
  def sample(conditioner: Cond): Value = {
    distributions(conditioner).sample
  }
}
