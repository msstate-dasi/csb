package edu.msstate.dasi

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._

import scala.reflect.ClassTag

/**
 * Helper methods to compute veracity metrics for [[org.apache.spark.graphx.Graph]]
 */
object Veracity {
  var globalBucketSize = 0.0

  /**
   * Computes a normalized bucketed distribution given a list of keys
   *
   * @return RDD containing the normalized distribution
   */
  private def normDistRDD(keys: RDD[Double], bucketSize: Double): RDD[(Double, Double)] = {

    // Computes the sum of keys
    val keysSum = keys.sum()

    // Normalizes keys
    var normKeys = keys.map { key => key / keysSum }

    if (bucketSize > 0) {
      val normBucketSize = bucketSize / keysSum
      // Groups keys in buckets adding their values
      normKeys = normKeys.map { key => key - key % normBucketSize }
      globalBucketSize = normBucketSize
    }

    // Computes how many times each key appears
    val keysCount = normKeys.map { key => (key, 1L) }.reduceByKey(_ + _).cache()

    // Computes the sum of values
    val valuesSum = keysCount.values.sum()

    // Normalizes values
    val normDist = keysCount.map { case (key, value) => (key, value / valuesSum) }

    normDist
  }

  /**
   * Squashes saveAsTextFile() part files together into a single file using the Hadoop’s merge function
   *
   * @note This is a better approach rather than rdd.coalesce(1).saveAsTextFile() because the latter will put all
   *       final data through a single reduce task, with no parallelism and the risk of overloading an executor
   */
  private def merge(srcPath: String, dstPath: String) = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  /**
   * Save a (key,value) RDD to a CSV file
   */
  private def RDDtoCSV[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], filename: String, overwrite: Boolean) = {
    val tmpFile = "__RDDtoCSV.tmp"
    val numPartitions = 16

    if (overwrite) {
      FileUtil.fullyDelete(new File(filename))
    }

    rdd.map {
      // for each (key,value) pair, create a "key,value" string
      case (key, value) => Array(key, value).mkString(",")
    }.coalesce(numPartitions).saveAsTextFile(tmpFile)

    merge(tmpFile, filename)

    FileUtil.fullyDelete(new File(tmpFile))
  }

  /**
   * Computes the Euclidean distance between the values of two (key,value) RDDs
   */
  private def euclideanDistance[V: ClassTag](rdd1: RDD[(V, Double)], rdd2: RDD[(V, Double)]): Double = {
    math.sqrt(
      // Unifies the RDDs
      rdd1.union(rdd2)
        // Computes the squared difference of the values belonging to the same keys
        .reduceByKey((value1, value2) => math.pow(value1 - value2, 2))
        // Sum all the resulting values
        .values.sum()
    )
  }

  /**
   * Computes the degree veracity factor between the degrees of two graphs
   *
   * @param seed
   * @param synth
   * @param saveDistAsCSV
   * @param filePrefix
   * @param overwrite
   *
   * @return
   */
  def degree(seed: VertexRDD[Int], synth: VertexRDD[Int], saveDistAsCSV: Boolean = false, filePrefix: String = "",
             overwrite: Boolean = false): Double = {
    val seedValues = seed.values.map { value => value.toDouble }.cache()
    // TODO: Handle the case when the array has less than two elements
    val seedBucketSize = seedValues.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min

    val synthValues = synth.values.map { value => value.toDouble }.cache()
    // TODO: Handle the case when the array has less than two elements
    val synthBucketSize = synthValues.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min

    val bucketSize = math.min(seedBucketSize, synthBucketSize)

    val seedRDD = normDistRDD(seedValues, bucketSize).cache()
    val synthRDD = normDistRDD(synthValues, bucketSize).cache()

    if (saveDistAsCSV) {
      RDDtoCSV(seedRDD, filePrefix + "_degrees_dist.seed.csv", overwrite)
      RDDtoCSV(synthRDD, filePrefix + "_degrees_dist.synth.csv", overwrite)
    }

    val bucketNum = 1.0 / globalBucketSize

    euclideanDistance(seedRDD, synthRDD) / bucketNum
  }

  /**
   * Computes the PageRank veracity factor between the PageRank of two graphs
   *
   * @return
   */
  def pageRank[VD: ClassTag, ED: ClassTag](seed: Graph[VD, ED], synth: Graph[VD, ED], saveDistAsCSV: Boolean = false,
                                           filePrefix: String = "", overwrite: Boolean = false): Double = {
    val pageRankTolerance = 0.001

    // Computes the bucket size as the minimum difference between any successive pair of ordered values
    val seedPrResult = seed.pageRank(pageRankTolerance).vertices.values
    val seedBucketSize = seedPrResult.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min

    val synthPrResult = synth.pageRank(pageRankTolerance).vertices.values
    val synthBucketSize = synthPrResult.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min

    val bucketSize = math.min(seedBucketSize, synthBucketSize)

    val seedRDD = normDistRDD(seedPrResult, bucketSize).cache()
    val synthRDD = normDistRDD(synthPrResult, bucketSize).cache()

    if (saveDistAsCSV) {
      RDDtoCSV(seedRDD, filePrefix + "_page_rank_dist.seed.csv", overwrite)
      RDDtoCSV(synthRDD, filePrefix + "_page_rank_dist.synth.csv", overwrite)
    }

    val bucketNum = 1.0 / globalBucketSize

    euclideanDistance(seedRDD, synthRDD) / bucketNum
  }
}