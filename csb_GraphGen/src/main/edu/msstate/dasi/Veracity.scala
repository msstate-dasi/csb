package edu.msstate.dasi

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph, VertexRDD}

import scala.reflect.ClassTag

/**
 * Helper methods to compute veracity metrics for [[org.apache.spark.graphx.Graph]]
 */
object Veracity {

  /**
   * Computes the neighboring vertex degrees distribution
   *
   * @param degrees The degrees to analyze
   * @return RDD containing the normalized degrees distributions
   */
  private def degreesDistRDD(degrees: VertexRDD[Int]): RDD[(Int, Double)] = {
    val degreesCount = degrees.map(x => (x._2, 1L)).reduceByKey(_ + _).cache()
    val degreesTotal = degreesCount.map(_._2).reduce(_ + _)
    degreesCount.map(x => (x._1, x._2 / degreesTotal.toDouble)).sortBy(_._2, ascending = false)
  }

  /**
   * Squashes saveAsTextFile() part files together into a single file using the Hadoop’s merge function
   *
   * @param srcPath
   * @param dstPath
   *
   * @note This is a better approach rather than rdd.coalesce(1).saveAsTextFile() because the latter will put all
   *       final data through a single reduce task, with no parallelism and the risk of overloading an executor
   */
  private def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  /**
   * Save a (key,value) RDD to a CSV file
   *
   * @param rdd
   * @param filename
   * @param overwrite
   */
  private def RDDtoCSV[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], filename: String, overwrite :Boolean): Unit = {
    val tmpFile = "__RDDtoCSV.tmp"

    if (overwrite) {
      FileUtil.fullyDelete(new File(filename))
    }

    rdd.map {
      // for each (key,value) pair, create a "key,value" string
      case (key, value) => Array(key, value).mkString(",")
    }.saveAsTextFile(tmpFile)

    merge(tmpFile, filename)

    FileUtil.fullyDelete(new File(tmpFile))
  }

  /**
   * Computes the squared Euclidean distance between two vectors
   *
   * @note If the arrays differ in size, the lower size defines how many elements are taken for the computation
   */
  private def squaredDistance(v1: Array[Double], v2: Array[Double]): Double = {
    var d = 0.0

    for (i <- 0 until v1.length.min(v2.length)) {
      d += math.pow( v1(i) - v2(i), 2 )
    }
    d
  }

  /**
   * Computes the degree veracity factor between the degrees of two graphs
   *
   * @param seed
   * @param synth
   * @param saveDistAsCSV
   * @param filePrefix
   * @param overwrite
   */
  def degree(seed: VertexRDD[Int], synth: VertexRDD[Int], saveDistAsCSV : Boolean = false, filePrefix: String = "",
              overwrite :Boolean = false): Double = {
    val seedRDD = degreesDistRDD(seed).cache()
    val synthRDD = degreesDistRDD(synth).cache()

    if (saveDistAsCSV) {
      RDDtoCSV(seedRDD, filePrefix + "_degrees_dist.seed.csv", overwrite)
      RDDtoCSV(synthRDD, filePrefix + "_degrees_dist.synth.csv", overwrite)
    }

    val v1 = seedRDD.map(_._2).collect()
    val v2 = synthRDD.map(_._2).collect()

    seedRDD.unpersist()
    synthRDD.unpersist()

    squaredDistance(v1,v2)
  }

  /**
   * Computes the effective diameter of the graph, defined as the minimum number of links (steps/hops) in which some
   * fraction (or quantile q, say q = 0.9) of all connected pairs of nodes can reach each other
   *
   * @param graph The graph to analyze
   * @return The value of the effective diameter
   */
  def effectiveDiameter(graph: Graph[nodeData, edgeData]): Long = {
    0
  }
}
