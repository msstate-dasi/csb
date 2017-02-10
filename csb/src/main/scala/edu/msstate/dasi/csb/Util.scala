package edu.msstate.dasi.csb

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD

object Util {
  /**
   * Squashes part files of saveAsTextFile() together into a single file using the Hadoop’s merge function.
   *
   * @note This is a better approach rather than rdd.coalesce(1).saveAsTextFile() because the latter will put all
   *       final data through a single reduce task, with no parallelism and the risk of overloading an executor.
   */
  def merge(srcPath: String, dstPath: String): Boolean = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  /**
   * Saves a (key,value) RDD to a CSV file.
   */
  def RDDtoCSV[K, V](rdd: RDD[(K, V)], filename: String, overwrite: Boolean): Boolean = {
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
   * Executes a task and prints the elapsed time.
   *
   * @param taskName the name of the task
   * @param task the code block to be executed

   * @return the return value of the task
   */
  def time[R](taskName: String, task: => R): R = {
    println("[TIME] " + taskName + " started...")
    val start = System.nanoTime
    val ret = task // call-by-name
    val end = System.nanoTime
    println("[TIME] " + taskName + " completed in " + (end - start) / 1e9 + " s")
    ret
  }
}
