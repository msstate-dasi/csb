package edu.msstate.dasi.csb

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

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
    println(s"[TIME] $taskName started...")
    val start = System.nanoTime
    val ret = task // call-by-name
    val end = System.nanoTime
    println(s"[TIME] $taskName completed in ${(end - start) / 1e9} s")
    ret
  }

  def convertLabelsToStandardForm[VD: ClassTag, ED: ClassTag](G: Graph[VD, ED]): Graph[VertexData, EdgeData] =
  {
   val nodeList = G.vertices
    val edgeList = G.edges
    val hash = new mutable.HashMap[Long, Long]
    val nodes = nodeList.map(record => record._1).collect()
    var counter = 0
    for(entry <- nodes)
    {
      hash.put(entry, counter)
      counter += 1
    }
    val newNodes = nodeList.map(record => hash.get(record._1).head).sortBy(record => record, ascending = true)
    val newEdges = edgeList.map(record => (hash.get(record.srcId).head, hash.get(record.dstId).head))
    val newEdgesRDD: RDD[Edge[EdgeData]] = newEdges.map(record => Edge(record._1, record._2))
    //    val newEdges = edgeList.flatMap(record => Array((hash.get(record._1).head, hash.get(record._2).head), (hash.get(record._2).head, hash.get(record._1).head)))
    return Graph.fromEdges(newEdgesRDD, VertexData())
  }

  def stripMultiEdges(G: Graph[VertexData, EdgeData]): Graph[VertexData, EdgeData] =
  {
    val stripedEdges = G.edges.groupBy(record => (record.srcId, record.dstId)).map(record => record._2.head)
    return Graph.fromEdges(EdgeRDD.fromEdges(stripedEdges), VertexData())
  }

}
