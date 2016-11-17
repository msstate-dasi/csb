import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.rdd.RDD


/***
  * Created by spencer on 11/3/16.
  *
  * base_GraphGen: Class containing inherited methods that both ba_GraphGen and kro_GraphGen use
  */
class base_GraphGen {

  /*** Print the graph to the terminal
    *
    * @param theGraph The graph to be printed
    * @return Tuple containing the Facts, degrees, and degree list of the graph
    */
  def printGraph(theGraph: Graph[nodeData, edgeData]) = {
    val facts: RDD[String] = theGraph.triplets.map(record =>
      record.srcId + " " + record.dstId + " " + record.attr)
    println("Graph:")
    facts.foreach(println)
    println()

    val nodeDegs: VertexRDD[Int] = theGraph.degrees
    //println("(ID, Degree):")
    //nodeDegs.foreach(println)
    //println()

    val degList: RDD[(Int, Int)] = nodeDegs.map(record => (record._2,1)).reduceByKey(_+_)
    //println("(Deg, Number of Nodes with Deg")
    //degList.foreach(println)
    //println()

    (facts, nodeDegs, degList)
  }

  /*** Saves the graph to the specified path
    *
    * @param sc Current SparkContext
    * @param theGraph The graph to save
    * @param path The folder name to save the graph files under
    */
  def saveGraph(sc: SparkContext, theGraph: Graph[nodeData, edgeData], path: String): Unit = {
    val edgeFacts: RDD[String] = sc.parallelize(Array("Source,Target,Weight")).union(theGraph.triplets.map(record =>
      record.srcId + "," + record.dstId + "," + record.attr))
    val vertFacts: RDD[String] = sc.parallelize(Array("ID,Desc")).union(theGraph.vertices.map(record => record._1.toString + record._2.toString))
    try {
      edgeFacts.repartition(1).saveAsTextFile(path)
      vertFacts.repartition(1).saveAsTextFile(path+"_vert")
    } catch {
      case e: Exception => println("Couldn't save file " + path)
    }
  }

  /*** Function to generate the degree distribution
    *
    * @param theGraph The graph to analyze
    * @return RDD containing degree numbers, and the number of nodes at that specific degree
    */
  def inDegreesDist(theGraph: Graph[nodeData, edgeData]): RDD[(Int,Int)] = {
    theGraph.inDegrees.map(record => (record._2,1)).reduceByKey(_+_)
  }

  /*** Function to generate outDegree distribution
    *
    * @param theGraph The graph to analyze
    * @return RDD containing degree numbers, and the number of nodes at that specific degree
    */
  def outDegreesDist(theGraph: Graph[nodeData, edgeData]): RDD[(Int,Int)] = {
    theGraph.outDegrees.map(record => (record._2,1)).reduceByKey(_+_)
  }
}
