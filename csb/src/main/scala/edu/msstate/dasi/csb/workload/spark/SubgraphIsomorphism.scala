package edu.msstate.dasi.csb.workload.spark

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Finds one or more subgraphs of the graph which are isomorphic to a pattern.
 */
class SubgraphIsomorphism[PVD: ClassTag, PED: ClassTag](engine: SparkEngine, pattern: Graph[PVD, PED]) extends Workload {
  val name = "Subgraph Isomorphism"

  /**
   * Finds one or more subgraphs of the graph which are isomorphic to a pattern.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val patternVerticesCount = pattern.vertices.count

    val partitions = graph.vertices.getNumPartitions

    val patternVerticesWithDegree = pattern.outDegrees.union(
      // Get all vertices with a degree value of 0
      pattern.vertices.subtractByKey(pattern.outDegrees).mapValues( _ => 0)
    )

    val graphVerticesWithDegree = graph.outDegrees.union(
      // Get all vertices with a 0 degree
      graph.vertices.subtractByKey(graph.outDegrees).mapValues( _ => 0)
    )

    /**
     * The definition of a candidate is the following statement:
     *
     * y is a candidate of x iff degree(y) ≥ degree(x)
     *
     * c(x) = {y ∈ vertices(graph): degree(y) ≥ degree(x)}
     */
    val candidates = patternVerticesWithDegree.sortBy(_._2, ascending = false)
      .cartesian(graphVerticesWithDegree)
      .coalesce(partitions)
      .filter{ case ( (_, vertexDegree), (_, candidateDegree) ) => candidateDegree >= vertexDegree }
      .repartition(partitions)
      .map{ case ( (vertex, _), (candidate, _) ) => (vertex, Array(candidate)) }
      .reduceByKey((array1, array2) => array1 ++ array2, partitions)
      .cache()

    if (candidates.count < patternVerticesCount) {
      // One or more vertices of the pattern have no candidates
      println("Subgraph not found, one or more vertices of the pattern have no initial candidates.")

      candidates.unpersist()
      return
    }

    val graphNeighbors = graph.collectNeighborIds(EdgeDirection.Out).cache()

    val patternNeighbors = pattern.collectNeighborIds(EdgeDirection.Out)
      .flatMap{ case (vertex, neighbors) => neighbors.map( (vertex, _) ) } // Unroll the neighbors array into separate entries
      .cache()

    val refinedCandidates = refine(candidates, graphNeighbors, patternNeighbors, partitions).cache()
    val refinedCandidatesCount = refinedCandidates.count

    candidates.unpersist()

    if (refinedCandidatesCount < patternVerticesCount) {
      // One or more vertices of the pattern have no candidates
      println("Subgraph not found, one or more vertices of the pattern have no refined candidates.")

      refinedCandidates.unpersist()
      graphNeighbors.unpersist()
      patternNeighbors.unpersist()
      return
    }

    val count = backtracking(refinedCandidates, patternVerticesCount, graphNeighbors, patternNeighbors, partitions)

    println(s"$count subgraphs found.")

    refinedCandidates.unpersist()
    graphNeighbors.unpersist()
    patternNeighbors.unpersist()
  }

  /**
   * Refines the candidates' list, keeping only the candidates that satisfy the following statement:
   *
   * "y is a refined candidate of x iff every neighbor of x has at least one candidate among neighbors of y"
   *
   * r(x) = {y ∈ c(x): ∀w ∈ n(x).c(W) ∩ n(y) ≠ ∅}
   */
  private def refine(candidates: RDD[(VertexId, Array[VertexId])], graphNeighbors: RDD[(VertexId, Array[VertexId])],
                     patternNeighbors: RDD[(VertexId, VertexId)], partitions: Int): RDD[(VertexId, Array[VertexId])] = {
    val neighborsOfCandidates = candidates
      .flatMap{ case (patternVertex, candidatesArray) => candidatesArray.map( (patternVertex, _) ) }
      .map(_.swap).join(graphNeighbors)
      .map{ case (candidate, (vertex, candidateNeighbors)) => (vertex, (candidate, candidateNeighbors)) }

    val candidatesOfPatternNeighbors = patternNeighbors.map(_.swap).join(candidates)
      .map{ case (_, (vertex, candidatesArray)) => (vertex, Array(candidatesArray))}
      .reduceByKey((array1, array2) => array1 ++ array2, partitions)

    val refinedCandidates = neighborsOfCandidates.leftOuterJoin(candidatesOfPatternNeighbors)
      .filter{
        case (_, ((_, candidateNeighbors), Some(candidatesOfNeighbors))) =>
          candidatesOfNeighbors.forall( _.intersect(candidateNeighbors).nonEmpty )
        case (_, ((_, _), None)) => true
      }
      .repartition(partitions)
      .map{ case (vertex, ((candidate, _), _)) => (vertex, Array(candidate)) }
      .reduceByKey((array1, array2) => array1 ++ array2, partitions)

    refinedCandidates
  }

  /**
   * For each vertex with only one candidate, removes any other candidate's occurrences among the candidates of the
   * other vertices.
   */
  private def cleanup(candidates: RDD[(VertexId, Array[VertexId])], partitions: Int): RDD[(VertexId, Array[VertexId])] = {
    val uniqueCandidates = candidates.filter{ case (_, candidatesArray) => candidatesArray.length == 1 }
      .repartition(partitions)
      .values

    if ( ! uniqueCandidates.isEmpty() ) {
      val cleanedCandidates = candidates.cartesian(uniqueCandidates).coalesce(partitions)
        .map { case ((vertex, candidatesArray), uniqueCandidate) => if (candidatesArray.length > 1) {
          (vertex, candidatesArray.diff(uniqueCandidate))
        } else {
          (vertex, candidatesArray)
        }
        }.reduceByKey((array1, array2) => array1.intersect(array2), partitions)

      cleanedCandidates
    } else {
      candidates
    }
  }

  /**
   * Prints the candidates' list.
   */
  private def printCandidates(candidates: RDD[(VertexId, Array[VertexId])]): Unit = {
    for ( (vertex, candidatesArray) <- candidates.toLocalIterator ) {
      println(s"$vertex -> ${candidatesArray.mkString(",")}")
    }
  }

  /**
   * Given a vertex and one of its candidates:
   * * removes any other candidate's occurrences among the candidates of the other vertices
   * * removes all the other vertex's candidates
   */
  private def select(candidates: RDD[(VertexId, Array[VertexId])], selectedVertex: VertexId,
                     selectedCandidate: VertexId): RDD[(VertexId, Array[VertexId])] = {
    candidates.map{ case (vertex, candidatesArray) =>
      if (vertex == selectedVertex) {
        (vertex, Array(selectedCandidate))
      } else {
        (vertex, candidatesArray.filter(_ != selectedCandidate))
      }
    }
  }

  /**
   * Goes through the candidates' list following these steps:
   *  1. Cleanup unique candidates
   *  2. Select the vertex with fewer candidates (at least two) and select one of its candidates
   *  3. Refine
   *  4. Backtrack
   *
   * Stop when:
   * * any vertex has no more candidates, OR
   * * every vertex has exactly one candidate.
   */
  def backtracking(candidates: RDD[(VertexId, Array[VertexId])], patternVerticesCount: Long,
                   graphNeighbors: RDD[(VertexId, Array[VertexId])], patternNeighbors: RDD[(VertexId, VertexId)],
                   partitions: Int): Int = {
    var count = 0

    // Cleanup unique candidates
    val cleanedCandidates = cleanup(candidates, partitions).cache()

    // Extract all candidates which have more than one candidate
    val actualCandidates = cleanedCandidates.filter{ case (_, candidatesArray) => candidatesArray.length > 1 }
      .repartition(partitions)

    if ( actualCandidates.isEmpty() ) {
      // Every vertex has exactly one candidate
      count += 1

      println("** Subgraph found **")
      printCandidates(cleanedCandidates)
      println("********************")
      cleanedCandidates.unpersist()

      return count
    }

    // Pick the first vertex with the lowest number of candidates (at least two)
    val (currentVertex, currentArray) = actualCandidates.sortBy(_._2.length).first()

    for (candidate <- currentArray) {
      val candidatesAttempt = select(cleanedCandidates, currentVertex, candidate).cache()

      val candidatesResult = refine(candidatesAttempt, graphNeighbors, patternNeighbors, partitions).cache()
      val candidatesResultCount = candidatesResult.count

      candidatesAttempt.unpersist()
      cleanedCandidates.unpersist()

      if (candidatesResultCount == patternVerticesCount) {
        if ( candidatesResult.filter{ case (_, candidatesArray) => candidatesArray.length > 1 }.isEmpty() ) {
          // Every vertex has exactly one candidate
          count += 1

          println("** Subgraph found **")
          printCandidates(candidatesResult)
          println("********************")
        } else {
          // Some vertex has more than one candidates, backtrack
          count += backtracking(candidatesResult, patternVerticesCount, graphNeighbors, patternNeighbors, partitions)
        }
      }
      candidatesResult.unpersist()
    }
    count
  }
}
