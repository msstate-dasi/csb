package edu.msstate.dasi.csb.workload.neo4j

import java.util.concurrent.TimeUnit

import org.neo4j.driver.v1.{AccessMode, AuthTokens, GraphDatabase}
import org.neo4j.driver.v1.summary.ResultSummary

class Neo4jEngine(url: String, username: String, password: String) {
  /**
   * The Neo4j driver instance responsible for obtaining new sessions.
   */
  private val driver = GraphDatabase.driver(url, AuthTokens.basic(username, password))

  /**
   *
   */
  private def printSummary(querySummary: ResultSummary): Unit = {
    val timeAfter = querySummary.resultAvailableAfter(TimeUnit.MILLISECONDS) / 1000.0
    val timeConsumed = querySummary.resultConsumedAfter(TimeUnit.MILLISECONDS) / 1000.0

    println(s"[NEO4J] Query completed in $timeAfter s")
    println(s"[NEO4J] Result consumed in $timeConsumed s")
    println(s"[NEO4J] Execution completed in ${timeAfter + timeConsumed} s")
  }

  /**
   *
   */
  def run(query: String, accessMode: AccessMode = AccessMode.READ): Unit = {
    val session = driver.session(accessMode)

    val result = session.run(query)

    while ( result.hasNext ) result.next()

    printSummary( result.consume() )

    session.close()
  }
}
