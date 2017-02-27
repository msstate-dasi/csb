package edu.msstate.dasi

import org.apache.spark.sql.SparkSession

/**
 * This package provides functions to approximate the diameter of large graphs.
 * The main entry point to the library is the [[edu.msstate.dasi.csb.Benchmark]] object
 */
package object csb {
  private[csb] val sc = SparkSession.builder()
    .appName("Cyber Security Benchmark")
    .config("spark.neo4j.bolt.url", "bolt://localhost")
    .config("spark.neo4j.bolt.user", "neo4j")
    .config("spark.neo4j.bolt.password", "password")
    .getOrCreate()
    .sparkContext
}
