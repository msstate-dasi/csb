package edu.msstate.dasi.csb

import java.io.File

import org.apache.spark.graphx.{Graph, VertexRDD}

import org.neo4j.io.fs.{DefaultFileSystemAbstraction, FileUtils}
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.logging.{LogService, SimpleLogService}
import org.neo4j.logging.FormattedLogProvider
import org.neo4j.unsafe.impl.batchimport.{Configuration, InputIterable, InputIterator, ParallelBatchImporter}
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers
import org.neo4j.unsafe.impl.batchimport.input._
import org.neo4j.unsafe.impl.batchimport.staging.{ExecutionMonitor, ExecutionMonitors}

object Neo4jImporter {
  private class SparkInput(graph: Graph[VertexData, EdgeData]) extends Input {

    private class VertexIterator(vertices: VertexRDD[VertexData]) extends Iterator[InputNode] {
      private val labels = Array("seed")

      private val iterator = vertices.toLocalIterator

      def hasNext: Boolean = iterator.hasNext

      def next(): InputNode = {
        val (vertexId, vertexData) = iterator.next()
        new InputNode(vertices.name, 0, 0, vertexId, Array("name", vertexId.asInstanceOf[AnyRef]), null, labels, null)
      }
    }

    /**
     * @return IdMapper which will get populated by InputNode#id() input node ids and later queried by
     *         InputRelationship#startNode() and InputRelationship#endNode() ids to resolve potentially temporary input
     *         node ids to actual node ids in the database.
     */
    def idMapper(): IdMapper = IdMappers.actual()

    /**
     * @return IdGenerator which is responsible for generating actual node ids from input node ids.
     */
    def idGenerator(): IdGenerator = IdGenerators.startingFromTheBeginning

    /**
     * @return a Collector capable of writing bad InputRelationship and duplicate InputNode to an output stream for later
     *         handling.
     */
    def badCollector(): Collector = Collectors.badCollector(System.err, 0)

    /**
     * Provides all InputNode input nodes for an import. The returned InputIterable iterable's iterator() method may be
     * called multiple times.
     *
     * @return an InputIterable which will provide all InputNode input nodes for the whole import.
     */
    def nodes(): InputIterable[InputNode] = {
      val vertices = new VertexIterator(graph.vertices)

      new InputIterable[InputNode] {
        def supportsMultiplePasses(): Boolean = vertices.isTraversableAgain

        def iterator(): InputIterator[InputNode] = new InputIterator[InputNode] {
          def sourceDescription(): String = ""

          def position(): Long = 0

          def lineNumber(): Long = 0

          def close(): Unit = {}

          def next(): InputNode = vertices.next()

          def hasNext: Boolean = vertices.hasNext
        }
      }
    }

    /**
     * Provides all input relationships for an import. The returned iterable's iterator() method may be called multiple
     * times.
     *
     * @return an InputIterable which will provide all input relationships for the whole import.
     */
    def relationships(): InputIterable[InputRelationship] = {
      new InputIterable[InputRelationship] {
        def supportsMultiplePasses(): Boolean = false

        def iterator(): InputIterator[InputRelationship] = new InputIterator[InputRelationship] {
          def sourceDescription(): String = ""

          def position(): Long = 0

          def lineNumber(): Long = 0

          def close(): Unit = {}

          def next(): InputRelationship = null

          def hasNext: Boolean = false
        }
      }
    }
  }

  private def createConfiguration(): Configuration = Configuration.DEFAULT

  private def createExecutionMonitors(): ExecutionMonitor = ExecutionMonitors.defaultVisible

  private def createLogging(): LogService = {
    new SimpleLogService(
      FormattedLogProvider.toOutputStream(System.err),
      FormattedLogProvider.toOutputStream(System.err)
    )
  }

  def apply(graph: Graph[VertexData, EdgeData], dbPath: String, logsPath: String = "__logs"): Unit = {
    FileUtils.deleteRecursively(new File(dbPath))

    val dbDir = new File(dbPath)
    val fs = new DefaultFileSystemAbstraction()
    val logsDir = new File(logsPath)
    fs.mkdirs(logsDir)

    val importer = new ParallelBatchImporter(dbDir, createConfiguration(), createLogging(), createExecutionMonitors(),
      Config.defaults())

    importer.doImport(new SparkInput(graph))
  }
}
