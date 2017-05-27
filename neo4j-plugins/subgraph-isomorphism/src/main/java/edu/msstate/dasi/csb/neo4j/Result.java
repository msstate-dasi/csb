package edu.msstate.dasi.csb.neo4j;

import org.neo4j.graphdb.Node;

/**
 * The Result class defines the output to the Cypher console
 * Use ONLY String and Node for instance variable!!
 * Other object types may cause Neo4j server start failure
 *
 * The instance variables will be posted into different columns
 * e.g., in the Cypher console, four columns will be created for the returned results: resultNode, patternNode, index and totalNumSubgraph
 *
 * targetNode: a node in the isomorphic subgraph of the target graph
 * patternNode: a node in the pattern graph
 * subgraphIndex: which subgraph does the resultNode belong to
 */

public class Result {

    public Long subgraphIndex;

    public Node patternNode;

    public Node targetNode;


    public Result(Long subgraphIndex,Node patternNode,Node targetNode) {

        this.subgraphIndex = subgraphIndex;

        this.patternNode = patternNode;

        this.targetNode = targetNode;

    }
}


