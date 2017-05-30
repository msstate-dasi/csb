package edu.msstate.dasi.csb.neo4j;

import org.neo4j.graphdb.Node;

/**
 * The Result class defines the output of the procedure.
 */
public class Result {

    public Long subgraphIndex;
    public Node patternNode;
    public Node targetNode;

    /**
     * Builds a Result object.
     *
     * @param subgraphIndex which subgraph does the resultNode belong to
     * @param patternNode a node in the pattern graph
     * @param targetNode a node in the isomorphic subgraph of the target graph
     */
    public Result(Long subgraphIndex, Node patternNode, Node targetNode) {
        this.subgraphIndex = subgraphIndex;
        this.patternNode = patternNode;
        this.targetNode = targetNode;
    }
}
