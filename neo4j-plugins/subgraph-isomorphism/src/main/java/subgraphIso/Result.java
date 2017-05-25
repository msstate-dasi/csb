package subgraphIso;

/**
 * The Result class defines the output to the Cypher console
 * Use ONLY String and Node for instance variable!!
 * Other object types may cause Neo4j server start failure
 *
 * The instance variables will be posted into different columns
 * e.g., in the Cypher console, four columns will be created for the returned results: resultNode, queryNode, index and totalNumSubgraph
 *
 * resultNodeProperty: a node's property in the matching subgraph
 * queryNodeID: a node's unique Neo4j ID in the query graph
 * subgraphIndex: which subgraph does the resultNode belong to
 * totalNumSubgraph: the total number of subgraphs
 */

public class Result {

    public String resultNodeProperty;

    public String queryNodeID;

    public String subgraphIndex;

    public String totalNumSubgraph;
   // public String executionTime;

    public Result(String resultNodeProperty, String queryNodeID, String subgraphIndex, String totalNumSubgraph) {

        this.resultNodeProperty = resultNodeProperty;

        this.queryNodeID = queryNodeID;

        this.subgraphIndex = subgraphIndex;

        this.totalNumSubgraph=totalNumSubgraph;
        //this.executionTime=executionTime;
    }
}


