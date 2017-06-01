package edu.msstate.dasi.csb.neo4j;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Subgraph Isomorphism
 * A Java Plugin for Neo4j
 */
public class SubgraphIsomorphism {
    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    @Procedure("csb.subgraphIsomorphism")
    @Description("Given the pattern graph label and the target graph label, execute subgraph isomorphism algorithm " +
            "and return subgraphs. CALL csb.subgraphIsomorphism(patternLabel, targetLabel) YIELD subgraphIndex, " +
            "patternNode, targetNode")
    public Stream<Result> subgraphIsomorphism(@Name("patternLabel") String patternLabelString,
                                              @Name("targetLabel") String targetLabelString,
                                              @Name(value = "parallelFactor", defaultValue = "2") Long parallelFactor) {
        // The pattern label
        Label patternLabel = Label.label(patternLabelString);

        // The target label
        Label targetLabel = Label.label(targetLabelString);

        // The available number of CPU cores
        int numCores = Runtime.getRuntime().availableProcessors();

        ForkJoinPool threadPool = new ForkJoinPool(numCores * parallelFactor.intValue());

        // The list that stores all pattern nodes
        ArrayList<Node> patternNodeList = new ArrayList<>();

        // Execute the algorithm
        List<List<Node>> matchedSubgraphs = ullmannAlg(patternLabel, targetLabel, patternNodeList, threadPool);

        ArrayList<Result> resultList = new ArrayList<>();

        try {
            /* Return the results.
             * Each row of the matchedSubgraphs contains nodes in a matched subgraph ordered by the pattern nodes in
             * the patternNodeList, i.e., patternNodeList.size()==matchedSubgraphs.get(i).size();
             */

            if (matchedSubgraphs.isEmpty()) return null;

            else {

                for (Long i = 0L; i < matchedSubgraphs.size(); i++) {

                    for (int j = 0; j < matchedSubgraphs.get(i.intValue()).size(); j++) {
                        resultList.add(new Result(i, patternNodeList.get(j), matchedSubgraphs.get(i.intValue()).get(j)));
                    }

                }

                return resultList.stream();
            }

        } catch (Exception e) {

            String errMsg = "Error encountered while calculating subgraph isomorphism.";

            log.error(errMsg, e);

            throw new RuntimeException(errMsg, e);
        }
    }

    private List<List<Node>> ullmannAlg(Label patternLabel,
                                        Label targetLabel,
                                        ArrayList<Node> patternNodeList,
                                        ForkJoinPool threadPool) {

        /* The incoming and outgoing neighbor lists of pattern vertices and target vertices are for supporting directed graphs************/

        // The incoming and outgoing neighbor lists for pattern vertices
        List<List<Node>> patternInNeighborList = new ArrayList<>();

        List<List<Node>> patternOutNeighborList = new ArrayList<>();

        // The incoming and outgoing neighbor lists for the Neo4j database with the target label
        List<List<Node>> targetInNeighborList = new ArrayList<>();

        List<List<Node>> targetOutNeighborList = new ArrayList<>();

        // Stores the final results
        List<List<Node>> matchedSubgraphs = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {
            /* Create the neighbor list for the Neo4j database with the target label **********************************/

            ResourceIterator<Node> targetNodes;

            ArrayList<Node> targetNeighborListIndex = new ArrayList<>();

            // Given a node, get the index in the node neighbor list
            Map<Node, Integer> targetNeighborListMap = new HashMap<>();

            if (targetLabel.name().equals("")) targetNodes = db.getAllNodes().iterator();
            else targetNodes = db.findNodes(targetLabel);

            int index = 0;

            while (targetNodes.hasNext()) {

                Node targetNode = targetNodes.next();

                // Pass the pattern graph nodes if the whole neo4j database is selected
                if (targetLabel.name().equals("") && targetNode.hasLabel(patternLabel)) continue;

                targetInNeighborList.add(findNodeNeighbors(targetNode, targetLabel, false));

                targetOutNeighborList.add(findNodeNeighbors(targetNode, targetLabel, true));

                targetNeighborListIndex.add(targetNode);

                targetNeighborListMap.put(targetNode, index);

                index++;
            }

            targetNodes.close();

            /* Create the candidate list and its index ****************************************************************/

            Map<Node, Integer> candidateListMap = new HashMap<>();

            List<List<Node>> candidateList = findCandidates(patternLabel, candidateListMap, targetInNeighborList, targetOutNeighborList,
                    targetNeighborListIndex, patternNodeList, threadPool);

            /* Create the pattern graph's neighbor lists ***************************************************************/

            patternNodeList.forEach(node -> {

                patternInNeighborList.add(new ArrayList<>());

                patternOutNeighborList.add(new ArrayList<>());

            });

            patternNodeList.parallelStream().forEach(node -> {
                try (Transaction tx1 = db.beginTx()) {

                    patternInNeighborList.set(candidateListMap.get(node), findNodeNeighbors(node, patternLabel, false));

                    patternOutNeighborList.set(candidateListMap.get(node), findNodeNeighbors(node, patternLabel, true));

                    tx1.success();

                }
            });

            /* Refine the candidate list preliminarily, check if it is valid ******************************************/

            refineCandidate(candidateList, patternInNeighborList, patternOutNeighborList, targetInNeighborList, targetOutNeighborList, candidateListMap, targetNeighborListMap);

            if (!isCorrect(candidateList)) {

                tx.success();

                return matchedSubgraphs;

            }


            /* Begin multithreading execution of the algorithm ********************************************************/

            int threadPoolSize = threadPool.getParallelism();

            int jobSize = candidateList.get(0).size();

            final long splitSize = (jobSize > threadPoolSize * 2) ? jobSize / (threadPoolSize * 2) : 1;

            SubgraphProcessor mainProcessor = new SubgraphProcessor(candidateList, candidateListMap,
                    patternInNeighborList, patternOutNeighborList,
                    targetInNeighborList, targetOutNeighborList, targetNeighborListMap,
                    splitSize, threadPool);

            Future<List<List<Node>>> futureMatchedSubgraphs = threadPool.submit(mainProcessor);

            try {

                matchedSubgraphs.addAll(futureMatchedSubgraphs.get());

            } catch (Exception e) {

                e.printStackTrace();
            }

            threadPool.shutdown();

            tx.success();
        }

        return matchedSubgraphs;
    }


    /**
     * Check if the current candidate list is correct. (i.e., is there any empty candidate list?)
     */
    private boolean isCorrect(List<List<Node>> candidateList) {

        for (List<Node> aCandidateList : candidateList) {

            if (aCandidateList.isEmpty()) return false;

        }

        return true;
    }

    /**
     * Given the candidate and neighbor lists, refine the candidate list
     */
    private void refineCandidate(List<List<Node>> candidateList,
                                 List<List<Node>> patternInNeighborList,
                                 List<List<Node>> patternOutNeighborList,
                                 List<List<Node>> targetInNeighborList,
                                 List<List<Node>> targetOutNeighborList,
                                 Map<Node, Integer> candidateListMap,
                                 Map<Node, Integer> targetNeighborListMap) {

        List<List<Node>> nodesToRemove = new ArrayList<>();

        // Create the list of node that should be removed
        candidateList.forEach(list -> nodesToRemove.add(new ArrayList<>()));

        IntStream.range(0, candidateList.size()).parallel().forEach(ii -> candidateList.get(ii).forEach(node -> {

            boolean inRefinable = patternInNeighborList.get(ii).parallelStream().allMatch(qnode ->
                    candidateList.get(candidateListMap.get(qnode)).parallelStream().anyMatch(subnode ->
                            targetInNeighborList.get(targetNeighborListMap.get(node)).contains(subnode)));

            boolean outRefinable = patternOutNeighborList.get(ii).parallelStream().allMatch(qnode ->
                    candidateList.get(candidateListMap.get(qnode)).parallelStream().anyMatch(subnode ->
                            targetOutNeighborList.get(targetNeighborListMap.get(node)).contains(subnode)));

            if (!(inRefinable && outRefinable)) nodesToRemove.get(ii).add(node);

        }));
    }

    /**
     * Find all pattern vertex candidates in the Neo4j database under the label "targetLabel"
     */
    private List<List<Node>> findCandidates(Label patternLabel,
                                            Map<Node, Integer> candidateListMap,
                                            List<List<Node>> targetInNeighborList, List<List<Node>> targetOutNeighborList,
                                            ArrayList<Node> targetNeighborListIndex,
                                            ArrayList<Node> patternNodeList,
                                            ForkJoinPool threadPool) {
        // Input an empty candidate-list index list for modification
        List<List<Node>> candidateList = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {

            ResourceIterator<Node> patternNodes = db.findNodes(patternLabel);

            while (patternNodes.hasNext()) {

                ArrayList<Node> candidateListPerVertex = new ArrayList<>();

                Node patternNode = patternNodes.next();

                int patternInDegree = patternNode.getDegree(Direction.INCOMING);

                int patternOutDegree = patternNode.getDegree(Direction.OUTGOING);

                for (int i = 0; i < targetNeighborListIndex.size(); i++) {

                    int targetInDegree = targetInNeighborList.get(i).size();

                    int targetOutDegree = targetOutNeighborList.get(i).size();

                    if (targetInDegree >= patternInDegree && targetOutDegree >= patternOutDegree)
                        candidateListPerVertex.add(targetNeighborListIndex.get(i));
                }

                // Temporarily append the pattern node to the first position of the list
                candidateListPerVertex.add(0, patternNode);

                candidateList.add(candidateListPerVertex);
            }

            // Sort the candidate list by the number of candidates (ascending)
            Comparator<List<Node>> candidateListSizeComparator = Comparator.comparingInt(List::size);

            candidateList.sort(candidateListSizeComparator);

            //make the size of the first element in the candidiate list close to 2*threadPoolSize

            int swapPoint = candidateList.size() - 1;

            for (int i = 0; i < candidateList.size(); i++) {

                if (candidateList.get(i).size() >= (threadPool.getParallelism() * 2 + 1)) {//consider also the temporarily appended node
                    swapPoint = i;

                    break;
                }
            }

            if (swapPoint != 0) Collections.swap(candidateList, 0, swapPoint);

            for (int i = 0; i < candidateList.size(); i++) {

                patternNodeList.add(candidateList.get(i).get(0));

                candidateListMap.put(candidateList.get(i).get(0), i);
            }

            // Remove the temporary pattern node at position 0
            candidateList.forEach(nodes -> nodes.remove(0));

            patternNodes.close();

            tx.success();
        }
        return candidateList;
    }

    /**
     * Find the neighbors of a node in the Neo4j database,
     * either incoming or outgoing
     */
    private ArrayList<Node> findNodeNeighbors(Node node, Label targetLabel, boolean isOutGoing) {//find the neighbors of a node in the Neo4j database
        //isOutGoing (direction):false-incoming, true-outgoing

        ArrayList<Node> neighborsOfnode = new ArrayList<>();

        Iterator<Relationship> relationships = isOutGoing ? node.getRelationships(Direction.OUTGOING).iterator() : node.getRelationships(Direction.INCOMING).iterator();

        while (relationships.hasNext()) {

            Node neighborNode = relationships.next().getOtherNode(node);

            if (targetLabel.name().equals("All")) {

                neighborsOfnode.add(neighborNode);

            } else if (neighborNode.hasLabel(targetLabel)) {

                neighborsOfnode.add(neighborNode);// only add nodes that have the target label and ignore others

            }

        }
        return neighborsOfnode;
    }
}
